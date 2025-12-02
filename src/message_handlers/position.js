exports.messageType = ['PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport'];

// in-memory cache to avoid a SELECT for every incoming message
// keys: mmsi -> { longitude, latitude, timestampMs }
const lastSeen = new Map();
// Buffer for history inserts to perform batch writes
const historyBuffer = [];

// Configuration (can be tuned via env)
const HISTORY_FLUSH_INTERVAL_MS = parseInt(process.env.POS_HISTORY_FLUSH_MS) || 1000; // flush every 1s
const HISTORY_MAX_BATCH = parseInt(process.env.POS_HISTORY_MAX_BATCH) || 500; // max rows per batch
const LASTSEEN_TTL_MS = parseInt(process.env.POS_LASTSEEN_TTL_MS) || 24 * 60 * 60 * 1000; // 24h
const LASTSEEN_CLEAN_INTERVAL_MS = parseInt(process.env.POS_LASTSEEN_CLEAN_MS) || 10 * 60 * 1000; // 10min

// Background flusher for historyBuffer
let flusherInterval = null;
function startHistoryFlusher(pool) {
	if (flusherInterval) return;
	flusherInterval = setInterval(async () => {
		if (historyBuffer.length === 0) return;
		// take up to HISTORY_MAX_BATCH entries
		const batch = historyBuffer.splice(0, HISTORY_MAX_BATCH);
		// build single INSERT with multiple VALUES
		const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
		const sql = `INSERT INTO position_history (mmsi, navigational_status, rot, sog, cog, true_heading, longitude, latitude, special_manoeuvre_indicator, timestamp) VALUES ${placeholders}`;
		const params = [];
		for (const r of batch) params.push(r.mmsi, r.navigational_status, r.rot, r.sog, r.cog, r.true_heading, r.longitude, r.latitude, r.special_manoeuvre_indicator, r.timestamp);
		try {
			await pool.query(sql, params);
		} catch (err) {
			console.error('Batch history insert failed:', err);
			// On failure, requeue at front to try later (but avoid infinite loop)
			historyBuffer.unshift(...batch);
		}
	}, HISTORY_FLUSH_INTERVAL_MS);
}

// Cleaner for lastSeen to avoid unbounded growth
let cleanerInterval = null;
function startLastSeenCleaner() {
	if (cleanerInterval) return;
	cleanerInterval = setInterval(() => {
		const now = Date.now();
		for (const [mmsi, v] of lastSeen.entries()) {
			if (now - v.timestampMs > LASTSEEN_TTL_MS) lastSeen.delete(mmsi);
		}
	}, LASTSEEN_CLEAN_INTERVAL_MS);
}

exports.ensure = async function(pool) {
	await pool.query(`
		CREATE TABLE IF NOT EXISTS current_positions (
			mmsi VARCHAR(20) PRIMARY KEY,
			ship_name VARCHAR(255) NULL,
			navigational_status INT NULL,
			rot DOUBLE NULL,
			sog DOUBLE NULL,
			cog DOUBLE NULL,
			true_heading INT NULL,
			longitude DOUBLE NULL,
			latitude DOUBLE NULL,
			special_manoeuvre_indicator INT NULL,
			timestamp DATETIME NULL
		)
	`);

	await pool.query(`
		CREATE TABLE IF NOT EXISTS position_history (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			mmsi VARCHAR(20) NOT NULL,
			navigational_status INT NULL,
			rot DOUBLE NULL,
			sog DOUBLE NULL,
			cog DOUBLE NULL,
			true_heading INT NULL,
			longitude DOUBLE NULL,
			latitude DOUBLE NULL,
			special_manoeuvre_indicator INT NULL,
			timestamp DATETIME NULL,
			INDEX (mmsi),
			INDEX idx_mmsi_timestamp (mmsi, timestamp)
		)
	`);
};

exports.handle = async function(pool, message) {
	if (!pool) {
		return;
	}

	const payload = message && message.Message && message.PositionReport;
	if (!payload) {
		return;
	}

	// Metadata is provided on the top-level message in the stream (MetaData)
	const metaData = message.MetaData || payload.Metadata || {};

	const m = metaData.MMSI || metaData.MMSI_String || payload.MMSI || payload.mmsi || null;
	if (!m) {
		console.debug('PositionReport handler: no MMSI found in message, skipping. metaData sample:', JSON.stringify(metaData).slice(0,200));
		return;
	}

	const ship_name = metaData.ShipName || metaData.shipName || null;
	const navigational_status = payload.NavigationalStatus ?? null;
	const rot = payload.RateOfTurn ?? null;
	const sog = payload.Sog ?? null;
	const cog = payload.Cog ?? null;
	const true_heading = payload.TrueHeading ?? null;
	const lon = payload.Longitude ?? null;
	const lat = payload.Latitude ?? null;
	const special_manoeuvre_indicator = payload.SpecialManoeuvreIndicator ?? null;
	const timestamp = metaData.time_utc ? new Date(metaData.time_utc) : null;

	try {
		const [result] = await pool.query(
			`INSERT INTO current_positions (mmsi, ship_name, navigational_status, rot, sog, cog, true_heading, longitude, latitude, special_manoeuvre_indicator, timestamp)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
						ON DUPLICATE KEY UPDATE
							ship_name = IF(VALUES(timestamp) > timestamp, VALUES(ship_name), ship_name),
							navigational_status = IF(VALUES(timestamp) > timestamp, VALUES(navigational_status), navigational_status),
							rot = IF(VALUES(timestamp) > timestamp, VALUES(rot), rot),
							sog = IF(VALUES(timestamp) > timestamp, VALUES(sog), sog),
							cog = IF(VALUES(timestamp) > timestamp, VALUES(cog), cog),
							true_heading = IF(VALUES(timestamp) > timestamp, VALUES(true_heading), true_heading),
							longitude = IF(VALUES(timestamp) > timestamp, VALUES(longitude), longitude),
							latitude = IF(VALUES(timestamp) > timestamp, VALUES(latitude), latitude),
							special_manoeuvre_indicator = IF(VALUES(timestamp) > timestamp, VALUES(special_manoeuvre_indicator), special_manoeuvre_indicator),
							timestamp = IF(VALUES(timestamp) > timestamp, VALUES(timestamp), timestamp)`,
			[m, ship_name, navigational_status, rot, sog, cog, true_heading, lon, lat, special_manoeuvre_indicator, timestamp]
		);

			// History save: use in-memory cache to avoid a SELECT for each message
		try {
			const MIN_DISTANCE = 0.001; // ~100m
			const MIN_TIME_DIFF_MS = 5 * 60 * 1000; // 5 minutes

			const timestampMs = timestamp ? (timestamp instanceof Date ? timestamp.getTime() : Date.parse(timestamp)) : Date.now();
			if (isNaN(timestampMs)) {
				// fallback
				// don't attempt history save if timestamp invalid
				return result;
			}

			let shouldSave = false;
			const last = lastSeen.get(m);
			if (!last) {
				// first message we see for this MMSI -> save
				shouldSave = true;
			} else {
				const timeDiff = timestampMs - last.timestampMs;
				let positionChanged = false;
				if (lon !== null && lat !== null && last.longitude !== null && last.latitude !== null) {
					const lonDiff = Math.abs(last.longitude - lon);
					const latDiff = Math.abs(last.latitude - lat);
					positionChanged = (lonDiff >= MIN_DISTANCE || latDiff >= MIN_DISTANCE);
				}

				shouldSave = timeDiff >= MIN_TIME_DIFF_MS || positionChanged;
			}

			if (shouldSave) {
				// push to batch buffer instead of writing directly
				historyBuffer.push({ mmsi: m, navigational_status, rot, sog, cog, true_heading, longitude: lon, latitude: lat, special_manoeuvre_indicator, timestamp });
				// ensure flusher is running
				startHistoryFlusher(pool);
			}

			// Update cache
			lastSeen.set(m, { longitude: lon, latitude: lat, timestampMs });
			// ensure cleaner running
			startLastSeenCleaner();
		} catch (err) {
			console.error('PositionReport handler: failed to save to history', err, { m });
		}

		return result;
	} catch (err) {
		console.error('PositionReport handler DB error:', err, { m, ship_name });
		throw err;
	}
};
// end