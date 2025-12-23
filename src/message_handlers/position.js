exports.messageType = ['PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport'];

// in-memory cache to avoid a SELECT for every incoming message
// keys: mmsi -> { longitude, latitude, timestampMs }
const lastSeen = new Map();
// Buffer for history inserts to perform batch writes
const historyBuffer = [];

// Configuration (can be tuned via env)
const HISTORY_FLUSH_INTERVAL_MS = parseInt(process.env.POS_HISTORY_FLUSH_MS) || 1000; // flush every 1s
const HISTORY_MAX_BATCH = parseInt(process.env.POS_HISTORY_MAX_BATCH) || 500; // max rows per batch
const HISTORY_BUFFER_MAX = parseInt(process.env.POS_HISTORY_BUFFER_MAX) || 5000; // max entries in buffer
const LASTSEEN_TTL_MS = parseInt(process.env.POS_LASTSEEN_TTL_MS) || 24 * 60 * 60 * 1000; // 24h
const LASTSEEN_CLEAN_INTERVAL_MS = parseInt(process.env.POS_LASTSEEN_CLEAN_MS) || 10 * 60 * 1000; // 10min
const LASTSEEN_MAX_SIZE = parseInt(process.env.POS_LASTSEEN_MAX_SIZE) || 10000; // configurable
// neue konfigurierbare Schwellenwerte (moved up)
const MIN_DISTANCE_METERS = parseFloat(process.env.POS_MIN_DISTANCE_METERS) || 100; // meter
const MIN_TIME_DIFF_MS = parseInt(process.env.POS_MIN_TIME_DIFF_MS) || 5 * 60 * 1000; // 5 minutes

// Background flusher for historyBuffer
let flusherInterval = null;
let isFlushing = false;

async function flushHistoryOnce(pool) {
    if (isFlushing) return;
    if (historyBuffer.length === 0) return;
    isFlushing = true;
    try {
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
            // Requeue with limited retries: increase _retries and push to end to avoid blocking
            for (const e of batch) {
                e._retries = (e._retries || 0) + 1;
                if (e._retries <= 3) {
                    historyBuffer.push(e); // retry later
                } else {
                    console.warn('Dropping history entry after retries', { mmsi: e.mmsi, retries: e._retries });
                }
            }
        }
    } finally {
        isFlushing = false;
    }
}

function startHistoryFlusher(pool) {
    if (flusherInterval) return;
    flusherInterval = setInterval(() => {
        flushHistoryOnce(pool).catch(err => console.error('History flusher unexpected error:', err));
    }, HISTORY_FLUSH_INTERVAL_MS);

    // try to flush remaining on process exit — now awaits final flush and clears interval
    const flushAndExit = async () => {
        if (!pool) return;
        clearInterval(flusherInterval);
        flusherInterval = null;
        try {
            // wait up to 2s for final flush (best-effort)
            const p = flushHistoryOnce(pool);
            await Promise.race([p, new Promise(r => setTimeout(r, 2000))]);
        } catch (e) {
            /* ignore */
        }
    };
    process.once('beforeExit', flushAndExit);
    process.once('SIGINT', flushAndExit);
    process.once('SIGTERM', flushAndExit);
}

// Cleaner for lastSeen to avoid unbounded growth
let cleanerInterval = null;
function startLastSeenCleaner() {
    if (cleanerInterval) return;
    cleanerInterval = setInterval(() => {
        const now = Date.now();
        // Entferne abgelaufene
        for (const [mmsi, v] of lastSeen.entries()) {
            if (now - v.timestampMs > LASTSEEN_TTL_MS) lastSeen.delete(mmsi);
        }
        // Begrenze Größe, entferne älteste
        if (lastSeen.size > LASTSEEN_MAX_SIZE) {
            const sorted = Array.from(lastSeen.entries()).sort((a, b) => a[1].timestampMs - b[1].timestampMs);
            const toRemove = sorted.slice(0, lastSeen.size - LASTSEEN_MAX_SIZE);
            for (const [mmsi] of toRemove) lastSeen.delete(mmsi);
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

    const payload = message && message.Message && message.Message.PositionReport;
    if (!payload) {
        return;
    }

    // Metadata is provided on the top-level message in the stream (MetaData)
    const metaData = message.MetaData || payload.Metadata || {} ;

    const m = metaData.MMSI || metaData.MMSI_String || payload.MMSI || payload.mmsi || null;

    if (!m) {
        console.warn('PositionReport handler: no MMSI found in message, skipping');
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

    // Validate coords but DO NOT return early: allow current_positions update even if coordinates invalid.
    let coordsValid = true;
    if ((lon !== null && (lon < -180 || lon > 180)) || (lat !== null && (lat < -90 || lat > 90))) {
        console.warn('Invalid coordinates, will skip history save but still update current_positions:', { lon, lat });
        coordsValid = false;
    }

    // robustes Timestamp-Parsing: support numeric epoch (s or ms) and strings
    const timestampRaw = metaData.time_utc ?? metaData.timestamp ?? metaData.time ?? null;
    let timestamp = null;
    if (timestampRaw != null) {
        if (typeof timestampRaw === 'number' || /^\d+$/.test(String(timestampRaw))) {
            const n = Number(timestampRaw);
            // heuristic: >1e12 => ms, else seconds
            timestamp = new Date(n > 1e12 ? n : n * 1000);
        } else {
            timestamp = new Date(String(timestampRaw));
        }
        if (isNaN(timestamp.getTime())) {
            console.warn('Invalid timestamp in message, skipping history save:', timestampRaw);
            timestamp = null;
        }
    }

    try {
        const [result] = await pool.query(
            `INSERT INTO current_positions (mmsi, ship_name, navigational_status, rot, sog, cog, true_heading, longitude, latitude, special_manoeuvre_indicator, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            ship_name = VALUES(ship_name),
                            navigational_status = VALUES(navigational_status),
                            rot = VALUES(rot),
                            sog = VALUES(sog),
                            cog = VALUES(cog),
                            true_heading = VALUES(true_heading),
                            longitude = VALUES(longitude),
                            latitude = VALUES(latitude),
                            special_manoeuvre_indicator = VALUES(special_manoeuvre_indicator),
                            timestamp = VALUES(timestamp)` ,
            [m, ship_name, navigational_status, rot, sog, cog, true_heading, lon, lat, special_manoeuvre_indicator, timestamp]
        );

        // History save: use in-memory cache to avoid a SELECT for each message
        try {
            // use configured time diff constant (moved up)
            const timestampMs = timestamp ? (timestamp instanceof Date ? timestamp.getTime() : Date.parse(timestamp)) : Date.now();
            if (isNaN(timestampMs)) {
                // don't attempt history save if timestamp invalid
                // but current position already saved above
                return result;
            }

            let shouldSave = false;
            const last = lastSeen.get(m);
            let positionChanged = false;
            if (coordsValid && lon !== null && lat !== null && last && last.longitude !== null && last.latitude !== null) {
                const dist = haversineMeters(last.longitude, last.latitude, lon, lat);
                positionChanged = (dist >= MIN_DISTANCE_METERS);
            }

            if (!last) {
                // first message we see for this MMSI -> save (only if coords valid)
                shouldSave = coordsValid;
            } else {
                const timeDiff = timestampMs - last.timestampMs;
                shouldSave = coordsValid && (positionChanged || timeDiff >= MIN_TIME_DIFF_MS);
            }

            if (shouldSave) {
                // keep buffer bounded
                if (historyBuffer.length >= HISTORY_BUFFER_MAX) {
                    console.warn('History buffer full, dropping oldest entry to make room');
                    historyBuffer.shift();
                }
                // reduce batch size if placeholders might get too big (defensive)
                if (HISTORY_MAX_BATCH <= 0) HISTORY_MAX_BATCH = 1;
                historyBuffer.push({ mmsi: m, navigational_status, rot, sog, cog, true_heading, longitude: lon, latitude: lat, special_manoeuvre_indicator, timestamp, _retries: 0 });
                startHistoryFlusher(pool);
            }

            // Update cache (always update to reflect last seen time even if coords invalid)
            lastSeen.set(m, { longitude: coordsValid ? lon : null, latitude: coordsValid ? lat : null, timestampMs });
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

// helper: Haversine distance in meters
function haversineMeters(lon1, lat1, lon2, lat2) {
    const toRad = (v) => v * Math.PI / 180;
    const R = 6371000; // Earth radius in meters
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
              Math.sin(dLon/2) * Math.sin(dLon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
}
// end