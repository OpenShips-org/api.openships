exports.messageType = ['PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport'];

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
			INDEX (mmsi)
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
								ship_name = VALUES(ship_name),
								navigational_status = VALUES(navigational_status),
								rot = VALUES(rot),
								sog = VALUES(sog),
								cog = VALUES(cog),
								true_heading = VALUES(true_heading),
								longitude = VALUES(longitude),
								latitude = VALUES(latitude),
								special_manoeuvre_indicator = VALUES(special_manoeuvre_indicator),
								timestamp = VALUES(timestamp)`,
			[m, ship_name, navigational_status, rot, sog, cog, true_heading, lon, lat, special_manoeuvre_indicator, timestamp]
		);

		if (result && typeof result.affectedRows !== 'undefined') {
			// console.log(`PositionReport DB write for mmsi=${m} affectedRows=${result.affectedRows}`);
		} else {
			// console.log(`PositionReport DB write for mmsi=${m} result=${JSON.stringify(result).slice(0,200)}`);
		}

		// Save to history
		try {
			await saveToHistory(pool, payload, metaData, m);
		} catch (err) {
			console.error('PositionReport handler: failed to save to history', err, { m });
		}

		return result;
	} catch (err) {
		console.error('PositionReport handler DB error:', err, { m, ship_name });
		throw err;
	}
};

async function saveToHistory(pool, payload, metaData, m) {
    const navigational_status = payload.NavigationalStatus ?? null;
    const rot = payload.RateOfTurn ?? null;
    const sog = payload.Sog ?? null;
    const cog = payload.Cog ?? null;
    const true_heading = payload.TrueHeading ?? null;
    const lon = payload.Longitude ?? null;
    const lat = payload.Latitude ?? null;
    const special_manoeuvre_indicator = payload.SpecialManoeuvreIndicator ?? null;
    const timestamp = metaData.time_utc ? new Date(metaData.time_utc) : null;
	if (isNaN(timestamp)) {
    	console.debug('Invalid timestamp, skipping entry for mmsi:', m);
    	return;
	}


    // Angepasste Schwellenwerte
    const MIN_DISTANCE = 0.001; // ca. 100 Meter (realistischer)
    const MIN_TIME_DIFF_MS = 2 * 60 * 1000; // 2 Minuten (häufigere Updates)

	let timeDiff = 0;

    try {
        const [result] = await pool.query(
            `SELECT * FROM position_history WHERE mmsi = ? ORDER BY timestamp DESC LIMIT 1`,
            [m]
        );

        if (result && result.length > 0) {
            const lastEntry = result[0];
            
            // Zeitprüfung
            const lastTime = lastEntry.timestamp ? new Date(lastEntry.timestamp).getTime() : 0;
            const currentTime = timestamp ? timestamp.getTime() : Date.now();
            timeDiff = currentTime - lastTime;
            
            // Positionsprüfung
            let positionChanged = false;
            if (lon !== null && lat !== null && 
                lastEntry.longitude !== null && lastEntry.latitude !== null) {
                const lonDiff = Math.abs(lastEntry.longitude - lon);
                const latDiff = Math.abs(lastEntry.latitude - lat);
                positionChanged = (lonDiff >= MIN_DISTANCE || latDiff >= MIN_DISTANCE);
            }
            
            // Speichere wenn ENTWEDER genug Zeit vergangen ist ODER Position sich geändert hat
            const shouldSave = timeDiff >= MIN_TIME_DIFF_MS || positionChanged;
            
            if (!shouldSave) {
                console.debug(`Skipping history save for mmsi=${m}: timeDiff=${timeDiff}ms, positionChanged=${positionChanged}`);
                return;
            }
        }

        // Speichere neuen Eintrag
        await pool.query(
            `INSERT INTO position_history (mmsi, navigational_status, rot, sog, cog, true_heading, longitude, latitude, special_manoeuvre_indicator, timestamp)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [m, navigational_status, rot, sog, cog, true_heading, lon, lat, special_manoeuvre_indicator, timestamp]
        );
        
        console.log(`Position history saved for mmsi=${m} (timeDiff=${timeDiff}ms)`);
        
    } catch (err) {
        console.error('PositionReport history DB error:', err, { m });
        throw err;
    }
}