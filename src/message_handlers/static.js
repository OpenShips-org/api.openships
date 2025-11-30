exports.messageType = ['ShipStaticData'];

exports.ensure = async function(pool) {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS static_reports (
            mmsi VARCHAR(20) PRIMARY KEY,
            imo VARCHAR(20) NULL,
            callSign VARCHAR(20) NULL,
            ship_name VARCHAR(255) NULL,
            destination VARCHAR(255) NULL,
            dimensionA INT NULL,
            dimensionB INT NULL,
            dimensionC INT NULL,
            dimensionD INT NULL,
            ship_type INT NULL,
            max_draught INT NULL,
            eta DATETIME NULL
        )
    `);
};

exports.handle = async function(pool, message) {
    if (!pool) {
        console.warn('StaticReport handler: no DB pool available, skipping write');
        return;
    }

    // incoming messages use different field names depending on provider/version
    const payload = message && message.Message && (
        message.Message.StaticReport || message.Message.StaticDataReport || message.Message.ShipStaticData || message.Message.Static
    );
    if (!payload) {
        try {
            const sample = JSON.stringify(message).slice(0, 1000);
            console.debug('StaticReport handler: missing StaticReport payload, message sample:', sample);
        } catch (err) {
            console.debug('StaticReport handler: missing payload and failed to stringify message');
        }
        return;
    }

    const meta = message.MetaData || payload.Metadata || {};
    const m = meta.MMSI || meta.MMSI_String || payload.MMSI || payload.mmsi || null;
    if (!m) {
        console.debug('StaticReport handler: no MMSI found, sample meta:', JSON.stringify(meta).slice(0,200));
        return;
    }
    
    const imo = payload.ImoNumber || null;
    const callSign = payload.CallSign || payload.callSign || null;
    const ship_name = payload.Name || payload.name || null;
    const destination = payload.Destination || payload.destination || null;
    const dimensionA = (payload.Dimension && payload.Dimension.A) ?? null;
    const dimensionB = (payload.Dimension && payload.Dimension.B) ?? null;
    const dimensionC = (payload.Dimension && payload.Dimension.C) ?? null;
    const dimensionD = (payload.Dimension && payload.Dimension.D) ?? null;
    const ship_type = payload.Type ?? null;
    const max_draught = payload.MaximumStaticDraught ?? null;

    let eta = null;
    if (payload.Eta && payload.Eta.Year != null && payload.Eta.Month != null && payload.Eta.Day != null && payload.Eta.Minute != null) {
        const hour = payload.Eta.Hour != null ? payload.Eta.Hour : 0;
        eta = new Date(Date.UTC(payload.Eta.Year, payload.Eta.Month - 1, payload.Eta.Day, hour, payload.Eta.Minute));
    }

    try {
        const [result] = await pool.query(
            `INSERT INTO static_reports (mmsi, imo, callSign, ship_name, destination, dimensionA, dimensionB, dimensionC, dimensionD, ship_type, max_draught, eta)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                imo = VALUES(imo),
                callSign = VALUES(callSign),
                ship_name = VALUES(ship_name),
                destination = VALUES(destination),
                dimensionA = VALUES(dimensionA),
                dimensionB = VALUES(dimensionB),
                dimensionC = VALUES(dimensionC),
                dimensionD = VALUES(dimensionD),
                ship_type = VALUES(ship_type),
                max_draught = VALUES(max_draught),
                eta = VALUES(eta)`,
            [m, imo, callSign, ship_name, destination, dimensionA, dimensionB, dimensionC, dimensionD, ship_type, max_draught, eta]
        );

        if (result && typeof result.affectedRows !== 'undefined') {
            // console.log(`StaticReport DB write for mmsi=${m} affectedRows=${result.affectedRows}`);
        } else {
            // console.log(`StaticReport DB write for mmsi=${m} result=${JSON.stringify(result).slice(0,200)}`);
        }
        return result;
    } catch (err) {
        console.error('StaticReport handler DB error:', err, { m, ship_name });
        throw err;
    }
};