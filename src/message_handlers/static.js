exports.messageType = 'StaticReport';

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
    if (!pool) return;

    const payload = message && message.Message && message.Message.StaticReport;
    if (!payload) return;

    const m = payload.MMSI || payload.mmsi || null;
    if (!m) return;
    
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

    await pool.query(
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
};