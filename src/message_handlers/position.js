exports.messageType = 'PositionReport';

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
};

exports.handle = async function(pool, message) {
    if (!pool) return;

    const payload = message && message.Message && message.Message.PositionReport;
    if (!payload) return;

    const metaData = payload.Metadata || {};

    const m = payload.MMSI || payload.mmsi || null;
    if (!m) return;

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

    await pool.query(
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
};