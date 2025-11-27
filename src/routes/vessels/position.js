const express = require('express');
const router = express.Router();
const mysql = require('mysql2');

const DB_USER = process.env.DB_USER || 'API';
const DB_PSWD = process.env.DB_PSWD;
const DB_HOST = process.env.DB_HOST || 'localhost';
const DB_NAME = process.env.DB_NAME || 'Vessels';

router.use(express.json());

const pool = mysql.createPool({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PSWD,
    database: DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
}).promise();

// =============================================
// routes
// =============================================

router.get('/', (req, res) => {
    res.json({
        message: 'Vessels Position Endpoint',
        possibleRoutes: [
            {
                "Route": '/all',
                "Description": 'Get positions of all vessels',
                "Optional Query Parameters": {
                    "limit": "Number of results to return (default unlimited)",
                    "minLatitude/minLat": "Minimum Latitude filter",
                    "maxLatitude/maxLat": "Maximum Latitude filter",
                    "minLongitude/minLon": "Minimum Longitude filter",
                    "maxLongitude/maxLon": "Maximum Longitude filter",
                    "shipType/vesselTypes": "Filter by Ship Types (List of integer value)",
                    "More Optional Parameters may be added in the future": "Check documentation for updates"
                },
                "Example": "/vessels/position/all?limit=50&minLatitude=10&maxLatitude=50"
            },
            {
                "Route": '/:mmsi',
				"Description": 'Get position of a specific vessel by MMSI',
				"Example": "/vessels/position/123456789"
            }
        ]
    });
});

router.get('/all', async (req, res) => {
	
	// If non paramters are provided then no limits are applied

	try {
        const limit = parseInt(req.query.limit) || 9999999; // Default to a very high limit if not specified
        
        // Accept both parameter name formats
        const minLat = parseFloat(req.query.minLatitude || req.query.minLat) || -90;
        const maxLat = parseFloat(req.query.maxLatitude || req.query.maxLat) || 90;
        const minLon = parseFloat(req.query.minLongitude || req.query.minLon) || -180;
        const maxLon = parseFloat(req.query.maxLongitude || req.query.maxLon) || 180;
        
        // Accept both shipType and vesselTypes parameters
        const shipTypeParam = req.query.shipType || req.query.vesselTypes;
        const shipType = shipTypeParam ? shipTypeParam.split(',').map(type => parseInt(type.trim())).filter(type => !isNaN(type)) : [];

        // Fetch position data from the position database and join with static data for ship type
        let query = `
            SELECT
                p.mmsi               			AS MMSI,
				p.ship_name          			AS ShipName,
				p.timestamp          			AS Timestamp,
                p.latitude           			AS Latitude,
                p.longitude          			AS Longitude,
				p.navigational_status         	AS NavigationStatus,
				p.rot                			AS RateOfTurn,
                p.sog                			AS SpeedOverGround,
                p.cog                			AS CourseOverGround,
                p.true_heading					AS TrueHeading,
                s.ship_type          			AS VesselType
            FROM current_positions p
            LEFT JOIN static_reports s ON p.mmsi = s.mmsi
            WHERE p.latitude BETWEEN ? AND ?
            AND p.longitude BETWEEN ? AND ?
        `;
        const params = [minLat, maxLat, minLon, maxLon];
        if (shipType.length > 0) {
            query += ` AND s.ship_type IN (${shipType.map(() => '?').join(',')})`;
            params.push(...shipType);
        }
        query += ` ORDER BY p.timestamp DESC LIMIT ?`;
        params.push(limit);
        const [rows] = await pool.query(query, params);
        res.json(rows);
    } catch (err) {
        console.error('Error fetching vessel positions:', err.message, err.stack);
        res.status(500).json({ error: 'Internal Server Error', message: err.message });
    }
});

router.get('/:mmsi', async (req, res) => {
	const mmsi = req.params.mmsi;
	if (!mmsi || isNaN(parseInt(mmsi))) {
		return res.status(400).json({ error: 'Invalid MMSI parameter' });
	}
	// This methode is currently not implemented, returning 501
	res.status(501).json({ message: 'Not Implemented: This endpoint is under development.' });
	return;
});

module.exports = router;