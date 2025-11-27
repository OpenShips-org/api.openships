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

// Helper function to get week start date
function getWeekStartDate(year, week) {
    const jan1 = new Date(year, 0, 1);
    const daysToAdd = (week - 1) * 7 - jan1.getDay() + 1;
    return new Date(year, 0, 1 + daysToAdd);
}

router.get('/', (req, res) => {
    res.json({
        message: 'Vessels History Endpoint',
        possibleRoutes: [
            {
                "Route": '/:mmsi',
                "Description": 'Get position history of a specific vessel by MMSI',
                "Optional Query Parameters": {
                    "limit": "Number of results to return (default 1000, 'no' for unlimited)",
                    "order": "Sort order: 'asc' (default) or 'desc'",
                    "hour": "Filter from specific hour today (0-23)",
                    "day": "Filter from specific day of year (1-366)",
                    "week": "Filter from specific week of year (1-53)",
                    "month": "Filter from specific month (1-12)",
                    "year": "Filter from specific year (2000-2025)"
                },
                "Example": "/vessels/history/123456789?limit=50&order=desc&hour=14"
            },
            {
                "Route": '/:mmsi/count',
                "Description": 'Get count of position records for a specific vessel by MMSI',
                "Example": "/vessels/history/123456789/count"
            },
            {   "Route": '/:mmsi/latest',
                "Description": 'Get the latest position record for a specific vessel by MMSI',
                "Example": "/vessels/history/123456789/latest"
            }
        ]
    });
});

router.get('/:mmsi', async (req, res) => {
    const mmsi = req.params.mmsi;
    if (!mmsi || isNaN(parseInt(mmsi))) {
        return res.status(400).json({ error: 'Invalid MMSI parameter' });
    }

    try {
        // Parameter parsing
        const limit = req.query.limit === 'no' ? null : (parseInt(req.query.limit) || 1000);
        const order = req.query.order === 'desc' ? 'DESC' : 'ASC';
        
        // Zeit-Filter aufbauen
        let whereConditions = ['mmsi = ?'];
        let params = [mmsi];
        
        const now = new Date();
        const currentYear = now.getFullYear();
        
        // Year filter
        let filterYear = currentYear;
        if (req.query.year !== undefined) {
            const year = parseInt(req.query.year);
            if (year >= 2000 && year <= 2025) {
                filterYear = year;
            }
        }
        
        // Month filter
        if (req.query.month !== undefined) {
            const month = parseInt(req.query.month);
            if (month >= 1 && month <= 12) {
                const startDate = new Date(filterYear, month - 1, 1);
                whereConditions.push('timestamp >= ?');
                params.push(startDate);
            }
        }
        // Week filter
        else if (req.query.week !== undefined) {
            const week = parseInt(req.query.week);
            if (week >= 1 && week <= 53) {
                const startDate = getWeekStartDate(filterYear, week);
                whereConditions.push('timestamp >= ?');
                params.push(startDate);
            }
        }
        // Day filter
        else if (req.query.day !== undefined) {
            const day = parseInt(req.query.day);
            if (day >= 1 && day <= 366) {
                const startDate = new Date(filterYear, 0, day);
                whereConditions.push('timestamp >= ?');
                params.push(startDate);
            }
        }
        // Hour filter
        else if (req.query.hour !== undefined) {
            const hour = parseInt(req.query.hour);
            if (hour >= 0 && hour <= 23) {
                const startDate = new Date(now.getFullYear(), now.getMonth(), now.getDate(), hour);
                whereConditions.push('timestamp >= ?');
                params.push(startDate);
            }
        }
        
        let query = `
            SELECT 
                mmsi AS MMSI,
                navigational_status AS NavigationStatus,
                rot AS RateOfTurn,
                sog AS SpeedOverGround,
                cog AS CourseOverGround,
                true_heading AS TrueHeading,
                longitude AS Longitude,
                latitude AS Latitude,
                special_manoeuvre_indicator AS SpecialManoeuvreIndicator,
                timestamp AS Timestamp
            FROM position_history 
            WHERE ${whereConditions.join(' AND ')}
            ORDER BY timestamp ${order}
        `;
        
        if (limit) {
            query += ' LIMIT ?';
            params.push(limit);
        }
        
        const [rows] = await pool.query(query, params);
        res.json(rows);
        
    } catch (err) {
        console.error('Error fetching vessel history:', err.message, err.stack);
        res.status(500).json({ error: 'Internal Server Error', message: err.message });
    }
});

router.get('/:mmsi/count', async (req, res) => {
    const mmsi = req.params.mmsi;
    if (!mmsi || isNaN(parseInt(mmsi))) {
        return res.status(400).json({ error: 'Invalid MMSI parameter' });
    }
    try {
        const [rows] = await pool.query(
            'SELECT COUNT(*) AS count FROM position_history WHERE mmsi = ?',
            [mmsi]
        );
        res.json({ mmsi: mmsi, count: rows[0].count });
    } catch (err) {
        console.error('Error fetching vessel history count:', err.message, err.stack);
        res.status(500).json({ error: 'Internal Server Error', message: err.message });
    }
});

router.get('/:mmsi/latest', async (req, res) => {
    const mmsi = req.params.mmsi;
    if (!mmsi || isNaN(parseInt(mmsi))) {
        return res.status(400).json({ error: 'Invalid MMSI parameter' });
    }
    try {
        const [rows] = await pool.query(
            `SELECT
                mmsi AS MMSI,
                navigational_status AS NavigationStatus,
                rot AS RateOfTurn,
                sog AS SpeedOverGround,
                cog AS CourseOverGround,
                true_heading AS TrueHeading,
                longitude AS Longitude,
                latitude AS Latitude,
                special_manoeuvre_indicator AS SpecialManoeuvreIndicator,
                timestamp AS Timestamp
             FROM position_history 
             WHERE mmsi = ?
                ORDER BY timestamp DESC
                LIMIT 1`,
            [mmsi]
        );
        if (rows.length === 0) {
            return res.status(404).json({ error: 'No position history found for this MMSI' });
        }
        res.json(rows[0]);
    } catch (err) {
        console.error('Error fetching latest vessel position:', err.message, err.stack);
        res.status(500).json({ error: 'Internal Server Error', message: err.message });
    }
});

module.exports = router;
