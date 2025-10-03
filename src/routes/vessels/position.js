const express = require('express');
const router = express.Router();
const mysql = require('mysql2');

const DB_USER = process.env.DB_USER;
const DB_PSWD = process.env.DB_PSWD;

// Create a MySQL connection pool
const pool = mysql.createPool({
	host: 'localhost',
	user: DB_USER,
	password: DB_PSWD,
	database: 'openships',
	waitForConnections: true,
	connectionLimit: 10,
	queueLimit: 0
});

// Route to get all vessel positions wíth optional parameters (max/min long, max/min lat, type)
router.get('/all', (req, res) => {
	const { max_long, min_long, max_lat, min_lat, type } = req.query;
	let query = 'SELECT * FROM vessel_positions WHERE 1=1';
	const params = [];
	if (max_long) {
		query += ' AND longitude <= ?';
		params.push(parseFloat(max_long));
	}
	if (min_long) {
		query += ' AND longitude >= ?';
		params.push(parseFloat(min_long));
	}
	if (max_lat) {
		query += ' AND latitude <= ?';
		params.push(parseFloat(max_lat));
	}
	if (min_lat) {
		query += ' AND latitude >= ?';
		params.push(parseFloat(min_lat));
	}
	if (type) {
		query += ' AND type = ?';
		params.push(type);
	}
	pool.query(query, params, (error, results) => {
		if (error) {
			return res.status(500).json({ error: 'Database query error' });
		}
		res.json(results);
	});
});