const express = require('express');
const router = express.Router();
const mysql = require('mysql2');

const DB_USER = process.env.DB_USER;
const DB_PSWD = process.env.DB_PSWD;
const DB_HOST = process.env.DB_HOST;
const DB_NAME = process.env.DB_NAME;

router.use(express.json());

router.get('/', (req, res) => {
    res.json({
        message: 'Vessels Position Endpoint',
        possibleRoutes: [
            {
                Route: '/all',
                Description: 'Get positions of all vessels',
            },
            {
                Route: '/:mmsi',
                Description: 'Get position of a specific vessel by MMSI',
            }
        ]
    });
});

module.exports = router;