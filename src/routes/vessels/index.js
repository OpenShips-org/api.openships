const express = require('express');
const router = express.Router();
const positionRouter = require('./position');

router.use('/position', positionRouter);

router.use(express.json());

router.get('/', (req, res) => {
    res.json({
        message: 'Vessels Endpoint',
        possibleRoutes: [
            {
                Route: '/position',
                Description: 'Sub-endpoint for vessel positions',
            }
        ]
    });
});

module.exports = router;