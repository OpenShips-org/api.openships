const express = require('express');
const router = express.Router();
const positionRouter = require('./position');
const historyRouter = require('./history');

router.use('/position', positionRouter);
router.use('/history', historyRouter);

router.use(express.json());

router.get('/', (req, res) => {
    res.json({
        message: 'Vessels Endpoint',
        possibleRoutes: [
            {
                Route: '/position',
                Description: 'Sub-endpoint for vessel positions',
            },
            {
                Route: '/history',
                Description: 'Sub-endpoint for vessel position history',
            }
        ]
    });
});

module.exports = router;