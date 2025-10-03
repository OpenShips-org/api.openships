const express = require('express');
const app = express();
const vesselsRouter = require('./src/routes/vessels');

// =============================================
// configuration
// =============================================

const port = process.env.PORT || 3000;

app.use(express.json());

// =============================================
// routes
// =============================================

app.use('/vessels', vesselsRouter);

app.get('/', (req, res) => {
	res.json({ message: 'Welcome to the OpenShips API' });
});


// =============================================
// start server
// =============================================

app.listen(port, () => {
	console.log(`API server running at http://localhost:${port}`);
	try {
		require('./aisstream');
		console.log('AIS stream started.');
	} catch (err) {
		console.error('Failed to start AIS stream:', err);
	}
});