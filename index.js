require('dotenv').config(); // This must be first!

const express = require('express');
const cors = require('cors');
const app = express();
const vesselsRouter = require('./src/routes/vessels');

// =============================================
// configuration
// =============================================

const port = process.env.PORT || 3000;

app.use(cors({
  origin: '*', // Be more restrictive in production
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json({
  limit: '10mb', // Increase JSON size limit if needed
  verify: (req, res, buf) => {
    try {
      JSON.parse(buf);
    } catch (e) {
      console.error('Invalid JSON received:', e.message);
      res.status(400).json({ error: 'Invalid JSON format' });
      throw new Error('Invalid JSON');
    }
  }
}));

// =============================================
// AIS Stream Initialization
// =============================================

function startAisStream() {
  if (process.env.ENABLE_AISTREAM === 'true') {
    const aisStream = require('./src/services/aisstream');
    console.log('AIS Stream started.');
  } else {
    console.log('AIS Stream is disabled.');
  }
}

// =============================================
// routes
// =============================================

app.use('/v1/vessels', vesselsRouter);

app.get('/', (req, res) => {
	res.json({ message: 'Welcome to the OpenShips API' });
});


// =============================================
// start server
// =============================================

app.listen(port, '0.0.0.0', () => {
  console.log(`API server running at http://0.0.0.0:${port}`);
  startAisStream();
});