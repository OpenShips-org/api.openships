const express = require('express');

const app = express();
const port = 3000;

app.use(express.json());

// Sample route
app.get('/', (req, res) => {
  res.json({ message: 'Welcome to the OpenShips API' });
});

app.listen(port, () => {
  console.log(`API server running at http://localhost:${port}`);
  // Start AIS stream when the server is up
  try {
    require('./aisstream');
    console.log('AIS stream started.');
  } catch (err) {
    console.error('Failed to start AIS stream:', err);
  }
});