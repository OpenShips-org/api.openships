require("dotenv").config();
const WebSocket = require("ws"); // npm install ws
const mysql = require("mysql2");

// Database connection setup
const DB_USER = process.env.DB_USER;
const DB_PSWD = process.env.DB_PSWD;
const DB_HOST = process.env.DB_HOST || "localhost";
const DB_NAME = process.env.DB_NAME || "Vessels";

const API_KEY = process.env.AISSTREAM_API_KEY;
if (!API_KEY) {
  console.error("AISSTREAM_API_KEY is not set. Set it in your .env or environment and rerun.");
  process.exit(1);
}

// create pool only if DB_USER/PSWD present
let pool;
let handlers = {};
if (DB_USER && DB_PSWD) {
  pool = mysql
    .createPool({
      host: DB_HOST,
      user: DB_USER,
      password: DB_PSWD,
      database: DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    })
    .promise();

  // load handlers and let them ensure tables
  (async function initHandlers() {
    try {
      const mh = require('./message_handlers');
      handlers = await mh.loadHandlers(pool);
      console.log('Loaded message handlers:', Object.keys(handlers));
    } catch (err) {
      console.error('Failed to load message handlers:', err);
    }
  })();
} else {
  console.warn('DB_USER/DB_PSWD not set — database writes are disabled.');
}

const socket = new WebSocket("wss://stream.aisstream.io/v0/stream");

socket.on("open", () => {
  console.log("WebSocket connected");
  const subscriptionMessage = {
    APIkey: API_KEY,
    BoundingBoxes: [
      [
        [-180, -90],
        [180, 90],
      ],
    ],
  };
  // don't log the API key
  socket.send(JSON.stringify(subscriptionMessage));
  console.log("Subscription sent (API key redacted)");
});

socket.on("error", (err) => {
  console.error("WebSocket error:", err);
});

socket.on("close", (code, reason) => {
  console.log("WebSocket closed:", code, reason && reason.toString());
});

socket.on('message', async (data) => {
  const payload = typeof data === 'string' ? data : data.toString();
  try {
    const aisMessage = JSON.parse(payload);

    const type = aisMessage.MessageType;
    if (type && handlers[type] && typeof handlers[type].handle === 'function') {
      try {
        await handlers[type].handle(pool, aisMessage);
      } catch (err) {
        console.error(`Handler error for ${type}:`, err);
      }
    } else {
      // fallback: ignore or log
      // console.log('Unhandled message type:', type);
    }
  } catch (err) {
    console.error('Failed to parse incoming message:', err, payload);
  }
});