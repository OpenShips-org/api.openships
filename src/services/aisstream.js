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
// promise that resolves once handlers are loaded (or immediately if no DB)
let handlersReady = Promise.resolve();

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
  handlersReady = (async function initHandlers() {
    try {
      const mh = require('../message_handlers');
      handlers = await mh.loadHandlers(pool);
      console.log('Loaded message handlers:', Object.keys(handlers));
    } catch (err) {
      console.error('Failed to load message handlers:', err);
    }
  })();
} else {
  console.warn('DB_USER/DB_PSWD not set — database writes are disabled.');
}

// Reconnection parameters
let socket = null;
let shouldReconnect = true; // set to false to stop reconnect attempts (e.g. on graceful shutdown)
let reconnectDelay = 1000; // ms initial
const MAX_RECONNECT_DELAY = 60_000; // ms
let reconnectTimer = null;

function scheduleReconnect() {
  if (!shouldReconnect) return;
  const delayWithJitter = reconnectDelay + Math.floor(Math.random() * 300);
  console.log(`Reconnecting in ${delayWithJitter} ms...`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    createSocket();
  }, delayWithJitter);
  // exponential backoff for next attempt
  reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY);
}

function resetBackoff() {
  reconnectDelay = 1000;
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

function createSocket() {
  // if there's an old socket, make sure it's cleaned up
  if (socket) {
    try {
      socket.removeAllListeners();
      socket.terminate();
    } catch (e) {
      // ignore
    }
    socket = null;
  }

  socket = new WebSocket("wss://stream.aisstream.io/v0/stream");

  socket.on("open", () => {
    console.log("WebSocket connected");
    resetBackoff();

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
    // let 'close' handle reconnect scheduling; but ensure socket is closed
    try {
      socket.close();
    } catch (e) {
      // ignore
    }
  });

  socket.on("close", (code, reason) => {
    console.log("WebSocket closed:", code, reason && reason.toString());
    if (shouldReconnect) scheduleReconnect();
  });

  socket.on('message', async (data) => {
    // avoid extra allocation where possible
    const payloadStr = typeof data === 'string' ? data : data.toString('utf8');

    // handlersReady will usually be resolved before we get messages; awaiting is cheap
    try {
      await handlersReady;
    } catch (err) {
      console.error('handlersReady rejected:', err);
    }

    try {
      const aisMessage = JSON.parse(payloadStr);
      const type = aisMessage.MessageType;
      if (!type) return;

      const handlerEntry = handlers[type];
      if (!handlerEntry || typeof handlerEntry.handle !== 'function') return;

      try {
        // pass pool and message to handler; handlers may accept a third context parameter in future
        await handlerEntry.handle(pool, aisMessage);
      } catch (err) {
        console.error(`Handler error for ${type}:`, err);
      }
    } catch (err) {
      console.error('Failed to parse incoming message:', err);
    }
  });
}

// start initial connection
// Start socket once handlers are initialized (handlersReady is resolved immediately if no DB)
handlersReady.then(() => {
  try {
    createSocket();
  } catch (err) {
    console.error('Failed to create WebSocket after handlersReady:', err);
  }
});

// graceful shutdown: prevent reconnect attempts and close socket
function shutdown() {
  console.log('Shutting down, closing socket and stopping reconnects.');
  shouldReconnect = false;
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (socket) {
    try {
      socket.removeAllListeners();
      socket.close();
    } catch (e) {}
  }
  // give socket time to close, then exit
  setTimeout(() => process.exit(0), 1000);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// global error handlers to catch async mistakes
process.on('unhandledRejection', (reason, promise) => {
  console.error('unhandledRejection at:', promise, 'reason:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('uncaughtException:', err);
});