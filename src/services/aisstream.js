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
    const payload = typeof data === 'string' ? data : data.toString();
    // wait until handlers (and DB tables) are ready before processing messages
    try {
      await handlersReady;
    } catch (err) {
      // handlersReady should not reject, but guard just in case
      console.error('handlersReady rejected:', err);
    }

    try {
      const aisMessage = JSON.parse(payload);

      const type = aisMessage.MessageType;
      if (!type) {
        // be explicit in logs so we can map incoming payloads to handlers
        // console.debug('Incoming message without MessageType', aisMessage);
        return;
      }

      const handlerEntry = handlers[type];
      if (!handlerEntry || typeof handlerEntry.handle !== 'function') {
        console.warn('No handler registered for MessageType:', type);
        return;
      }

      try {
        console.debug(`Dispatching ${type} to handler`);
        const res = await handlerEntry.handle(pool, aisMessage);
        // handler implementations may not return anything; log success anyway
        console.log(`Handler '${type}' executed${res ? `, result: ${JSON.stringify(res).slice(0,200)}` : ''}`);
      } catch (err) {
        console.error(`Handler error for ${type}:`, err);
      }
    } catch (err) {
      console.error('Failed to parse incoming message:', err, payload);
    }
  });
}

// start initial connection
createSocket();

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