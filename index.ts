import { Client } from "pg";

// Map to track which clientId is associated with which WebSocket
const clients = new Map(); // Map<WebSocket, clientId>

const server = Bun.serve({
    port: 9080,
    fetch(req, server) {
        if (server.upgrade(req)) return;
        return new Response("WebSocket upgrade failed", { status: 400 });
    },
    websocket: {
        open(ws) {
            console.log("📡 New client connected");
        },
        close(ws) {
            console.log("❌ Client disconnected");
            clients.delete(ws); // Remove client from map on disconnect
        },
        message(ws, message) {
            console.log(`💬 Client message: ${message}`);
            try {
                const { clientId } = JSON.parse(message);

                if (clientId) {
                    clients.set(ws, clientId); // Associate this WebSocket with the clientId
                    queryDatabaseAndNotifyClient(ws, clientId); // Send initial data
                } else {
                    console.error("❗ Client ID not provided in message");
                }
            } catch (error) {
                console.error("❗ Error parsing client message:", error);
            }
        },
    },
});

console.log("🚀 WebSocket server listening on ws://localhost:9080");

// PostgreSQL client setup
const pg = new Client({
    connectionString: "postgres://postgres:Welcomee73ai@e73.c78g6g2ka1nw.eu-north-1.rds.amazonaws.com:5432/E73_Ramachandran",
    ssl: {
        rejectUnauthorized: false,
    },
});

await pg.connect();

// Start listening to PostgreSQL notifications on the items_channel
await pg.query("LISTEN items_channel");

// Listen for PostgreSQL NOTIFY messages
pg.on("notification", async (msg) => {
    console.log("🛎️ PostgreSQL NOTIFY received:", msg.payload);

    try {
        const payload = JSON.parse(msg.payload);
        const clientIdFromDb = payload?.clientId;

        // Send to only matching clients
        for (const [ws, clientId] of clients.entries()) {
            if (clientId === clientIdFromDb) {
                ws.send(JSON.stringify({
                    payload: `Notification for client ${clientId}`,
                    data: payload,
                }));
            }
        }
    } catch (err) {
        console.error("❗ Error handling PostgreSQL notification:", err);
    }
});

console.log("📡 Listening for PostgreSQL notifications...");

// Query the database with the clientId and send data to the specific client
async function queryDatabaseAndNotifyClient(ws, clientId) {
    try {
        console.log("🔍 Querying database for clientId:", clientId);

        const result = await pg.query("SELECT fn_latest_cashier_available($1) AS data", [clientId]);
        const jsonResponse = result.rows[0].data;

        ws.send(JSON.stringify({
            payload: `Data for client ${clientId}`,
            data: jsonResponse,
        }));
    } catch (error) {
        console.error("❗ Error querying database:", error);
    }
}
