import { Client } from "pg";

const clients = new Set();

// WebSocket server setup
const server = Bun.serve({
    port: 9080,
    fetch(req, server) {
        if (server.upgrade(req)) return;
        return new Response("WebSocket upgrade failed", { status: 400 });
    },
    websocket: {
        open(ws) {
            console.log("📡 New client connected");
            clients.add(ws);
            // ws.send("✅ Connected to real-time DB updates");
        },
        close(ws) {
            console.log("❌ Client disconnected");
            clients.delete(ws);
        },
        message(ws, message) {
            console.log(`💬 Client message: ${message}`);
            try {
                // Parse the incoming message to get the clientId
                const { clientId } = JSON.parse(message);

                if (clientId) {
                    // Call the database query with the clientId from the WebSocket message
                    queryDatabaseAndNotifyClients(clientId);
                } else {
                    console.error("Client ID not provided in message");
                }
            } catch (error) {
                console.error("Error parsing client message:", error);
            }
        },
    },
});

console.log("🚀 WebSocket server listening on ws://localhost:9000");

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

// Listen for notifications from PostgreSQL
pg.on("notification", async(msg) => {
    console.log("🛎️ PostgreSQL NOTIFY received:", msg.payload);
    // Send the notification to all connected WebSocket clients
    for (const client of clients) {
        client.send(JSON.stringify({
            payload: msg.payload,
        }));
    }
});

console.log("📡 Listening for PostgreSQL notifications...");

// Function to query the database with the clientId and send data to connected clients
async function queryDatabaseAndNotifyClients(clientId) {
    try {
        console.log('clientid', clientId)
        const result = await pg.query("SELECT fn_latest_cashier_available($1) AS data", [clientId]);
        const jsonResponse = result.rows[0].data;

        // Send the data to all connected WebSocket clients
        for (const client of clients) {
            client.send(JSON.stringify({
                payload: `Data for client ${clientId}`,
                data: jsonResponse,
            }));
        }
    } catch (error) {
        console.error("Error querying database:", error);
    }
}