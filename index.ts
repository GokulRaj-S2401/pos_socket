import { Client } from "pg";

const clients = new Map(); // Map<WebSocket, shopid>

// Setup Bun WebSocket server
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
            clients.delete(ws);
        },
        message(ws, message) {
            console.log(`💬 Client message: ${message}`);
            try {
                const { clientId } = JSON.parse(message);
                if (clientId) {
                    clients.set(ws, clientId); // Associate this socket with shopid (clientId)
                    console.log(`🔗 Client registered with shopid: ${clientId}`);

                    // Send initial data for this client/shopid
                    queryDatabaseAndNotifyClients(clientId, ws);
                } else {
                    console.error("⚠️ Client ID (shopid) not provided in message");
                }
            } catch (error) {
                console.error("❌ Error parsing client message:", error);
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

// Listen to PostgreSQL NOTIFY events on the 'items_channel'
await pg.query("LISTEN items_channel");

// Listen for NOTIFY events
pg.on("notification", async(msg) => {
    console.log("🛎️ PostgreSQL NOTIFY received:", msg.payload);
    try {
        const { shopid, operation, data } = JSON.parse(msg.payload);

        for (const [client, subscribedShopId] of clients.entries()) {
            if (parseInt(subscribedShopId, 10) === shopid) {
                client.send(
                    JSON.stringify({
                        payload: `🔄 Update for shopid ${shopid}`,
                        operation,
                        data,
                    })
                );
            }
        }
    } catch (error) {
        console.error("❌ Error parsing PostgreSQL notification:", error);
    }
});

console.log("📡 Listening for PostgreSQL notifications...");

// Function to fetch initial data from DB and notify relevant clients
async function queryDatabaseAndNotifyClients(shopid, specificClient = null) {
    try {
        const result = await pg.query(
            "SELECT fn_latest_cashier_available($1) AS data", [shopid]
        );
        const jsonResponse = result.rows[0].data;

        for (const [client, subscribedShopId] of clients.entries()) {
            if (parseInt(subscribedShopId, 10) === shopid) {
                if (!specificClient || specificClient === client) {
                    client.send(
                        JSON.stringify({
                            payload: `📤 Initial data for shopid ${shopid}`,
                            data: jsonResponse,
                        })
                    );
                }
            }
        }
    } catch (error) {
        console.error("❌ Error querying database:", error);
    }
}