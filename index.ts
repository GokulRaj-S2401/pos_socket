import { Client } from "pg";

const clients = new Set();

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
            ws.send("✅ Connected to real-time DB updates");
        },
        close(ws) {
            console.log("❌ Client disconnected");
            clients.delete(ws);
        },
        message(ws, message) {
            console.log(`💬 Client message: ${message}`);
        },
    },
});

console.log("🚀 WebSocket server listening on ws://localhost:9080");

const pg = new Client({
    connectionString: "postgres://postgres:Welcomee73ai@e73.c78g6g2ka1nw.eu-north-1.rds.amazonaws.com:5432/E73_Ramachandran",
    ssl: {
        rejectUnauthorized: false
    }
});

await pg.connect();

await pg.query("LISTEN items_channel");


pg.on("notification", async (msg) => {
    console.log("🛎️ PostgreSQL NOTIFY received:", msg.payload);
    const result = await pg.query("SELECT fn_latest_cashier_available($1) AS data", [4]);
    const jsonResponse = result.rows[0].data;
    for (const client of clients) {
        client.send(JSON.stringify({
            payload: msg.payload,
            data: jsonResponse
        }));
    }
});

console.log("📡 Listening for PostgreSQL notifications...");