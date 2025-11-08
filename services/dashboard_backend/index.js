const WebSocket = require("ws");

const PORT = 3000;
const wss = new WebSocket.Server({ port: PORT });

console.log(`✅ dashboard_backend WebSocket running on ws://localhost:${PORT}`);

wss.on("connection", (ws) => {
  console.log("🟢 Cliente conectado al Dashboard WebSocket");

  ws.on("close", () => {
    console.log("🔴 Cliente desconectado");
  });
});
