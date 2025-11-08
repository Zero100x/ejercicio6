const express = require("express");
const amqp = require("amqplib");

const app = express();
app.use(express.json());

const RABBITMQ_URL = "amqp://rabbitmq:5672";
const QUEUE = "new_orders_queue";

app.post("/order", async (req, res) => {
  try {
    const order = {
      order_id: "order-" + Math.random().toString(36).substring(2, 10),
      client_id: "user-" + Math.floor(Math.random() * 1000),
      details: req.body.details ?? "Pedido generado automáticamente",
      phone: req.body.phone ?? "+57-3000000000"
    };

    const conn = await amqp.connect(RABBITMQ_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUE, { durable: true });
    ch.sendToQueue(QUEUE, Buffer.from(JSON.stringify(order)));

    console.log("📦 Pedido enviado a RabbitMQ:", order);
    res.json({ status: "queued", order });
  } catch (err) {
    console.error("❌ Error:", err);
    res.status(500).json({ error: true, message: err.message });
  }
});

app.listen(3000, () =>
  console.log("✅ order_api corriendo en http://localhost:3001/order")
);
