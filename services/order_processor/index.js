const amqp = require("amqplib");
const { Kafka } = require("kafkajs");

const QUEUE = "new_orders_queue";
const KAFKA_BROKER = "kafka:9092";

const kafka = new Kafka({
  clientId: "order_processor",
  brokers: [KAFKA_BROKER]
});

async function start() {
  const consumer = await amqp.connect("amqp://rabbitmq:5672");
  const channel = await consumer.createChannel();
  await channel.assertQueue(QUEUE);

  const producer = kafka.producer();
  await producer.connect();

  console.log("✅ order_processor conectado a RabbitMQ y Kafka");

  channel.consume(QUEUE, async (msg) => {
    const order = JSON.parse(msg.content.toString());

    console.log("📥 Pedido recibido desde RabbitMQ:", order);

    const event = {
      event_type: "DRIVER_ASSIGNED",
      order_id: order.order_id,
      driver_id: "driver-" + Math.floor(Math.random() * 999)
    };

    await producer.send({
      topic: "order_events",
      messages: [
        { key: order.order_id, value: JSON.stringify(event) }
      ],
    });

    console.log("🚚 Evento DRIVER_ASSIGNED publicado en Kafka:", event);

    channel.ack(msg);
  });
}

start();
