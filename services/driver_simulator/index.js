const { Kafka } = require("kafkajs");

const KAFKA_BROKER = "kafka:9092";
const kafka = new Kafka({
  clientId: "driver_simulator",
  brokers: [KAFKA_BROKER],
});

async function start() {
  const consumer = kafka.consumer({ groupId: "driver_simulator_group" });
  const producer = kafka.producer();

  await consumer.connect();
  await consumer.subscribe({ topic: "order_events", fromBeginning: true });
  await producer.connect();

  console.log("🚕 driver_simulator escuchando eventos en Kafka...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());

      if (event.event_type === "DRIVER_ASSIGNED") {
        console.log("📦 Pedido asignado, repartidor en camino:", event);

        setTimeout(async () => {
          const arrivingEvent = {
            event_type: "DRIVER_ARRIVING",
            order_id: event.order_id,
          };

          await producer.send({
            topic: "order_events",
            messages: [{ key: event.order_id, value: JSON.stringify(arrivingEvent) }],
          });

          console.log("🚕 DRIVER_ARRIVING publicado:", arrivingEvent);
        }, 10000);

        setTimeout(async () => {
          const deliveredEvent = {
            event_type: "ORDER_DELIVERED",
            order_id: event.order_id,
          };

          await producer.send({
            topic: "order_events",
            messages: [{ key: event.order_id, value: JSON.stringify(deliveredEvent) }],
          });

          console.log("✅ ORDER_DELIVERED publicado:", deliveredEvent);
        }, 15000);
      }
    },
  });
}

start();
