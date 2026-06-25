import { Kafka } from "kafkajs";
import { KafkaTopics } from "../config.js";

const kafka = new Kafka({
  clientId: "log-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "log-service" });

const run = async () => {
  try {
    await consumer.connect();

    consumer.subscribe({
      topics: Object.values(KafkaTopics),
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (topic === KafkaTopics.OrderCreated) {
          const { orderId, amount } = JSON.parse(message.value.toString());

          console.log(`Order created for ${orderId}, of amount: ${amount}.`);
        }
      },
    });
  } catch (err) {
    console.error(err);
  }
};

run();
