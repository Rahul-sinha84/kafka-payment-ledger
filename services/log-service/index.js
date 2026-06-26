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
        switch (topic) {
          case KafkaTopics.OrderCreated: {
            const { orderId, amount } = JSON.parse(message.value.toString());

            console.log(`Order created for ${orderId}, of amount: ${amount}.`);
            break;
          }
          case KafkaTopics.OrderCreatedDLQ: {
            const { orderId, amount } = JSON.parse(message.value.toString());

            console.log(`Order reached DLQ:${orderId}, of amount: ${amount}.`);
            break;
          }
          case KafkaTopics.PaymentCreated: {
            const { orderId, amount } = JSON.parse(message.value.toString());

            console.log(
              `Payment created for ${orderId}, of amount: ${amount}.`,
            );
            break;
          }

          default: {
          }
        }
      },
    });
  } catch (err) {
    console.error(err);
  }
};

run();
