import { Kafka } from "kafkajs";
import { KafkaTopics } from "../config.js";

const kafka = new Kafka({
  clientId: "dlq-consumer",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "dlq-consumer" });

const run = async () => {
  try {
    await consumer.connect();

    consumer.subscribe({
      topic: KafkaTopics.OrderCreatedDLQ,
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: ({ topic, message }) => {
        switch (topic) {
          case KafkaTopics.OrderCreatedDLQ: {
            const { orderId, amount } = JSON.parse(message.value.toString());

            console.info(
              `Event arrived in DLQ, orderId: ${orderId} and amount: ${amount}`,
              message.headers,
            );
            break;
          }
        }
      },
    });
  } catch (err) {
    console.error({ err });
  }
};

run();
