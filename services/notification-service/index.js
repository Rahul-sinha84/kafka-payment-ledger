import "./env.js";

import { Kafka } from "kafkajs";
import { setupMongoConnection } from "./src/db/connection.js";
import { KafkaTopics, MAX_PROCESS_RETRY_COUNT } from "../config.js";
import {
  handleOrderCreatedMessage,
  handlePaymentCompletedMessage,
  handlePaymentFailedMessage,
} from "./src/kafka.js";

const kafka = new Kafka({
  clientId: "notification-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "notification-service" });
const producer = kafka.producer();

const sleep = async (ms) => {
  await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
};

const processWithRetry = async (
  messageHandler,
  producer,
  message,
  topic,
  maxRetry = MAX_PROCESS_RETRY_COUNT,
) => {
  try {
    for (let attempt = 1; attempt <= maxRetry; attempt += 1) {
      try {
        await messageHandler(message);
        return;
      } catch (err) {
        console.warn(
          `Attempt ${attempt}/${maxRetry} failed for topic: ${topic}`,
        );
        if (attempt < maxRetry) await sleep(2000 * attempt);
      }
    }

    // sending the message in the notification.dlq topic
    const { customerId, orderId, customerEmail } = JSON.parse(
      message.value.toString(),
    );
    const value = JSON.stringify({ customerEmail, customerId, orderId, topic });
    await producer.send({
      topic: KafkaTopics.NotificationDLQ,
      messages: [
        {
          key: message.key,
          value,
          headers: { ...message.headers, failedAt: new Date().toString() },
        },
      ],
    });
  } catch (err) {
    console.error({ err });
    throw err;
  }
};

const run = async () => {
  try {
    await setupMongoConnection();
    await consumer.connect();
    await producer.connect();

    consumer.subscribe({
      topics: [
        KafkaTopics.OrderCreated,
        KafkaTopics.PaymentCompleted,
        KafkaTopics.PaymentFailed,
      ],
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        switch (topic) {
          case KafkaTopics.OrderCreated: {
            await processWithRetry(
              handleOrderCreatedMessage,
              producer,
              message,
              topic,
            );
            break;
          }
          case KafkaTopics.PaymentCompleted: {
            await processWithRetry(
              handlePaymentCompletedMessage,
              producer,
              message,
              topic,
            );
            break;
          }
          case KafkaTopics.PaymentFailed: {
            await processWithRetry(
              handlePaymentFailedMessage,
              producer,
              message,
              topic,
            );
            break;
          }
          default: {
          }
        }
      },
    });
  } catch (err) {
    console.error({ err });
  }
};

run();
