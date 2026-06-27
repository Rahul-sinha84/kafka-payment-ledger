import { Kafka } from "kafkajs";
import { KafkaTopics, MAX_PROCESS_RETRY_COUNT } from "../../../config.js";
import {
  handlePaymentCompletedMessage,
  handlePaymentFailedMessage,
} from "./controllers.js";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "order-service" });
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
  for (let attempt = 1; attempt <= maxRetry; attempt += 1) {
    try {
      await messageHandler(message);
      return;
    } catch (err) {
      console.warn(`Attempt ${attempt}/${maxRetry} failed for topic: ${topic}`);
      if (attempt < maxRetry) await sleep(2000 * attempt);
    }
  }

  // sending the message to order.status.dlq
  await producer.send({
    topic: KafkaTopics.OrderStatusDLQ,
    messages: [
      {
        key: message.key,
        value: message.value,
        headers: { ...message.headers, failedAt: new Date().toString(), topic },
      },
    ],
  });
};

const run = async () => {
  try {
    await consumer.connect();
    await producer.connect();

    consumer.subscribe({
      topics: [KafkaTopics.PaymentCompleted, KafkaTopics.PaymentFailed],
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        switch (topic) {
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

export default run;
