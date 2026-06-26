import { KafkaTopics } from "../config.js";
import "./env.js";

import { setupMongoConnection } from "./src/db/connection.js";
import { Kafka } from "kafkajs";
import InboxEvent from "./src/db/models/inboxEvent.js";
import mongoose from "mongoose";
import Payment from "./src/db/models/payment.js";
import { PaymentDeclinedError } from "./src/error.js";

const MAX_RETRY_COUNT = 3;

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "payment-service" });
const producer = kafka.producer();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const chargeCustomer = () => {
  // just for simulation
  // ? payment declined error: error is clearly defined from the payment provider
  // ? other error: unknown error, occurred during network calls, etc
  if (Math.random() <= 0.15) throw new PaymentDeclinedError("Payment failure");
  else if (Math.random() < 0.3) throw new Error("Payment processing failed");
  return true;
};

const handleOrderCreatedMessage = async (message) => {
  try {
    const eventId = message.headers?.eventId?.toString();
    const { orderId, amount } = JSON.parse(message.value.toString());
    console.debug(`Order created message received: ${orderId}`);

    // checking if this event is already processed
    // * idempotency check
    const inboxEvent = await InboxEvent.findOne({ eventId });
    if (inboxEvent) {
      console.log("Event already processed, exiting...");
      return;
    }

    // simulating the payment (fake payment)
    chargeCustomer();

    // adding the payment and inbox for this order
    const session = await mongoose.startSession();
    try {
      session.startTransaction();
      await Payment.create(
        [
          {
            amount,
            orderId,
            status: "completed",
          },
        ],
        { session },
      );

      await InboxEvent.create([{ eventId }], session);

      await session.commitTransaction();
    } catch (err) {
      await session.abortTransaction();
      throw err;
    } finally {
      session.endSession();
    }

    // announce in the payment.created topic
    await producer.send({
      topic: KafkaTopics.PaymentCompleted,
      messages: [
        {
          value: JSON.stringify({ orderId, amount }),
          key: orderId,
        },
      ],
    });
  } catch (err) {
    throw err;
  }
};

const processWithRetry = async (message, maxRetry = MAX_RETRY_COUNT) => {
  for (let i = 1; i <= maxRetry; i++) {
    try {
      await handleOrderCreatedMessage(message);
      return;
    } catch (err) {
      console.warn(`Attempt ${i}/${maxRetry} failed: ${err?.message || err}`);
      if (i < maxRetry) await sleep(2000);

      if (err instanceof PaymentDeclinedError) {
        // sending the message to payment.failed topic
        // ? no need to retry as it's a known error
        // retries are done in case of unknown errors
        await producer.send({
          topic: KafkaTopics.PaymentFailed,
          messages: [
            {
              key: message.key,
              value: message.value,
              headers: { ...message.headers },
            },
          ],
        });
        return;
      }
    }
  }

  // all the attempts failed, publish the event to dlq
  await producer.send({
    topic: KafkaTopics.OrderCreatedDLQ,
    messages: [
      {
        key: message.key,
        value: message.value,
        headers: { ...message.headers, failedAt: new Date().toString() },
      },
    ],
  });
  console.warn("Exhausted retries, moved to DLQ");
};

const run = async () => {
  try {
    await setupMongoConnection();
    await producer.connect();
    await consumer.connect();

    consumer.subscribe({
      topic: KafkaTopics.OrderCreated,
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        switch (topic) {
          case KafkaTopics.OrderCreated: {
            processWithRetry(message);
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
