import "./env.js";

import { setupMongoConnection } from "./src/db/connection.js";
import { Kafka } from "kafkajs";
import Outbox from "./src/db/models/outbox.js";
import { OutboxEventStatus } from "../config.js";

const kafka = new Kafka({
  clientId: "relay-service",
  brokers: ["localhost:9094"],
});

const producer = kafka.producer();

const relayService = async () => {
  try {
    const outboxes = await Outbox.find({ status: OutboxEventStatus.pending });

    for (const outbox of outboxes) {
      try {
        await producer.send({
          topic: outbox.topic,
          messages: [
            {
              key: outbox.aggregateId,
              value: JSON.stringify(outbox.payload),
              headers: { outboxId: outbox._id.toString() }, // consumer will use this to dedupe
            },
          ],
        });

        outbox.status = OutboxEventStatus.sent;
        outbox.sentAt = new Date();

        await outbox.save();
      } catch (err) {
        console.error(
          `Publish failed for ${outbox._id}, will retry next tick:`,
          err.message,
        );
        // deliberately do nothing else — leave status as 'pending'
      }
    }
  } catch (err) {
    throw err;
  }
};

const run = async () => {
  try {
    await setupMongoConnection();
    await producer.connect();

    console.info("Producer connected to kafka");

    setInterval(relayService, 5000);
  } catch (err) {
    console.error(err);
  }
};

run();
