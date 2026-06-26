import { Kafka } from "kafkajs";
import { KafkaTopics } from "../../../config.js";
import Ledger, { LedgerStatus, LedgerType } from "../db/models/ledger.js";
import { getRefundEventId, getReserveEventId } from "../utils.js";
import mongoose from "mongoose";
import Balance from "../db/models/balance.js";

const kafka = new Kafka({
  clientId: "ledger-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "ledger-service" });
const producer = kafka.producer();

const handlePaymentFailedMessage = async (message) => {
  try {
    const { orderId, customerId, amount } = JSON.parse(
      message.value?.toString(),
    );

    const session = await mongoose.startSession();

    try {
      session.startTransaction();
      // create the ledger of refund orderId
      const eventId = getRefundEventId(orderId);

      try {
        // ? idempotency check too, because eventId is unique,
        // ? throws error if add duplicate key
        await Ledger.create(
          [
            {
              eventId,
              ledgerType: LedgerType.CREDIT,
              orderId,
              customerId,
              amount,
            },
          ],
          { session },
        );
      } catch (err) {
        if (err?.code === 11000) return; // known duplication error, ignore
        throw err;
      }

      // increment the balance of the customer
      await Balance.findOneAndUpdate(
        {
          customerId,
        },
        {
          $inc: {
            balance: amount,
          },
        },
        { session },
      );

      await session.commitTransaction();
    } catch (err) {
      console.log({ err });
      await session.abortTransaction();
    } finally {
      await session.endSession();
    }
  } catch (err) {
    throw err;
  }
};

export default async () => {
  try {
    await producer.connect();
    await consumer.connect();

    consumer.subscribe({
      topics: Object.values(KafkaTopics),
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        switch (topic) {
          case KafkaTopics.PaymentCompleted: {
            // marking the ledger status to "CONFIRMED"
            // ? balance is already subtracted in the reserve api
            const { orderId } = JSON.parse(message.value.toString());
            // TODO how to find eventId from the message?
            const eventId = getReserveEventId(orderId);

            // ? no idempotency check here because this operation is idempotent already
            await Ledger.findOneAndUpdate(
              { eventId },
              { status: LedgerStatus.CONFIRMED },
            );
            break;
          }

          case KafkaTopics.PaymentFailed: {
            handlePaymentFailedMessage(message);
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
