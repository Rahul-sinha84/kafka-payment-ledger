import { Router } from "express";
import mongoose from "mongoose";
import Order from "../db/models/order.js";
import Outbox from "../db/models/outbox.js";
import { EventTypes, KafkaTopics } from "../../../config.js";
import { reserveFunds } from "../ledgerClient.js";

const orderRouter = Router();

orderRouter.post("/", async (request, response) => {
  try {
    console.log(request.body);
    const { ObjectId } = mongoose.Types;
    const orderId = new ObjectId().toString();
    const { amount, customerId, customerEmail } = request.body;

    if (!amount || !customerId || !customerEmail) {
      const missingValues = [];
      if (!amount) missingValues.push("amount");
      if (!customerEmail) missingValues.push("customerEmail");
      if (!customerId) missingValues.push("customerId");

      return response.status(400).json({
        message: "Bad request",
        missingValues,
      });
    }

    // check the balance from the ledger service first
    const isReservedSuccessful = await reserveFunds({
      amount,
      customerId,
      orderId,
    });

    if (!isReservedSuccessful) {
      throw new Error("Balance reservation failed");
    }

    const session = await mongoose.startSession();
    try {
      session.startTransaction();

      const [order] = await Order.create(
        [
          {
            _id: orderId,
            amount,
            customerId,
          },
        ],
        { session },
      );

      await Outbox.create(
        [
          {
            aggregateId: order._id.toString(),
            topic: KafkaTopics.OrderCreated,
            eventType: EventTypes.OrderCreated,
            payload: {
              orderId,
              customerId,
              customerEmail,
              amount,
            },
          },
        ],
        { session },
      );

      await session.commitTransaction();

      response.status(201).json({ message: "Order Created", order });
    } catch (err) {
      await session.abortTransaction();
    } finally {
      session.endSession();
    }
  } catch (err) {
    console.error({ err });
    response.status(500).json({
      message: "Error in create Order API",
      err,
    });
  }
});

export default orderRouter;
