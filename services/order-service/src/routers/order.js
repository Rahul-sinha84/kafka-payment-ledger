import { Router } from "express";
import mongoose from "mongoose";
import Order from "../db/models/order.js";
import Outbox from "../db/models/outbox.js";
import { EventTypes, KafkaTopics } from "../../../config.js";

const orderRouter = Router();

orderRouter.post("/", async (request, response) => {
  const session = await mongoose.startSession();
  try {
    session.startTransaction();
    console.log(request.body);
    const { amount, customerId } = request.body;

    const [order] = await Order.create(
      [
        {
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
            orderId: order._id.toString(),
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
    console.error({ err });
    throw new Error("Error in create Order API", err);
  } finally {
    session.endSession();
  }
});

export default orderRouter;
