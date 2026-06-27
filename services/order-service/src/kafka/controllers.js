import Order, { OrderStatus } from "../db/models/order.js";

export const handlePaymentCompletedMessage = async (message) => {
  const { orderId } = JSON.parse(message.value.toString());

  // atomic operation, is idempotent in itself
  await Order.findOneAndUpdate(
    { _id: orderId, status: OrderStatus.IN_PROCESS },
    {
      $set: {
        status: OrderStatus.COMPLETED,
      },
    },
  );
};

export const handlePaymentFailedMessage = async (message) => {
  const { orderId } = JSON.parse(message.value.toString());

  // atomic operation, is idempotent in itself
  await Order.findOneAndUpdate(
    { _id: orderId, status: OrderStatus.IN_PROCESS },
    {
      $set: {
        status: OrderStatus.FAILED,
      },
    },
  );
};
