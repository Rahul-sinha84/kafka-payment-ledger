export const OutboxEventStatus = {
  pending: "pending",
  sent: "sent",
};

export const KafkaTopics = {
  OrderCreated: "order.created",
  OrderCreatedDLQ: "order.created.dlq",
  PaymentCompleted: "payment.completed",
  PaymentFailed: "payment.failed",
};

export const EventTypes = {
  OrderCreated: "OrderCreated",
};
