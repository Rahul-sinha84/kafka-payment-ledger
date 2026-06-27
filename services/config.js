export const OutboxEventStatus = {
  pending: "pending",
  sent: "sent",
};

export const KafkaTopics = {
  OrderCreated: "orders.created",
  OrderCreatedDLQ: "orders.created.dlq",
  PaymentCompleted: "payments.completed",
  PaymentFailed: "payments.failed",
};

export const EventTypes = {
  OrderCreated: "OrderCreated",
};
