export const OutboxEventStatus = {
  pending: "pending",
  sent: "sent",
};

export const KafkaTopics = {
  OrderCreated: "order.created",
  OrderCreatedDLQ: "order.created.dlq",
  PaymentCreated: "payment.created",
};

export const EventTypes = {
  OrderCreated: "OrderCreated",
};
