export const OutboxEventStatus = {
  pending: "pending",
  sent: "sent",
};

export const KafkaTopics = {
  OrderCreated: "orders.created",
  OrderCreatedDLQ: "orders.created.dlq",
  PaymentCompleted: "payments.completed",
  PaymentFailed: "payments.failed",
  NotificationDLQ: "notifications.dlq",
  OrderStatusDLQ: "orders.status.dlq",
};

export const EventTypes = {
  OrderCreated: "OrderCreated",
};

export const MAX_PROCESS_RETRY_COUNT = 3;
