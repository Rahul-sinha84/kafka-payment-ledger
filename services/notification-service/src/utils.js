import { KafkaTopics } from "../../config.js";

export const getOrderCreatedEventId = (orderId) =>
  `${KafkaTopics.OrderCreated}-${orderId}`;
export const getPaymentCompletedEventId = (orderId) =>
  `${KafkaTopics.PaymentCompleted}-${orderId}`;
export const getPaymentFailedEventId = (orderId) =>
  `${KafkaTopics.PaymentFailed}-${orderId}`;
