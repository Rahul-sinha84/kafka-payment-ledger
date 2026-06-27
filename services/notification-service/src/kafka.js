import Inbox from "./db/models/inbox.js";
import {
  getOrderCreatedEventId,
  getPaymentCompletedEventId,
  getPaymentFailedEventId,
} from "./utils.js";

const simulateNotification = (customerEmail, text) => {
  if (Math.random() < 0.3) throw new Error("Notification sending failure");
  console.log(`[+] Notification successful, ${customerEmail}: ${text}`);
  return true;
};

export const handleOrderCreatedMessage = async (message) => {
  const { orderId, customerEmail } = JSON.parse(message.value.toString());
  const eventId = getOrderCreatedEventId(orderId);

  // check if it's already processed
  // ? idempotency check
  const inboxEvent = await Inbox.findOne({ eventId }, { eventId: 1 });
  if (inboxEvent) return; // already processed, ignore

  // send the notification
  // ? sending notification first because, if we save first and send later
  // ? there is possibility of saving and notification failure
  // ? and ultimately not sending the notification (since we already saved it)
  // ? it's still better for the user to send more than one notification of same event,
  // ? than no notification at all
  simulateNotification(
    customerEmail,
    `Orders created successfully, orderId: ${orderId}`,
  );

  await Inbox.create({ eventId });
};

export const handlePaymentCompletedMessage = async (message) => {
  const { orderId, customerEmail } = JSON.parse(message.value.toString());
  const eventId = getPaymentCompletedEventId(orderId);

  // check if it's already processed
  // ? idempotency check
  const inboxEvent = await Inbox.findOne({ eventId }, { eventId: 1 });
  if (inboxEvent) return; // already processed, ignore

  // send the notification
  simulateNotification(
    customerEmail,
    `Order payment is successful, orderId: ${orderId}`,
  );

  await Inbox.create({ eventId });
};

export const handlePaymentFailedMessage = async (message) => {
  const { orderId, customerEmail } = JSON.parse(message.value.toString());
  const eventId = getPaymentFailedEventId(orderId);

  // check if it's already processed
  // ? idempotency check
  const inboxEvent = await Inbox.findOne({ eventId }, { eventId: 1 });
  if (inboxEvent) return; // already processed, ignore

  // send the notification
  simulateNotification(
    customerEmail,
    `Order payment is failed, orderId: ${orderId}`,
  );

  await Inbox.create({ eventId });
};
