import mongoose from "mongoose";
import { OutboxEventStatus } from "../../../../config.js";

const OutboxSchema = new mongoose.Schema(
  {
    aggregateId: {
      type: String,
      required: true,
    },
    topic: {
      required: true,
      type: String,
    },
    eventType: {
      required: true,
      type: String,
    },
    payload: {
      type: Object,
    },
    status: {
      type: String,
      enum: OutboxEventStatus,
      default: OutboxEventStatus.pending,
    },
    sentAt: {
      type: Date,
    },
  },
  { timestamps: true },
);

export default mongoose.model("outbox", OutboxSchema);
