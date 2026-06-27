import mongoose from "mongoose";

const InboxSchema = new mongoose.Schema(
  {
    eventId: {
      type: String,
      required: true,
      unique: true,
    },
    processedAt: {
      type: Date,
      default: new Date(),
    },
  },
  { timestamps: true },
);

export default mongoose.model("inbox", InboxSchema);
