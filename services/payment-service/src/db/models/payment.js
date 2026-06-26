import mongoose from "mongoose";

const PaymentSchema = new mongoose.Schema(
  {
    orderId: {
      type: String,
      required: true,
      unique: true,
    },
    amount: {
      type: Number,
      required: true,
    },
    status: {
      type: String,
      enum: ["completed", "failed"],
      required: true,
    },
  },
  {
    timestamps: true,
  },
);

export default mongoose.model("payment", PaymentSchema);
