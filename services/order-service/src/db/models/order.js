import mongoose from "mongoose";

export const OrderStatus = {
  IN_PROCESS: "in_process",
  COMPLETED: "completed",
  FAILED: "failed",
};

const OrderSchema = new mongoose.Schema(
  {
    amount: {
      type: Number,
      required: true,
    },
    customerId: {
      required: true,
      type: String,
    },
    status: {
      type: String,
      enum: Object.values(OrderStatus),
      required: true,
      default: OrderStatus.IN_PROCESS,
    },
  },
  {
    timestamps: true,
  },
);

export default mongoose.model("orders", OrderSchema);
