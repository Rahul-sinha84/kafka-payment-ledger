import mongoose from "mongoose";

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
  },
  {
    timestamps: true,
  },
);

export default mongoose.model("orders", OrderSchema);
