import mongoose from "mongoose";

const BalanceSchema = new mongoose.Schema(
  {
    balance: {
      type: Number,
      required: true,
    },
    customerId: {
      required: true,
      type: String,
      unique: true,
    },
  },
  {
    timestamps: true,
  },
);

export default mongoose.model("balances", BalanceSchema);
