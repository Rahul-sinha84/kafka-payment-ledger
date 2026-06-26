import mongoose from "mongoose";

export const LedgerStatus = {
  RESERVED: "reserved",
  CONFIRMED: "confirmed",
};

export const LedgerType = {
  CREDIT: "credit",
  DEBIT: "debit",
};

const LedgerSchema = new mongoose.Schema(
  {
    eventId: {
      required: true,
      type: String,
      unique: true,
    },
    ledgerType: {
      required: true,
      type: String,
      enum: Object.values(LedgerType),
    },
    orderId: {
      required: true,
      type: String,
    },
    customerId: {
      type: String,
      required: true,
    },
    status: {
      required: false,
      enum: Object.values(LedgerStatus),
      type: String,
    },
    amount: {
      required: true,
      type: Number,
    },
  },
  { timestamps: true },
);

export default mongoose.model("ledger", LedgerSchema);
