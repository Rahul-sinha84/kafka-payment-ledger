import { Router } from "express";
import Ledger, { LedgerStatus, LedgerType } from "../db/models/ledger.js";
import mongoose from "mongoose";
import Balance from "../db/models/balance.js";
import { getReserveEventId } from "../utils.js";

const ledgerRouter = Router();

ledgerRouter.post("/reserve", async (request, response) => {
  try {
    console.debug(
      `[+] Incoming request with body: ${JSON.stringify(request.body)}`,
    );
    const { customerId, amount, orderId } = request.body;
    const eventId = getReserveEventId(orderId);

    // checking if already reserved (idempotency)
    const existing = await Ledger.findOne({ eventId }, { eventId: 1 });
    if (existing) {
      // already reserve
      return response.status(200).json({ reserved: true });
    }

    // single atomic transaction for checking and updating the balance and
    // create a ledger entry
    const session = await mongoose.startSession();

    try {
      session.startTransaction();
      const balanceRecord = await Balance.findOneAndUpdate(
        {
          customerId,
          balance: {
            $gte: amount,
          },
        },
        {
          $inc: {
            balance: -amount,
          },
        },
        {
          new: true,
          session,
        },
      );

      if (!balanceRecord) {
        await session.abortTransaction();
        return response
          .status(402)
          .json({ reserved: false, reason: "insufficient balance" });
      }

      await Ledger.create(
        [
          {
            eventId,
            ledgerType: LedgerType.DEBIT,
            orderId,
            customerId,
            status: LedgerStatus.RESERVED,
            amount,
          },
        ],
        { session },
      );

      await session.commitTransaction();

      return response
        .status(200)
        .json({ reserved: true, balance: balanceRecord.balance });
    } catch (err) {
      await session.abortTransaction();
      throw err;
    } finally {
      session.endSession();
    }
  } catch (err) {
    console.error({ err });
    response.status(500).json({
      err,
      message: err?.message ?? "Something went wrong",
    });
  }
});

export default ledgerRouter;
