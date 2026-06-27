import axios from "axios";
import { MAX_PROCESS_RETRY_COUNT } from "../../config.js";

const LEDGER_URL = `http://localhost:${process.env.LEDGER_SERVICE_PORT}/ledger-service`;

const sleep = async (ms) => {
  await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
};

export const reserveFunds = async (
  payload,
  maxRetries = MAX_PROCESS_RETRY_COUNT,
) => {
  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      console.info("[+] Attempting to call ledger service");
      const response = await axios.post(`${LEDGER_URL}/reserve`, {
        ...payload,
      });
      const { reserved } = await response.data;

      if (reserved) return true;
    } catch (err) {
      console.warn(`Attempt ${attempt}/${maxRetries} failed`, err?.message);

      // exponential backoff retrial
      if (attempt < maxRetries) await sleep(1000 * attempt);
      else throw err;
    }
  }

  return false;
};
