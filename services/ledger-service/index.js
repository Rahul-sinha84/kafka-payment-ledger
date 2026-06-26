import "./env.js";

import { setupMongoConnection } from "./src/db/connection.js";

import e from "express";
import ledgerRouter from "./src/routers/ledger.route.js";
import kafka from "./src/kafka/index.js";

const app = e();

app.use(e.json());

const PORT = process.env.LEDGER_SERVICE_PORT || 8001;

app.use("/ledger-service", ledgerRouter);

app.listen(PORT, async () => {
  await setupMongoConnection();
  // initializing kafka
  await kafka();
  console.info(`Ledger service server started on port: ${PORT}`);
});
