// for importing the .env file before starting the server
// ? on a different file because the imports will be hoisted
// ? and the code will run later
import "./env.js";

import e from "express";
import { setupMongoConnection } from "./src/db/connection.js";
import orderRouter from "./src/routers/order.js";

const app = e();
const PORT = process.env.PAYMENT_SERVICE_PORT || 8000;

app.use(e.json());

app.use("/order-service", orderRouter);

app.listen(PORT, async () => {
  try {
    await setupMongoConnection();
    console.info("Server started on", PORT);
  } catch (err) {
    console.log({ err }, "Error starting the server");
  }
});
