import mongoose from "mongoose";
import Order from "./models/order.js";

let isConnected = false;
let connectionAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 5;

const path = process.env.MONGO_DB_URI;

if (!path) {
  throw new Error("MONGO_DB_URI env is not set!");
}

const options = {
  autoIndex: true,
  serverSelectionTimeoutMS: 5000,
  socketTimeoutMS: 45000,
  family: 4,
  maxPoolSize: 10,
  retryWrites: true,
  retryReads: true,
  connectTimeoutMS: 10000,
  heartbeatFrequencyMS: 10000,
};

const getMongoConnectionStatus = () => {
  if (!mongoose.connection)
    throw new Error("MongoDB connection not initialized!");

  return {
    poolSize: mongoose.connection?.getClient()?.options?.maxPoolSize ?? 0,
    currentConnections:
      mongoose.connection?.getClient()?.options?.maxPoolSize ?? 0,
    host: mongoose.connection?.host ?? "unknown",
    name: mongoose.connection?.name ?? "name",
    readyState: ["disconnected", "connected", "connecting", "disconnecting"][
      mongoose.connection?.readyState ?? 0
    ],
    retryAttempts: connectionAttempts,
  };
};

const reconnect = async () => {
  if (connectionAttempts >= MAX_RECONNECT_ATTEMPTS)
    throw new Error("Max reconnection attempts reached");

  connectionAttempts += 1;
  await new Promise((resolve) => {
    setTimeout(resolve, 1000 * connectionAttempts);
  });

  return setupMongoConnection(false);
};

export const setupMongoConnection = async (syncIndexes = false) => {
  try {
    if (isConnected) return mongoose.connection;

    mongoose.set("strictQuery", false);
    mongoose.set("runValidators", true);

    mongoose.connection.on("connected", () => {
      isConnected = true;
      connectionAttempts = 0;
      console.info(
        getMongoConnectionStatus(),
        "MongoDB connected successfully",
      );
    });

    await mongoose.connect(path, options);

    mongoose.connection.on("error", (err) => {
      console.error({ err }, "MongoDB connection error");
      isConnected = false;
    });

    mongoose.connection.on("disconnected", () => {
      console.warn("MongoDB disconnected");
      isConnected = false;
      reconnect();
    });

    mongoose.connection.on("reconnected", () => {
      console.info("MongoDB reconnected");
    });

    mongoose.connection.on("connecting", () => {
      console.debug("Connecting to MongoDB...");
    });

    if (syncIndexes) {
      await Order.syncIndexes();
      console.info("MongoDB indexes synced successfully");
    }

    return mongoose.connection;
  } catch (err) {
    console.error({ err, path });
    throw new Error("MongoDB connection failure", err);
  }
};

export const gracefulShutdown = async (timeout = 5000) => {
  try {
    const closePromise = mongoose.connection.close();
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error("Shutdown timeout")), timeout);
    });

    await Promise.race([closePromise, timeoutPromise]);
    isConnected = false;
    console.info("MongoDB connection closed gracefully");
  } catch (err) {
    console.error({ err }, "Error during MongoDB shutdown");
    throw new Error(`Failed to shutdown MongoDB connection: ${err?.message}`);
  }
};
