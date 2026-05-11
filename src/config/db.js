import mongoose from "mongoose";
import { env } from "./env.js";
import { logger } from "../utils/logger.js";

export async function connectToDatabase() {
  if (!env.mongoUri) {
    logger.info("MongoDB disabled; using file cache mode");
    return false;
  }
  mongoose.set("strictQuery", true);
  await mongoose.connect(env.mongoUri, {
    serverSelectionTimeoutMS: env.requestTimeoutMs
  });
  logger.info("MongoDB connected");
  return true;
}
