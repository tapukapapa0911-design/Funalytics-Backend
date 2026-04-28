import { createApp } from "./app.js";
import { env } from "./config/env.js";
import { connectToDatabase } from "./config/db.js";
import { ensureSnapshotFile } from "./services/snapshotStore.js";
import { logger } from "./utils/logger.js";
import mongoose from "mongoose";

async function bootstrap() {
  ensureSnapshotFile();
  try {
    await connectToDatabase();
  } catch (error) {
    logger.warn(`MongoDB unavailable, continuing with file cache mode: ${error?.message || error}`);
  }

  const app = createApp();
  const port = process.env.PORT || 3000;
  const server = app.listen(port, () => {
    logger.info(`Server started on port ${port}`);
  });

  server.on("error", (error) => {
    logger.error("HTTP server failed", error);
    process.exit(1);
  });

  const shutdown = async (signal) => {
    logger.info(`Received ${signal}. Shutting down gracefully...`);
    await new Promise((resolve) => server.close(resolve));
    try {
      if (mongoose.connection.readyState === 1) {
        await mongoose.connection.close(false);
      }
    } catch (error) {
      logger.warn("MongoDB close during shutdown failed", error?.message || error);
    }
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

bootstrap().catch((error) => {
  logger.error("Server bootstrap failed", error);
  process.exit(1);
});
