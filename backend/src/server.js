import { createApp } from "./app.js";
import { env } from "./config/env.js";
import { connectToDatabase } from "./config/db.js";
import { startNavCron, triggerNavUpdate } from "./jobs/navUpdater.js";
import { logger } from "./utils/logger.js";
import mongoose from "mongoose";

async function bootstrap() {
  await connectToDatabase();

  const app = createApp();
  const server = app.listen(env.port, () => {
    logger.info(`Live NAV backend listening on port ${env.port}`);
  });

  server.on("error", (error) => {
    logger.error("HTTP server failed", error);
    process.exit(1);
  });

  startNavCron();

  try {
    await triggerNavUpdate();
  } catch (error) {
    logger.error("Initial NAV update failed", error.message);
  }

  const shutdown = async (signal) => {
    logger.info(`Received ${signal}. Shutting down gracefully...`);
    await new Promise((resolve) => server.close(resolve));
    try {
      await mongoose.connection.close(false);
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
