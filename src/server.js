import { createApp } from "./app.js";
import { env } from "./config/env.js";
import { connectToDatabase } from "./config/db.js";
import { ensureSnapshotFile, readSnapshotFile } from "./services/snapshotStore.js";
import { triggerNavUpdate } from "./jobs/navUpdater.js";
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
    const snapshot = readSnapshotFile();
    const generatedAt = snapshot?.generatedAt ? new Date(snapshot.generatedAt) : null;
    const ageMs = generatedAt ? Date.now() - generatedAt.getTime() : Infinity;
    const TWELVE_HOURS = 12 * 60 * 60 * 1000;

    if (ageMs > TWELVE_HOURS) {
      logger.info("Stale NAV snapshot on startup, triggering auto-refresh");
      triggerNavUpdate().catch((err) => {
        logger.warn(`Startup auto-refresh failed: ${err?.message || err}`);
      });
    } else {
      logger.info("NAV snapshot is fresh, skipping startup auto-refresh");
    }
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
