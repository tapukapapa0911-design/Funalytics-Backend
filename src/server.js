import { createApp } from "./app.js";
import { env } from "./config/env.js";
import { connectToDatabase } from "./config/db.js";
import { ensureSnapshotFile, readSnapshotFile, writeSnapshotFile } from "./services/snapshotStore.js";
import { triggerNavUpdate } from "./jobs/navUpdater.js";
import { isSupabaseAvailable, readNavFromSupabase } from "./services/supabaseNavStore.js";
import { logger } from "./utils/logger.js";
import mongoose from "mongoose";

async function restoreSnapshotFromSupabaseIfNewer() {
  if (!isSupabaseAvailable()) {
    logger.info("Supabase unavailable, using local file cache");
    return;
  }

  const remote = await readNavFromSupabase();
  if (!remote?.nav_date || !remote?.snapshot_json) {
    logger.info("Supabase unavailable, using local file cache");
    return;
  }

  const local = readSnapshotFile();
  const localDate = String(local?.latestDate || "").trim();
  if (localDate && localDate >= remote.nav_date) {
    return;
  }

  try {
    const parsedSnapshot = JSON.parse(remote.snapshot_json);
    writeSnapshotFile(parsedSnapshot);
    logger.info(`NAV restored from Supabase: ${remote.nav_date}`);
  } catch (error) {
    logger.warn(`Supabase NAV restore failed, using local file cache: ${error?.message || error}`);
  }
}

async function bootstrap() {
  ensureSnapshotFile();
  await restoreSnapshotFromSupabaseIfNewer();
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
    const isSnapshotEmpty = !snapshot.generatedAt || !snapshot.latestDate || !Array.isArray(snapshot.items) || !snapshot.items.length;
    if (isSnapshotEmpty) {
      logger.info("Snapshot empty on startup, triggering immediate NAV warmup");
      triggerNavUpdate().catch((error) => {
        logger.warn(`Startup NAV warmup failed: ${error?.message || error}`);
      });
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
