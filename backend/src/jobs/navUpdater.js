import cron from "node-cron";
import { runNavIngestion } from "../services/navIngestionService.js";
import { logger } from "../utils/logger.js";

let running = false;

export async function triggerNavUpdate() {
  if (running) {
    logger.warn("NAV update skipped because a run is already in progress");
    return null;
  }
  running = true;
  try {
    return await runNavIngestion();
  } finally {
    running = false;
  }
}

export function startNavCron() {
  cron.schedule("30 22 * * *", async () => {
    try {
      await triggerNavUpdate();
    } catch (error) {
      logger.error("Scheduled NAV update failed", error.message);
    }
  }, {
    timezone: "Asia/Kolkata"
  });
  logger.info("NAV cron scheduled for 10:30 PM IST");
}
