import test from "node:test";
import assert from "node:assert/strict";
import { shouldRunRetry } from "./navUpdater.js";

test("non-retry schedules always run", () => {
  assert.equal(shouldRunRetry({ retryOnly: false, failed: false, retryDate: "", currentDate: "" }), true);
});

test("retry schedules do not run unless the previous attempt failed", () => {
  assert.equal(shouldRunRetry({ retryOnly: true, failed: false, retryDate: "2026-04-28", currentDate: "2026-04-28" }), false);
});

test("retry schedules run only for the same IST calendar day", () => {
  assert.equal(shouldRunRetry({ retryOnly: true, failed: true, retryDate: "2026-04-28", currentDate: "2026-04-28" }), true);
  assert.equal(shouldRunRetry({ retryOnly: true, failed: true, retryDate: "2026-04-28", currentDate: "2026-04-29" }), false);
});
