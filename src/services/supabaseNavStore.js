import { supabase } from "../supabaseClient.js";
import { logger } from "../utils/logger.js";

export function isSupabaseAvailable() {
  return Boolean(supabase);
}

export async function readNavFromSupabase() {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase
      .from("nav_cache")
      .select("nav_date, updated_at, snapshot_json")
      .eq("id", 1)
      .maybeSingle();

    if (error) {
      logger.warn("supabase-nav-read-failed", { error: error.message });
      return null;
    }

    if (!data) return null;
    return {
      nav_date: String(data.nav_date || "").trim(),
      updated_at: String(data.updated_at || "").trim(),
      snapshot_json: typeof data.snapshot_json === "string" ? data.snapshot_json : ""
    };
  } catch (error) {
    logger.warn("supabase-nav-read-error", { error: String(error?.message || error) });
    return null;
  }
}

export async function writeNavToSupabase(navDate, snapshotJson) {
  if (!supabase) return false;
  try {
    const payload = {
      id: 1,
      nav_date: String(navDate || "").trim(),
      updated_at: new Date().toISOString(),
      snapshot_json: String(snapshotJson || "")
    };
    const { error } = await supabase
      .from("nav_cache")
      .upsert(payload, { onConflict: "id" });

    if (error) {
      logger.warn("supabase-nav-write-failed", { error: error.message, navDate: payload.nav_date });
      return false;
    }
    return true;
  } catch (error) {
    logger.warn("supabase-nav-write-error", {
      error: String(error?.message || error),
      navDate: String(navDate || "").trim()
    });
    return false;
  }
}
