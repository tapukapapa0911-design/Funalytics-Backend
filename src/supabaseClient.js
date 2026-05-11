import ws from 'ws';
import { createClient } from "@supabase/supabase-js";
import { env } from "./config/env.js";
import { logger } from "./utils/logger.js";

let warnedMissingConfig = false;

const hasConfig = Boolean(env.supabaseUrl && env.supabaseKey);

if (!hasConfig && !warnedMissingConfig) {
  logger.warn("supabase-config-missing");
  warnedMissingConfig = true;
}

export const supabase = hasConfig
  ? createClient(env.supabaseUrl, env.supabaseKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false
      },
      realtime: { transport: ws }
    })
  : null;
