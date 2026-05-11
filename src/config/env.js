import dotenv from "dotenv";

dotenv.config();

export const env = {
  port: Number(process.env.PORT || 3000),
  mongoUri: String(process.env.MONGODB_URI || "").trim(),
  nodeEnv: process.env.NODE_ENV || "development",
  amfiUrl: process.env.AMFI_URL || "https://www.amfiindia.com/spages/NAVAll.txt",
  requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 10000),
  cacheTtlMs: Number(process.env.CACHE_TTL_MS || 60000)
};
