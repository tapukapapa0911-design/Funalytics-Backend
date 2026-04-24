import dotenv from "dotenv";

dotenv.config();

export const env = {
  port: Number(process.env.PORT || 4000),
  mongoUri: process.env.MONGODB_URI || "mongodb://127.0.0.1:27017/funalytics-live",
  nodeEnv: process.env.NODE_ENV || "development",
  amfiUrl: process.env.AMFI_URL || "https://www.amfiindia.com/spages/NAVAll.txt",
  requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 15000),
  cacheTtlMs: Number(process.env.CACHE_TTL_MS || 60000)
};
