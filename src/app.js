import { createRequire } from "node:module";
import cors from "cors";
import express from "express";
import morgan from "morgan";
import { fundRoutes } from "./routes/fundRoutes.js";

const require = createRequire(import.meta.url);

function resolveCompressionMiddleware() {
  try {
    return require("compression")();
  } catch (error) {
    console.warn("compression middleware unavailable, continuing without gzip", error?.message || error);
    return (_req, _res, next) => next();
  }
}

export function createApp() {
  const app = express();
  app.use(cors());
  app.use(express.json());
  app.use("/api", routes);
  app.use(resolveCompressionMiddleware());
  app.use(cors({ origin: "*" }));
  app.use(express.json());
  app.use(morgan("dev"));
  app.use(fundRoutes);
  app.use((err, _req, res, _next) => {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  });
  return app;
}
