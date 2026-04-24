import cors from "cors";
import express from "express";
import morgan from "morgan";
import { fundRoutes } from "./routes/fundRoutes.js";

export function createApp() {
  const app = express();
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
