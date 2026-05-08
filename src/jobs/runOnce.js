import { connectToDatabase } from "../config/db.js";
import { syncNavData } from "./navUpdater.js";

await connectToDatabase();
await syncNavData();
process.exit(0);
