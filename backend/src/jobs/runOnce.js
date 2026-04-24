import { connectToDatabase } from "../config/db.js";
import { triggerNavUpdate } from "./navUpdater.js";

await connectToDatabase();
await triggerNavUpdate();
process.exit(0);
