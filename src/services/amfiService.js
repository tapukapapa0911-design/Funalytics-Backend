import axios from "axios";
import { env } from "../config/env.js";
import { parseNavAllText } from "../utils/parser.js";
import { withRetry } from "../utils/retry.js";

const http = axios.create({
  timeout: env.requestTimeoutMs,
  responseType: "text"
});

export async function fetchAmfiNavFeed() {
  return withRetry(async () => {
    const response = await http.get(env.amfiUrl, {
      headers: {
        "User-Agent": "Funalytics-Live-Backend/1.0"
      }
    });
    return parseNavAllText(response.data);
  }, { retries: 3, baseDelayMs: 750 });
}
