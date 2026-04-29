const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function withRetry(task, { retries = 3, baseDelayMs = 500 } = {}) {
  let attempt = 0;
  while (attempt < retries) {
    try {
      return await task(attempt + 1);
    } catch (error) {
      attempt += 1;
      if (attempt >= retries) throw error;
      await sleep(baseDelayMs * (2 ** (attempt - 1)));
    }
  }
  throw new Error("Retry loop exited unexpectedly");
}
