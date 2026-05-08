function sanitizeMeta(meta) {
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return meta;
  const out = {};
  for (const [key, value] of Object.entries(meta)) {
    if (value === null || value === undefined) continue;
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      out[key] = value;
      continue;
    }
    if (Array.isArray(value)) {
      out[`${key}Count`] = value.length;
      continue;
    }
    if (value instanceof Date) {
      out[key] = value.toISOString();
      continue;
    }
    if (typeof value === "object") {
      out[key] = "[object]";
    }
  }
  return out;
}

function write(level, message, meta) {
  const payload = {
    level,
    at: new Date().toISOString(),
    message
  };
  const safeMeta = sanitizeMeta(meta);
  if (safeMeta && (typeof safeMeta !== "object" || Object.keys(safeMeta).length > 0)) {
    payload.meta = safeMeta;
  }
  const line = JSON.stringify(payload);
  if (level === "error") {
    console.error(line);
    return;
  }
  if (level === "warn") {
    console.warn(line);
    return;
  }
  console.log(line);
}

export const logger = {
  info(message, meta) {
    write("info", message, meta);
  },
  warn(message, meta) {
    write("warn", message, meta);
  },
  error(message, meta) {
    write("error", message, meta);
  }
};
