window.LiveDataVersion = window.LiveDataVersion || {};

window.LiveDataVersion.cache = (() => {
  const readJson = (key) => {
    try {
      const raw = localStorage.getItem(key);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  };

  const writeJson = (key, value) => {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch {
      /* ignore storage quota errors */
    }
  };

  const remove = (key) => {
    try {
      localStorage.removeItem(key);
    } catch {
      /* ignore */
    }
  };

  const isFresh = (entry, ttlMs) => {
    if (!entry || !entry.savedAt) return false;
    return Date.now() - entry.savedAt <= ttlMs;
  };

  const purgeStaleByPrefix = (prefix, maxAgeMs) => {
    try {
      const now = Date.now();
      const keysToRemove = [];
      for (let index = 0; index < localStorage.length; index += 1) {
        const key = localStorage.key(index);
        if (!key || !key.startsWith(prefix)) continue;
        const entry = readJson(key);
        if (!entry?.savedAt || (now - Number(entry.savedAt)) > maxAgeMs) {
          keysToRemove.push(key);
        }
      }
      keysToRemove.forEach((key) => remove(key));
      return keysToRemove;
    } catch {
      return [];
    }
  };

  const purgeKeysOlderThan = (keys, maxAgeMs) => {
    try {
      const now = Date.now();
      const removed = [];
      keys.forEach((key) => {
        const entry = readJson(key);
        if (!entry?.savedAt || (now - Number(entry.savedAt)) > maxAgeMs) {
          remove(key);
          removed.push(key);
        }
      });
      return removed;
    } catch {
      return [];
    }
  };

  return { readJson, writeJson, remove, isFresh, purgeStaleByPrefix, purgeKeysOlderThan };
})();
