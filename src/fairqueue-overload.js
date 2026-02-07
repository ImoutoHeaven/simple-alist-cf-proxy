export const nextOverloadDelayMs = (streak, options = {}) => {
  const base = 500;
  const step = 500;
  const max = 2000;
  const n = Number.isFinite(streak) && streak > 0 ? Math.floor(streak) : 0;
  const boundedBaseDelay = Math.min(max, base + step * n);

  if (!options || options.jitter !== true) {
    return boundedBaseDelay;
  }

  const randomFn = typeof options.random === 'function' ? options.random : Math.random;
  const jitterMaxMsRaw = Number(options.jitterMaxMs);
  const jitterMaxMs = Number.isFinite(jitterMaxMsRaw) && jitterMaxMsRaw > 0
    ? Math.floor(jitterMaxMsRaw)
    : 100;
  const jitterCap = Math.max(0, Math.min(jitterMaxMs, max - boundedBaseDelay));
  if (!jitterCap) {
    return boundedBaseDelay;
  }
  const normalizedRandom = Math.min(1, Math.max(0, Number(randomFn()) || 0));
  const jitter = Math.min(jitterCap, Math.floor(normalizedRandom * (jitterCap + 1)));
  return Math.min(max, boundedBaseDelay + jitter);
};
