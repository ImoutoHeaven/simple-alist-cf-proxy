export const nextOverloadDelayMs = (streak) => {
  const base = 1000;
  const step = 500;
  const max = 4000;
  const n = Number.isFinite(streak) && streak > 0 ? Math.floor(streak) : 0;
  return Math.min(max, base + step * n);
};
