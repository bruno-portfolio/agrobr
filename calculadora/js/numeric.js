export function parseNum(value) {
  const normalized = String(value ?? "").trim().replace(",", ".");
  if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(normalized)) return NaN;
  const number = Number(normalized);
  return Number.isFinite(number) ? number : NaN;
}

export function stepNumber(current, increment, min, max) {
  const base = Number.isFinite(current) ? current : min;
  const next = Math.round((base + increment) * 1000) / 1000;
  return Math.min(max, Math.max(min, next));
}
