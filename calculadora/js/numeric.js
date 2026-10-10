export function parseNum(value) {
  const normalized = String(value ?? "").trim().replace(",", ".");
  if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(normalized)) return NaN;
  const number = Number(normalized);
  return Number.isFinite(number) ? number : NaN;
}
