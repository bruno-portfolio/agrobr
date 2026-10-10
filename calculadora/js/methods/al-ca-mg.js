import { Y_CLAY_COEFFICIENTS } from "../constants.js";

export function yFactor(clayPct) {
  const [a, b, c] = Y_CLAY_COEFFICIENTS;
  return a + b * clayPct + c * clayPct ** 2;
}

export function alCaMgDose(layer, { mt, x }, prnt) {
  if (layer.clay_pct === null) return null;
  const alTerm = Math.max(0, yFactor(layer.clay_pct) * (layer.al - (mt * layer.t_efetiva) / 100));
  const caMgTerm = Math.max(0, x - (layer.ca + layer.mg));
  return ((alTerm + caMgTerm) * 100) / prnt;
}
