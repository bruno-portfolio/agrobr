export function vPercentDose(layer, v2, prnt) {
  return Math.max(0, ((v2 - layer.v_pct) * layer.ctc_ph7) / prnt);
}
