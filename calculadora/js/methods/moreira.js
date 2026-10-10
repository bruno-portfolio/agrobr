import { CA_SAT_TARGET, CAO_KG_PER_CMOLC } from "../constants.js";

export function moreiraDose(layer, limestone) {
  const caTarget = CA_SAT_TARGET * layer.ctc_ph7;
  const dose = ((caTarget - layer.ca) * CAO_KG_PER_CMOLC * 10) / (limestone.cao_pct * limestone.prnt);
  return Math.max(0, dose);
}
