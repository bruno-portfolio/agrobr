import {
  SMP_SPD_FRACTION, SMP_SPD_MAX_THA, SMP_SPD_SKIP_AL_SAT_PCT, SMP_SPD_SKIP_V_PCT, SMP_TABLE,
} from "../constants.js";

const SMP_MIN = 4.4;
const SMP_MAX = 7.1;
const PH_TARGET_INDEX = { "5.5": 0, "6.0": 1, "6.5": 2 };

function lookupNc(phSmp, phTarget) {
  const idx = PH_TARGET_INDEX[phTarget.toFixed(1)];
  if (phSmp <= SMP_MIN) return SMP_TABLE[SMP_MIN.toFixed(1)][idx];

  const lowerKey = (Math.floor(phSmp * 10) / 10).toFixed(1);
  const upperKey = (Math.ceil(phSmp * 10) / 10).toFixed(1);
  if (lowerKey === upperKey) return SMP_TABLE[lowerKey][idx];

  const fraction = (phSmp - parseFloat(lowerKey)) / (parseFloat(upperKey) - parseFloat(lowerKey));
  return SMP_TABLE[lowerKey][idx] + fraction * (SMP_TABLE[upperKey][idx] - SMP_TABLE[lowerKey][idx]);
}

export function smpDose(layer, phTarget, system, prnt) {
  if (layer.ph_smp === null) return { dose: null, reason: "Informe o índice SMP do laudo." };
  if (layer.ph_smp > SMP_MAX) return { dose: 0, reason: "Índice SMP acima de 7,1: a tabela não indica calcário." };

  if (system !== "spd") return { dose: lookupNc(layer.ph_smp, phTarget) * (100 / prnt), reason: null };

  if (layer.v_pct >= SMP_SPD_SKIP_V_PCT && layer.al_sat_pct < SMP_SPD_SKIP_AL_SAT_PCT) {
    return { dose: 0, reason: "Plantio direto com V ≥ 65% e saturação por Al < 10%: o manual indica não aplicar." };
  }
  const prnt100 = Math.min(lookupNc(layer.ph_smp, phTarget) * SMP_SPD_FRACTION, SMP_SPD_MAX_THA);
  return { dose: prnt100 * (100 / prnt), reason: "Plantio direto: ¼ da dose SMP em superfície, até 5 t/ha com PRNT 100%." };
}
