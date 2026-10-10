import {
  AL_RANGE, CA_RANGE, CAO_RANGE, CLAY_RANGE, H_AL_RANGE, K_RANGE, LAYERS,
  MG_RANGE, MGO_RANGE, PH_SMP_RANGE, PRNT_RANGE,
} from "./constants.js";

export class ValidationError extends Error {
  constructor(field, message) {
    super(message);
    this.name = "ValidationError";
    this.field = field;
  }
}

function checkRange(field, label, value, [low, high]) {
  if (!Number.isFinite(value)) throw new ValidationError(field, `${label}: informe um número.`);
  if (value < low || value > high) {
    throw new ValidationError(field, `${label} fora da faixa aceita (${low} a ${high}).`);
  }
}

export function createSoilLayer({ depth, ca, mg, k, al, h_al, clay_pct = null, ph_smp = null }) {
  if (!LAYERS.includes(depth)) throw new ValidationError("depth", "Camada deve ser 0-20 ou 20-40.");

  checkRange("ca", "Ca", ca, CA_RANGE);
  checkRange("mg", "Mg", mg, MG_RANGE);
  checkRange("k", "K", k, K_RANGE);
  checkRange("al", "Al", al, AL_RANGE);
  checkRange("h_al", "H+Al", h_al, H_AL_RANGE);
  if (clay_pct !== null) checkRange("clay_pct", "Argila", clay_pct, CLAY_RANGE);
  if (ph_smp !== null) checkRange("ph_smp", "Índice SMP", ph_smp, PH_SMP_RANGE);
  if (al > h_al) throw new ValidationError("al", "Al não pode ser maior que H+Al.");

  const bases = ca + mg + k;
  const ctc_ph7 = bases + h_al;
  const t_efetiva = bases + al;

  return Object.freeze({
    depth, ca, mg, k, al, h_al, clay_pct, ph_smp,
    ctc_ph7,
    t_efetiva,
    v_pct: ctc_ph7 === 0 ? 0 : (bases / ctc_ph7) * 100,
    ca_sat_pct: ctc_ph7 === 0 ? 0 : (ca / ctc_ph7) * 100,
    al_sat_pct: t_efetiva === 0 ? 0 : (al / t_efetiva) * 100,
  });
}

export function createLimestone({ cao_pct, mgo_pct, prnt }) {
  checkRange("cao", "CaO", cao_pct, CAO_RANGE);
  checkRange("mgo", "MgO", mgo_pct, MGO_RANGE);
  checkRange("prnt", "PRNT", prnt, PRNT_RANGE);

  const tipo = mgo_pct > 12.0 ? "dolomítico" : mgo_pct >= 5.0 ? "magnesiano" : "calcítico";
  return Object.freeze({ cao_pct, mgo_pct, prnt, tipo });
}

export function createLimingRequest({ state, crop, system, layers, limestone }) {
  if (!["abertura", "spd"].includes(system)) throw new ValidationError("system", "Manejo deve ser abertura ou spd.");

  const depths = layers.map(l => l.depth);
  if (!depths.includes("0-20")) throw new ValidationError("layers", "A camada 0-20 é obrigatória.");
  if (new Set(depths).size !== depths.length) throw new ValidationError("layers", "Camada repetida.");

  return Object.freeze({ state, crop, system, layers, limestone });
}
