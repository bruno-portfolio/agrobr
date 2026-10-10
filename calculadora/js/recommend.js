import {
  CAO_KG_PER_CMOLC, CAO_MGO_LEGAL_MIN, CTC_HIGH_THRESHOLD, GRAIN_CROPS, GYPSUM_AL_SAT_TRIGGER,
  GYPSUM_CA_ECEC_TARGET, GYPSUM_CA_ECEC_TRIGGER, GYPSUM_CA_TRIGGER, GYPSUM_CAIRES_FACTOR,
  GYPSUM_KG_PER_CLAY_PCT, MG_CRITICAL, MGO_KG_PER_CMOLC, MGO_RANGE, PAPER_CTC_RANGE,
  HIGH_RATE_THA, PAPER_PRNT_RANGE, PAPER_RATE_RANGE, PARCEL_THRESHOLD_THA, SOUTH_STATES,
  SPD_REAPPLY_V, SPD_V_TARGET,
} from "./constants.js";
import { MANUALS, coffeeVeByCtc } from "./manuals.js";
import { alCaMgDose } from "./methods/al-ca-mg.js";
import { moreiraDose } from "./methods/moreira.js";
import { smpDose } from "./methods/smp.js";
import { vPercentDose } from "./methods/v-percent.js";

export function recommend(request) {
  const top = layerAt(request, "0-20");
  const sub = layerAt(request, "20-40");
  const research = request.system === "abertura" ? openingDose(request) : noTillDose(request, top);
  const manual = manualDose(request, top);
  const projection = request.system === "abertura" ? project(request, research.perLayer) : null;
  const notes = [
    ...domainNotes(request, research, sub),
    ...magnesiumNotes(request, research, projection),
    ...applicationNotes(request, research),
    ...gypsumNotes(request, sub),
    ...dataNotes(request),
  ];
  return { research, manual, projection, notes };
}

function layerAt(request, depth) {
  return request.layers.find(l => l.depth === depth) ?? null;
}

function sum(values) {
  return values.reduce((a, b) => a + b, 0);
}

function openingDose({ layers, limestone }) {
  const perLayer = Object.fromEntries(layers.map(l => [l.depth, moreiraDose(l, limestone)]));
  return {
    method: "moreira",
    total: sum(Object.values(perLayer)),
    perLayer,
    name: "Saturação por cálcio de 60% da CTC",
    basis: layers.length === 2 ? "Perfil 0-40 cm, calcário incorporado" : "Camada 0-20 cm, calcário incorporado",
    sources: ["moreira2026"],
  };
}

function noTillDose({ limestone }, top) {
  const due = top.v_pct < SPD_REAPPLY_V;
  const dose = due ? vPercentDose(top, SPD_V_TARGET, limestone.prnt) : 0;
  return {
    method: "spd",
    total: dose,
    perLayer: { "0-20": dose },
    name: `Saturação por bases de ${SPD_V_TARGET}% em superfície`,
    basis: due
      ? `V atual de ${fmt(top.v_pct, 0)}% está abaixo de ${SPD_REAPPLY_V}%: aplicar em superfície, sem incorporar, para levar a camada 0-20 cm a V de ${SPD_V_TARGET}%`
      : `V atual de ${fmt(top.v_pct, 0)}% não está abaixo de ${SPD_REAPPLY_V}%: a pesquisa em plantio direto só reaplica quando V cai abaixo disso`,
    sources: ["crusciol2016", "bossolani2022"],
  };
}

function manualDose({ state, crop, system, limestone }, top) {
  if (!state) return { available: false, reason: "Selecione o estado para comparar com o manual." };
  const config = MANUALS[state]?.[crop];
  if (!config) {
    return { available: false, reason: `Ainda sem parâmetro de manual conferido para ${crop} em ${state}.` };
  }

  const prnt = limestone.prnt;
  if (config.method === "smp") {
    const { dose, reason } = smpDose(top, config.phTarget, system, prnt);
    return { ...config, available: dose !== null, dose, reason };
  }
  if (config.method === "alcamg") {
    const dose = alCaMgDose(top, config, prnt);
    return { ...config, available: dose !== null, dose, reason: dose === null ? "Informe a argila da camada 0-20." : null };
  }
  const v2 = config.method === "vctc" ? coffeeVeByCtc(top.ctc_ph7) : config.v2;
  return { ...config, v2, available: true, dose: vPercentDose(top, v2, prnt), reason: null };
}

function project({ layers, limestone }, perLayer) {
  return Object.fromEntries(layers.map(layer => {
    const dose = perLayer[layer.depth] ?? 0;
    const ca = layer.ca + (dose * limestone.cao_pct * limestone.prnt) / (CAO_KG_PER_CMOLC * 10);
    const mg = layer.mg + (dose * limestone.mgo_pct * limestone.prnt) / (MGO_KG_PER_CMOLC * 10);
    const ctc = layer.ctc_ph7;
    return [layer.depth, {
      ca, mg,
      caSat: (ca / ctc) * 100,
      mgSat: (mg / ctc) * 100,
      v: ((ca + mg + layer.k) / ctc) * 100,
    }];
  }));
}

function note(level, text, sources = []) {
  return { level, text, sources };
}

function fmt(value, digits = 1) {
  return value.toLocaleString("pt-BR", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function domainNotes({ crop, system, layers, limestone }, research, sub) {
  if (system !== "abertura") {
    return [
      note("info", "Moreira et al. (2026) não testou plantio direto. A dose em superfície segue a pesquisa de calagem sem incorporação.", ["moreira2026", ...research.sources]),
      note("info", "No experimento de plantio direto da UNESP em Botucatu, iniciado em 2002, a dose passou a ser calculada para a camada 0-40 cm em 2016. Os micronutrientes foram repostos depois de cada calagem.", ["bossolani2022"]),
    ];
  }
  const notes = [];
  if (!GRAIN_CROPS.includes(crop)) {
    notes.push(note("warn", `O método foi calibrado com soja, feijão, milho, trigo e sorgo. Para ${crop}, use a dose como referência e confira com o manual.`, ["moreira2026"]));
  }
  if (!sub) {
    notes.push(note("info", "Sem a camada 20-40, a dose cobre só a 0-20. O método foi calibrado com correção até 40 cm.", ["moreira2026"]));
  }
  const [ctcMin, ctcMax] = PAPER_CTC_RANGE;
  for (const layer of layers) {
    if (layer.ctc_ph7 < ctcMin || layer.ctc_ph7 > ctcMax) {
      notes.push(note("info", `CTC da camada ${layer.depth} (${fmt(layer.ctc_ph7)} cmolc/dm³) fora da faixa das áreas do estudo (${fmt(ctcMin)} a ${fmt(ctcMax)}).`, ["moreira2026"]));
    }
  }
  const [prntMin, prntMax] = PAPER_PRNT_RANGE;
  if (limestone.prnt < prntMin || limestone.prnt > prntMax) {
    notes.push(note("info", `PRNT fora da faixa dos calcários do estudo (${prntMin}% a ${prntMax}%).`, ["moreira2026"]));
  }
  if (research.total > PAPER_RATE_RANGE[1]) {
    notes.push(note("warn", `Dose acima da maior testada no estudo (${PAPER_RATE_RANGE[1]} t/ha). Confira os dados do laudo.`, ["moreira2026"]));
  }
  return notes;
}

function magnesiumNotes({ layers, limestone }, research, projection) {
  if (!projection) return [];
  const notes = [];
  for (const layer of layers) {
    const dose = research.perLayer[layer.depth] ?? 0;
    const critical = MG_CRITICAL[layer.depth];
    if (dose === 0 || projection[layer.depth].mg >= critical) continue;
    const mgoNeeded = ((critical - layer.mg) * MGO_KG_PER_CMOLC * 10) / (dose * limestone.prnt);
    const how = mgoNeeded <= MGO_RANGE[1]
      ? `Com esta dose, um calcário com cerca de ${fmt(mgoNeeded, 0)}% de MgO chega lá.`
      : "Nenhum calcário chega lá só com esta dose; complemente com outra fonte de Mg.";
    notes.push(note("action", `Mg na camada ${layer.depth} ficaria em ${fmt(projection[layer.depth].mg, 2)} cmolc/dm³, abaixo do nível crítico de ${fmt(critical)}. ${how}`, ["moreira2026"]));
  }
  return notes;
}

function applicationNotes({ system, layers }, research) {
  if (system !== "abertura" || research.total === 0) return [];
  const notes = [];
  if (layers.length === 2) {
    notes.push(note("action", "A dose das duas camadas pressupõe incorporar o calcário até 40 cm, como no estudo: grade pesada e subsolador, ou arado de discos.", ["moreira2026"]));
  }
  if (research.total > PARCEL_THRESHOLD_THA) {
    notes.push(note("action", `Acima de ${fmt(PARCEL_THRESHOLD_THA, 0)} t/ha, divida a aplicação: metade da dose incorporada em cada operação, com o solo úmido.`, ["embrapaTrigo2026"]));
  }
  if (research.total > HIGH_RATE_THA) {
    notes.push(note("warn", `Acima de ${fmt(HIGH_RATE_THA, 0)} t/ha, P, K e micronutrientes caíram no solo e nas folhas da soja em áreas novas do Matopiba. Reponha esses nutrientes na adubação.`, ["oliveira2024", "bossolani2022"]));
  }
  return notes;
}

function gypsumNotes({ state, system }, sub) {
  if (system !== "spd" || !sub) return [];
  if (sub.al_sat_pct <= GYPSUM_AL_SAT_TRIGGER && sub.ca >= GYPSUM_CA_TRIGGER) return [];

  const why = `A camada 20-40 tem saturação por Al de ${fmt(sub.al_sat_pct, 0)}% e Ca de ${fmt(sub.ca, 2)} cmolc/dm³.`;
  const magnesium = note("info", "O gesso reduziu o Mg da camada superficial em plantio direto; acompanhe o Mg na próxima análise.", ["besen2021"]);
  if (SOUTH_STATES.includes(state)) {
    const caShare = sub.t_efetiva === 0 ? 1 : sub.ca / sub.t_efetiva;
    if (caShare >= GYPSUM_CA_ECEC_TRIGGER) return [];
    const tha = (GYPSUM_CA_ECEC_TARGET * sub.t_efetiva - sub.ca) * GYPSUM_CAIRES_FACTOR;
    return [note("action", `${why} Gesso: ${fmt(tha)} t/ha para elevar o Ca a 60% da CTC efetiva da 20-40.`, ["cairesGuimaraes2018", "embrapaSoja2020"]), magnesium];
  }
  if (sub.clay_pct === null) {
    return [note("action", `${why} O gesso é indicado; informe a argila da 20-40 para calcular a dose.`, ["embrapaSoja2020"])];
  }
  const kg = GYPSUM_KG_PER_CLAY_PCT * sub.clay_pct;
  return [note("action", `${why} Gesso: ${fmt(kg, 0)} kg/ha (50 kg por ponto de argila).`, ["embrapaSoja2020"]), magnesium];
}

function dataNotes({ layers, limestone }) {
  const notes = [];
  if (limestone.cao_pct + limestone.mgo_pct < CAO_MGO_LEGAL_MIN) {
    notes.push(note("warn", `CaO + MgO abaixo de ${CAO_MGO_LEGAL_MIN}%: o produto não atende ao mínimo legal de calcário agrícola.`, ["in35"]));
  }
  for (const layer of layers) {
    if (layer.ctc_ph7 > CTC_HIGH_THRESHOLD) {
      notes.push(note("warn", `CTC da camada ${layer.depth} acima de ${CTC_HIGH_THRESHOLD} cmolc/dm³. Confira se o laudo está em mmolc/dm³.`));
    }
  }
  return notes;
}
