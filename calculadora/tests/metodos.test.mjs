import assert from "node:assert/strict";
import { test } from "node:test";
import { alCaMgDose } from "../js/methods/al-ca-mg.js";
import { moreiraDose } from "../js/methods/moreira.js";
import { vPercentDose } from "../js/methods/v-percent.js";
import { createLimestone, createLimingRequest, createSoilLayer } from "../js/models.js";
import { recommend } from "../js/recommend.js";
import { SOURCES } from "../js/sources.js";

const PAPER_SITES = [
  { layers: [[0.3, 3.7, 0.9, 0.0, 4.2], [0.2, 3.4, 0.8, 0.0, 3.2]], limestone: [44, 14, 83], bs70: 2.85, moreira: 4.5 },
  { layers: [[0.1, 1.4, 0.5, 0.0, 2.7], [0.1, 1.1, 0.7, 0.0, 2.4]], limestone: [35, 20, 83], bs70: 2.93, moreira: 5.6 },
  { layers: [[0.1, 1.4, 0.8, 0.0, 7.2], [0.1, 0.9, 0.4, 0.0, 4.0]], limestone: [47, 14, 77], bs70: 8.83, moreira: 10.3 },
  { layers: [[0.1, 1.2, 0.5, 0.2, 1.8], [0.0, 1.2, 0.3, 0.2, 1.6]], limestone: [33, 18, 100], bs70: 1.37, moreira: 2.8 },
  { layers: [[0.3, 1.2, 1.1, 0.6, 5.3], [0.1, 0.8, 0.6, 0.8, 4.5]], limestone: [33, 18, 100], bs70: 5.56, moreira: 10.8 },
  { layers: [[0.1, 1.1, 0.4, 0.2, 3.7], [0.1, 0.9, 0.2, 0.2, 3.2]], limestone: [32, 10, 92], bs70: 4.36, moreira: 7.3 },
  { layers: [[0.0, 0.9, 0.1, 1.4, 7.0], [0.0, 0.8, 0.1, 1.4, 5.8]], limestone: [43, 10, 86], bs70: 9.80, moreira: 11.0 },
];

const DEPTHS = ["0-20", "20-40"];

function layer(depth, [k, ca, mg, al, h_al], extra = {}) {
  return createSoilLayer({ depth, k, ca, mg, al, h_al, ...extra });
}

function limestone([cao_pct, mgo_pct, prnt]) {
  return createLimestone({ cao_pct, mgo_pct, prnt });
}

function request({ state = "MG", crop = "soja", system = "abertura", layers, lime = [36, 12, 85] }) {
  return createLimingRequest({ state, crop, system, layers, limestone: limestone(lime) });
}

function siteDose(site) {
  return site.layers.map((values, i) => moreiraDose(layer(DEPTHS[i], values), limestone(site.limestone))).reduce((a, b) => a + b);
}

test("moreira reproduces the 0-40 cm rates published for sites 1 to 6", () => {
  for (const [i, site] of PAPER_SITES.slice(0, 6).entries()) {
    assert.ok(Math.abs(siteDose(site) - site.moreira) <= 0.06, `site ${i + 1}: ${siteDose(site)}`);
  }
});

test("moreira stays within 0.25 t/ha at site 7, whose Table 2 rounds K to 0.0", () => {
  const site = PAPER_SITES[6];
  assert.ok(Math.abs(siteDose(site) - site.moreira) <= 0.25, String(siteDose(site)));
});

test("base saturation at 70% summed over both layers reproduces the published rates", () => {
  for (const [i, site] of PAPER_SITES.entries()) {
    const dose = site.layers.map((values, j) => vPercentDose(layer(DEPTHS[j], values), 70, site.limestone[2])).reduce((a, b) => a + b);
    assert.ok(Math.abs(dose - site.bs70) <= 0.1, `site ${i + 1}: ${dose}`);
  }
});

test("opening recommendation sums both layers and projects Ca to 60% of CEC", () => {
  const site = PAPER_SITES[1];
  const req = request({ layers: site.layers.map((v, i) => layer(DEPTHS[i], v)), lime: site.limestone });
  const result = recommend(req);
  assert.ok(Math.abs(result.research.total - site.moreira) <= 0.06);
  for (const soil of req.layers) {
    assert.ok(Math.abs(result.projection[soil.depth].ca - 0.6 * soil.ctc_ph7) < 1e-9, soil.depth);
  }
});

test("calcitic limestone that leaves Mg below the critical level gets the MgO needed", () => {
  const result = recommend(request({ layers: [layer("0-20", PAPER_SITES[1].layers[0])], lime: [48, 3, 85] }));
  const mgNote = result.notes.find(n => n.text.includes("abaixo do nível crítico de 2,0"));
  assert.ok(mgNote);
  assert.match(mgNote.text, /cerca de \d+% de MgO|Nenhum calcário/);
  assert.ok(!result.notes.some(n => n.text.includes("Ca/Mg") || n.text.includes("antagonismo")));
});

test("no-till research dose raises base saturation to 70% in 0-20 only when it fell below 50%", () => {
  const acid = layer("0-20", [0.2, 2.0, 0.8, 0.5, 5.0]);
  const result = recommend(request({ system: "spd", layers: [acid] }));
  assert.ok(acid.v_pct < 50);
  assert.ok(Math.abs(result.research.total - vPercentDose(acid, 70, 85)) < 1e-9);
  assert.equal(result.projection, null);

  const corrected = layer("0-20", [0.3, 3.0, 1.0, 0.0, 4.0]);
  assert.ok(corrected.v_pct >= 50);
  const skipped = recommend(request({ system: "spd", layers: [corrected] })).research;
  assert.equal(skipped.total, 0);
  assert.match(skipped.basis, /só reaplica/);
});

test("opening rates above 10 t/ha warn about P, K and micronutrients", () => {
  const site = PAPER_SITES[6];
  const notes = recommend(request({ layers: site.layers.map((v, i) => layer(DEPTHS[i], v)), lime: site.limestone })).notes;
  assert.ok(notes.some(n => n.sources.includes("oliveira2024")));
  assert.ok(notes.some(n => n.text.includes("incorporar o calcário até 40 cm")));
});

test("manual comparison uses only verified parameters and says when there is none", () => {
  const top = layer("0-20", [0.2, 2.0, 0.8, 0.5, 5.0], { clay_pct: 50 });
  const sp = recommend(request({ state: "SP", layers: [top] })).manual;
  assert.equal(sp.v2, 70);
  assert.ok(Math.abs(sp.dose - vPercentDose(top, 70, 85)) < 1e-9);
  assert.equal(recommend(request({ state: "MT", crop: "milho", layers: [top] })).manual.v2, 50);
  assert.equal(recommend(request({ state: "SP", crop: "milho", layers: [top] })).manual.available, false);
  assert.equal(recommend(request({ state: "PA", layers: [top] })).manual.available, false);
  const coffee = recommend(request({ state: "MG", crop: "café", layers: [top] })).manual;
  assert.ok(Math.abs(coffee.dose - alCaMgDose(top, { mt: 25, x: 3.5 }, 85)) < 1e-9);
  assert.equal(recommend(request({ state: "ES", crop: "café", layers: [top] })).manual.v2, 70);
});

test("Al+Ca+Mg uses the continuous Y buffer curve published for clay content", () => {
  const publishedY = { 10: 0.66, 20: 1.23, 35: 2.0, 50: 2.65, 80: 3.61 };
  for (const [clay, y] of Object.entries(publishedY)) {
    const soil = layer("0-20", [0.1, 0.5, 0.2, 1.0, 5.0], { clay_pct: Number(clay) });
    const expected = y * (1.0 - 0.2 * 1.8) + (2.0 - 0.7);
    assert.ok(Math.abs(alCaMgDose(soil, { mt: 20, x: 2.0 }, 100) - expected) < 0.01, `clay ${clay}`);
  }
});

test("SMP under consolidated no-till applies a quarter of the pH 6.0 dose, capped at 5 t/ha", () => {
  const smp = (ph_smp, prnt) => recommend(request({
    state: "RS", system: "spd", layers: [layer("0-20", [0.2, 2.0, 0.8, 1.0, 6.0], { ph_smp })], lime: [36, 12, prnt],
  })).manual.dose;
  assert.ok(Math.abs(smp(5.5, 100) - 6.1 / 4) < 1e-9);
  assert.equal(smp(4.4, 100), 5.0);
  assert.ok(Math.abs(smp(4.4, 80) - 6.25) < 1e-9);
});

test("SMP under consolidated no-till skips liming when V ≥ 65% and Al saturation < 10%", () => {
  const manual = recommend(request({
    state: "RS", system: "spd", layers: [layer("0-20", [0.3, 5.0, 1.5, 0.0, 3.0], { ph_smp: 6.0 })],
  })).manual;
  assert.equal(manual.dose, 0);
  assert.match(manual.reason, /não aplicar/);
});

test("SMP outside no-till keeps the full table dose and pastures target pH 6.0", () => {
  const smp = crop => recommend(request({
    state: "RS", crop, layers: [layer("0-20", [0.2, 2.0, 0.8, 1.0, 6.0], { ph_smp: 5.5 })], lime: [36, 12, 100],
  })).manual.dose;
  assert.equal(smp("soja"), 6.1);
  assert.equal(smp("pastagem"), 6.1);
});

test("gypsum under no-till follows Caires & Guimarães in the South and 50 kg per clay point elsewhere", () => {
  const top = layer("0-20", [0.2, 3.0, 1.0, 0.0, 4.0]);
  const sub = layer("20-40", [0.1, 0.4, 0.2, 1.0, 5.0], { clay_pct: 40 });
  const south = recommend(request({ state: "PR", system: "spd", layers: [top, sub] })).notes;
  const caires = (0.6 * sub.t_efetiva - sub.ca) * 6.4;
  assert.ok(south.some(n => n.sources.includes("cairesGuimaraes2018") && n.text.includes(caires.toLocaleString("pt-BR", { minimumFractionDigits: 1, maximumFractionDigits: 1 }))));
  const cerrado = recommend(request({ state: "GO", system: "spd", layers: [top, sub] })).notes;
  assert.ok(cerrado.some(n => n.text.includes("Gesso: 2.000 kg/ha")));
});

test("limestone below the legal CaO + MgO minimum is flagged", () => {
  const result = recommend(request({ layers: [layer("0-20", [0.2, 2.0, 0.8, 0.5, 5.0])], lime: [30, 6, 85] }));
  assert.ok(result.notes.some(n => n.sources.includes("in35")));
});

test("every source cited by a note exists in the references", () => {
  const top = layer("0-20", [0.2, 1.0, 0.2, 1.0, 7.0]);
  const sub = layer("20-40", [0.1, 0.3, 0.1, 1.2, 6.0], { clay_pct: 50 });
  for (const system of ["abertura", "spd"]) {
    for (const state of ["PR", "GO", "SP", "RS"]) {
      const result = recommend(request({ state, crop: "café", system, layers: [top, sub], lime: [30, 6, 120] }));
      const cited = [...result.research.sources, ...result.notes.flatMap(n => n.sources), ...(result.manual.sources ?? [])];
      for (const id of cited) assert.ok(SOURCES[id], id);
    }
  }
});
