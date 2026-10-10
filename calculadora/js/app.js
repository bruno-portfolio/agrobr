import { CAO_MGO_LEGAL_MIN, CA_SAT_TARGET, K_MG_TO_CMOLC, MG_CRITICAL } from "./constants.js";
import { ValidationError, createLimestone, createLimingRequest, createSoilLayer } from "./models.js";
import { parseNum } from "./numeric.js";
import { recommend } from "./recommend.js";
import { SOURCES, sourceIndex } from "./sources.js";

const $ = id => document.getElementById(id);
const form = $("form");
const resultEl = $("result");

const SOIL_FIELDS = [
  { key: "ca", id: "ca", label: "Ca", unit: "base" },
  { key: "mg", id: "mg", label: "Mg", unit: "base" },
  { key: "k", id: "k", label: "K", unit: "k" },
  { key: "al", id: "al", label: "Al", unit: "base" },
  { key: "h_al", id: "h-al", label: "H+Al", unit: "base" },
];
const OPTIONAL_FIELDS = { clay_pct: "clay", ph_smp: "ph-smp" };
const LAYER_SUFFIX = { "0-20": "020", "20-40": "2040" };
const SMP_STATES = ["RS", "SC"];
const UNIT_LABEL = { cmolc: "cmolc/dm³", mmolc: "mmolc/dm³", mg: "mg/dm³" };

function fmt(value, digits = 1) {
  return value.toLocaleString("pt-BR", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function esc(text) {
  return String(text).replace(/[&<>"']/g, ch => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
}

function cites(ids = []) {
  return ids.map(id => `<a class="cite" href="#ref-${id}" title="${esc(SOURCES[id].short)}">[${sourceIndex(id)}]</a>`).join(" ");
}

function radio(name) {
  return form.querySelector(`input[name="${name}"]:checked`).value;
}

function toCmolc(value, unit) {
  if (unit === "mmolc") return value / 10;
  if (unit === "mg") return value / K_MG_TO_CMOLC;
  return value;
}

function setFieldError(id, message) {
  const input = $(id);
  const error = $(`${id}-error`);
  if (input) input.toggleAttribute("aria-invalid", Boolean(message));
  if (error) error.textContent = message ?? "";
}

function readLayer(depth) {
  const suffix = LAYER_SUFFIX[depth];
  const unit = radio("unit");
  const kunit = radio("kunit");
  const values = {};
  const missing = [];

  for (const field of SOIL_FIELDS) {
    const id = `${field.id}-${suffix}`;
    const raw = $(id).value;
    const number = parseNum(raw);
    setFieldError(id, raw.trim() !== "" && Number.isNaN(number) ? "Número inválido" : null);
    if (Number.isNaN(number)) missing.push(field.label);
    values[field.key] = toCmolc(number, field.unit === "k" ? kunit : unit);
  }
  for (const [key, prefix] of Object.entries(OPTIONAL_FIELDS)) {
    const input = $(`${prefix}-${suffix}`);
    if (!input || input.closest("[hidden]")) continue;
    const number = parseNum(input.value);
    setFieldError(input.id, input.value.trim() !== "" && Number.isNaN(number) ? "Número inválido" : null);
    if (!Number.isNaN(number)) values[key] = number;
  }
  if (missing.length) return { missing };

  try {
    return { layer: createSoilLayer({ depth, ...values }) };
  } catch (error) {
    if (!(error instanceof ValidationError)) throw error;
    const prefix = { h_al: "h-al", clay_pct: "clay", ph_smp: "ph-smp" }[error.field] ?? error.field;
    setFieldError(`${prefix}-${suffix}`, error.message);
    return { error: error.message };
  }
}

function readLimestone() {
  const values = { cao_pct: parseNum($("cao").value), mgo_pct: parseNum($("mgo").value), prnt: parseNum($("prnt").value) };
  ["cao", "mgo", "prnt"].forEach(id => setFieldError(id, null));
  try {
    return { limestone: createLimestone(values) };
  } catch (error) {
    if (!(error instanceof ValidationError)) throw error;
    setFieldError(error.field, error.message);
    return { error: error.message };
  }
}

function renderLive(depth, outcome) {
  const el = $(`live-${LAYER_SUFFIX[depth]}`);
  if (!outcome.layer) {
    el.innerHTML = outcome.error ? `<span class="bad">${esc(outcome.error)}</span>` : "CTC, V% e saturações aparecem aqui.";
    return;
  }
  const l = outcome.layer;
  const caClass = l.ca_sat_pct >= CA_SAT_TARGET * 100 ? "ok" : "bad";
  el.innerHTML = `<span>CTC pH 7 <b>${fmt(l.ctc_ph7, 2)}</b></span><span>V <b>${fmt(l.v_pct)}%</b></span><span>Ca na CTC <b class="${caClass}">${fmt(l.ca_sat_pct)}%</b></span><span>Sat. Al <b>${fmt(l.al_sat_pct, 0)}%</b></span>`;
}

function renderLimestoneLive() {
  const cao = parseNum($("cao").value);
  const mgo = parseNum($("mgo").value);
  const el = $("live-limestone");
  if (Number.isNaN(cao) || Number.isNaN(mgo)) {
    el.textContent = "Tipo e garantia mínima aparecem aqui.";
    return;
  }
  const tipo = mgo > 12 ? "dolomítico" : mgo >= 5 ? "magnesiano" : "calcítico";
  const legal = cao + mgo >= CAO_MGO_LEGAL_MIN;
  el.innerHTML = `<span>Tipo <b>${tipo}</b></span><span>CaO + MgO <b class="${legal ? "ok" : "bad"}">${fmt(cao + mgo)}%</b> ${legal ? "atende" : "abaixo de"} o mínimo legal de ${CAO_MGO_LEGAL_MIN}%</span>`;
}

function emptyState(message) {
  resultEl.innerHTML = `<div class="result-empty"><strong>Resultado</strong><span>${esc(message)}</span></div>`;
}

function meter(label, before, after, target) {
  const pct = v => Math.max(0, Math.min(100, v));
  return `<div class="meter">
    <div class="meter-head"><span>${label}</span><b>${fmt(before)}% → ${fmt(after)}%</b></div>
    <div class="meter-track">
      <div class="meter-fill after" style="width:${pct(after)}%"></div>
      <div class="meter-fill" style="width:${pct(before)}%"></div>
      <div class="meter-target" style="left:${pct(target)}%"></div>
    </div>
  </div>`;
}

function renderDose(request, research) {
  const layers = Object.entries(research.perLayer);
  const chips = layers.length > 1
    ? `<div class="dose-layers">${layers.map(([d, v]) => `<span class="chip">${d} cm <b>${fmt(v, 2)} t/ha</b></span>`).join("")}</div>`
    : "";
  const lime = request.limestone;
  return `<div class="dose">
    <span class="dose-label">Dose pela pesquisa · ${esc(research.name)} ${cites(research.sources)}</span>
    <div class="dose-value"><strong>${fmt(research.total, 1)}</strong><span>t/ha</span></div>
    <p class="dose-basis">${esc(research.basis)}. Calcário com ${fmt(lime.cao_pct, 0)}% de CaO, ${fmt(lime.mgo_pct, 0)}% de MgO e PRNT ${fmt(lime.prnt, 0)}%.</p>
    ${chips}
  </div>`;
}

function renderProjection(request, projection) {
  if (!projection) return "";
  const meters = request.layers.map(l => {
    const after = projection[l.depth];
    return meter(`Ca na CTC, ${l.depth} cm`, l.ca_sat_pct, after.caSat, CA_SAT_TARGET * 100);
  }).join("");
  const rows = request.layers.map(l => {
    const after = projection[l.depth];
    return `<tr><td>${l.depth} cm</td><td>${fmt(l.ca, 2)} → <span class="strong">${fmt(after.ca, 2)}</span></td><td>${fmt(l.mg, 2)} → <span class="strong">${fmt(after.mg, 2)}</span> <span class="ref">mín. ${fmt(MG_CRITICAL[l.depth])}</span></td><td>${fmt(l.v_pct, 0)} → <span class="strong">${fmt(after.v, 0)}%</span></td></tr>`;
  }).join("");
  return `<section class="result-section">
    <div class="section-label">Antes e depois ${cites(["moreira2026"])}</div>
    ${meters}
    <div class="meter-legend"><span><i style="background:var(--muted)"></i>hoje</span><span><i style="background:var(--gold)"></i>após a calagem</span><span><i style="background:var(--text)"></i>alvo de 60%</span></div>
    <table class="table"><thead><tr><th>Camada</th><th>Ca (cmolc)</th><th>Mg (cmolc)</th><th>V</th></tr></thead><tbody>${rows}</tbody></table>
  </section>`;
}

function renderComparison(request, result) {
  const { research, manual, projection } = result;
  const top = request.layers.find(l => l.depth === "0-20");
  const research020 = research.perLayer["0-20"];
  const researchRow = `<div class="compare-row primary"><span class="compare-name">Pela pesquisa, camada 0-20</span><span class="compare-value">${fmt(research020, 2)} t/ha</span><span class="compare-desc">${esc(research.name)} ${cites(research.sources)}</span></div>`;

  if (!manual.available || manual.dose === null) {
    return `<section class="result-section"><div class="section-label">Manual do estado</div>${researchRow}<p class="compare-explain">${esc(manual.reason)}</p></section>`;
  }

  const manualRow = `<div class="compare-row"><span class="compare-name">${esc(manual.name)}</span><span class="compare-value">${fmt(manual.dose, 2)} t/ha</span><span class="compare-desc">${esc(manual.target)}${manual.reason ? `. ${esc(manual.reason)}` : ""} ${cites(manual.sources)}</span></div>`;
  const diff = research020 - manual.dose;
  let explain;
  if (Math.abs(diff) < 0.3) {
    explain = "Na camada 0-20, a pesquisa e o manual chegam à mesma dose.";
  } else if (projection) {
    explain = `Na camada 0-20, a dose pela pesquisa é ${fmt(Math.abs(diff), 1)} t/ha ${diff > 0 ? "maior" : "menor"} que a do manual. A diferença vem do alvo: a pesquisa leva o Ca a 60% da CTC, o que deixa o solo com V de ${fmt(projection["0-20"].v, 0)}%. O alvo do manual é outro: ${esc(manual.target)}.`;
  } else {
    explain = `Na camada 0-20, a dose pela pesquisa é ${fmt(Math.abs(diff), 1)} t/ha ${diff > 0 ? "maior" : "menor"} que a do manual. Hoje o solo está com V de ${fmt(top.v_pct, 0)}%.`;
  }
  return `<section class="result-section"><div class="section-label">Comparação com o manual do estado</div><div class="compare">${researchRow}${manualRow}</div><p class="compare-explain">${explain}</p></section>`;
}

function renderNotes(notes) {
  if (!notes.length) return "";
  const items = notes.map(n => `<li class="note ${n.level === "warn" ? "warn" : n.level === "info" ? "info" : ""}"><span>${esc(n.text)} ${cites(n.sources)}</span></li>`).join("");
  return `<section class="result-section"><div class="section-label">Recomendações e avisos</div><ul class="notes">${items}</ul></section>`;
}

function render() {
  const smpVisible = SMP_STATES.includes($("state").value);
  $("ph-smp-field").hidden = !smpVisible;

  const include2040 = $("include-2040").checked;
  $("fields-2040").hidden = !include2040;
  $("empty-2040").hidden = include2040;
  $("live-2040").hidden = !include2040;

  document.querySelectorAll("[data-unit]").forEach(el => { el.textContent = UNIT_LABEL[radio("unit")]; });
  document.querySelectorAll("[data-kunit]").forEach(el => { el.textContent = UNIT_LABEL[radio("kunit")]; });

  const top = readLayer("0-20");
  renderLive("0-20", top);
  const sub = include2040 ? readLayer("20-40") : null;
  if (sub) renderLive("20-40", sub);
  renderLimestoneLive();
  const lime = readLimestone();

  if (top.missing) return emptyState(`Preencha ${top.missing.join(", ")} da camada 0-20 cm para ver a dose.`);
  if (top.error) return emptyState(top.error);
  if (sub?.missing) return emptyState(`Complete ${sub.missing.join(", ")} da camada 20-40 cm ou desligue essa camada.`);
  if (sub?.error) return emptyState(sub.error);
  if (lime.error) return emptyState(lime.error);

  const request = createLimingRequest({
    state: $("state").value,
    crop: radio("crop"),
    system: radio("system"),
    layers: sub ? [top.layer, sub.layer] : [top.layer],
    limestone: lime.limestone,
  });
  const result = recommend(request);

  resultEl.innerHTML = renderDose(request, result.research)
    + renderProjection(request, result.projection)
    + renderComparison(request, result)
    + renderNotes(result.notes)
    + `<div class="result-actions"><button type="button" class="button button-primary" id="print">Imprimir</button><a class="button button-secondary" href="#metodo">Como é calculado</a></div>`;
  $("print").addEventListener("click", () => window.print());
}

function renderReferences() {
  $("refs").innerHTML = Object.entries(SOURCES).map(([id, s]) =>
    `<li id="ref-${id}"><span>${esc(s.citation)} <a href="${esc(s.url)}" target="_blank" rel="noopener noreferrer">${esc(s.url.replace(/^https?:\/\//, ""))}</a><span class="kind">${esc(s.kind)}</span></span></li>`
  ).join("");
}

function renderMethod() {
  $("metodo-corpo").innerHTML = `
    <div>
      <h3>Abertura ou reforma de área</h3>
      <p>A dose leva o cálcio a 60% da CTC a pH 7, camada por camada, e soma as duas camadas quando há análise da 20-40 cm. É a equação de Moreira et al. (2026), calibrada em sete experimentos de quatro anos em Latossolos de Minas Gerais, com soja, feijão, milho, trigo e sorgo e calcário incorporado até 40 cm. ${cites(["moreira2026"])}</p>
    </div>
    <p class="formula">NC (t/ha) = (0,6 × CTC pH 7 − Ca) × 5600 ÷ (CaO% × PRNT%)</p>
    <p>Nas áreas do estudo, 95% da produtividade máxima veio com cerca de 60% de Ca e 29% de Mg na CTC da camada 0-20 cm, e 39% de Ca e 20% de Mg na 20-40 cm. Os níveis críticos foram 4,1 e 1,9 cmolc/dm³ de Ca e 2,0 e 1,0 cmolc/dm³ de Mg. ${cites(["moreira2026"])} A correção em profundidade é o que sustenta a resposta: com 15 t/ha incorporadas até 40 cm, o milho de segunda safra produziu 2.700 kg/ha a mais que sem calcário, em condição de calor e seca. ${cites(["moraes2023"])}</p>
    <div>
      <h3>Plantio direto consolidado</h3>
      <p>O estudo de Moreira et al. não avaliou calcário em superfície. No plantio direto, a calculadora segue a pesquisa de longo prazo da UNESP em Botucatu: reaplicar quando a saturação por bases da camada 0-20 cm cai abaixo de 50% e, nesse caso, aplicar em superfície a dose que a leva a 70%. A produtividade máxima de milho e aveia ficou muito próxima dessa dose, com o maior lucro em quatro safras. ${cites(["crusciol2016", "bossolani2022"])} Em 2016, no mesmo experimento, a dose passou a ser calculada para a camada 0-40 cm, porque o cálculo pela 0-20 subestimava a necessidade. Os micronutrientes foram repostos depois de cada calagem. ${cites(["bossolani2022"])}</p>
      <p>Quando a 20-40 cm tem saturação por Al acima de 20% ou Ca abaixo de 0,5 cmolc/dm³, a calculadora indica gesso: no Sul, pela fórmula que eleva o Ca a 60% da CTC efetiva; nas demais regiões, 50 kg por ponto de argila. ${cites(["embrapaSoja2020", "cairesGuimaraes2018"])} Em plantio direto no Paraná, incorporar o calcário reduziu a matéria orgânica da superfície, e o gesso com o calcário foi a alternativa indicada. ${cites(["besen2021"])}</p>
    </div>
    <div>
      <h3>O que a pesquisa recente mostra</h3>
      <p>Em 33 solos de Mato Grosso, chegar a uma saturação por bases de 60% a 80% exigiu até o dobro da dose do método de saturação por bases. Os autores consideram as metas de Ca+Mg de 2,0 cmolc/dm³ e de V de 50% do Cerrado ultrapassadas para a agricultura atual. ${cites(["lange2025"])}</p>
      <p>Em áreas novas de soja no Piauí, 10 t/ha de calcário renderam 18% e 12% a mais em duas safras. Acima de 10 t/ha, P, K e micronutrientes caíram no solo e nas folhas. Os autores recomendam não passar de 10 t/ha ou do dobro do que indicam os manuais. ${cites(["oliveira2024"])} Henrique Antunes de Souza, da Embrapa Meio-Norte e coautor do estudo, defende rever os documentos oficiais, apoiados em pesquisas dos anos 1980 e 1990. ${cites(["cultivar2024"])}</p>
    </div>
    <div>
      <h3>Limites</h3>
      <p>O artigo adota 60% de Ca nas duas camadas, embora seus dados indiquem 39% na 20-40 cm. O MgO do calcário não entra na equação; por isso a calculadora avisa quando o Mg projetado fica abaixo do nível crítico. Mesmo a dose do método ficou abaixo da dose de máxima produtividade medida em campo (em uma das áreas, 6 t/ha contra 11,2 t/ha). Fora de Latossolos, de grãos e da faixa de CTC de 3,1 a 9,5 cmolc/dm³, o resultado é indicativo. ${cites(["moreira2026"])}</p>
    </div>
    <div>
      <h3>Manual do estado</h3>
      <p>A comparação usa o critério oficial do seu estado quando o valor foi conferido na fonte. Quando não foi, a calculadora diz que não tem o parâmetro em vez de supor um valor.</p>
    </div>`;
}

function applyPreset(button) {
  $("cao").value = button.dataset.cao;
  $("mgo").value = button.dataset.mgo;
  $("prnt").value = button.dataset.prnt;
  render();
}

form.addEventListener("input", render);
form.addEventListener("change", render);
$("presets").addEventListener("click", event => {
  const button = event.target.closest(".preset");
  if (button) applyPreset(button);
});

renderMethod();
renderReferences();
render();
