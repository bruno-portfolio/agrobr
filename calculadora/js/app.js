import { STATE_PRIMARY_METHOD } from "./constants.js";
import { createSoilLayer, createLimestone, createLimingRequest, ValidationError } from "./models.js";
import { runDiagnostic } from "./diagnostic.js";
import { renderChart, destroyChart } from "./charts.js";
import { parseNum, stepNumber } from "./numeric.js";

// ─── STATE ───
const state = {
  currentStep: 1,
  state: "",
  crop: "",
  system: "abertura",
  include2040: false,
};

const METHOD_DISPLAY = {
  moreira_2025: { name: "Moreira et al. (2025)", desc: "Elevação da %Ca na CTC, multicamada, calibrado em Latossolos de MG" },
  v_percent: { name: "Saturação por Bases (V%)", desc: "Raij (1979), calibração estadual" },
  al_ca_mg: { name: "Al+Ca+Mg (5ª Aprox.)", desc: "Alvarez V. & Ribeiro (1999), MG" },
  smp: { name: "Tabela SMP", desc: "CQFS (2016), RS/SC" },
};

const STATE_METHOD_NAMES = {
  v_percent: "V% (Saturação por Bases)",
  al_ca_mg: "Al+Ca+Mg (5ª Aproximação)",
  smp: "SMP (Tabela RS/SC)",
};

// ─── DOM REFS ───
const $ = id => document.getElementById(id);
const panels = [null, $("step-1"), $("step-2"), $("step-3"), $("step-4")];
const stepperItems = document.querySelectorAll(".stepper-item");

// ─── STEPPER +/- BUTTONS ───
function initSteppers() {
  document.querySelectorAll("input[data-step]").forEach(input => {
    const step = parseFloat(input.dataset.step);
    const min = parseFloat(input.dataset.min);
    const max = parseFloat(input.dataset.max);
    const decimals = step < 1 ? (step < 0.1 ? 2 : 1) : 0;

    const wrapper = document.createElement("div");
    wrapper.className = "stepper-input";

    const btnDec = document.createElement("button");
    btnDec.type = "button";
    btnDec.className = "stepper-btn stepper-dec";
    btnDec.textContent = "−";
    btnDec.setAttribute("aria-label", `Diminuir ${input.id}`);
    btnDec.tabIndex = -1;

    const btnInc = document.createElement("button");
    btnInc.type = "button";
    btnInc.className = "stepper-btn stepper-inc";
    btnInc.textContent = "+";
    btnInc.setAttribute("aria-label", `Aumentar ${input.id}`);
    btnInc.tabIndex = -1;

    input.parentNode.insertBefore(wrapper, input);
    wrapper.appendChild(btnDec);
    wrapper.appendChild(input);
    wrapper.appendChild(btnInc);

    function stepValue(direction) {
      const next = stepNumber(parseNum(input.value), step * direction, min, max);
      input.value = next.toFixed(decimals);
      input.dispatchEvent(new Event("input", { bubbles: true }));
    }

    btnDec.addEventListener("click", () => stepValue(-1));
    btnInc.addEventListener("click", () => stepValue(1));
  });
}

initSteppers();

// ─── NAVIGATION ───
function goToStep(n) {
  panels[state.currentStep].classList.remove("active");
  panels[n].classList.add("active");

  stepperItems.forEach((item, i) => {
    item.classList.remove("active", "completed");
    const stepNum = i + 1;
    if (stepNum < n) item.classList.add("completed");
    if (stepNum === n) item.classList.add("active");
    item.setAttribute("aria-selected", stepNum === n ? "true" : "false");
  });

  state.currentStep = n;
  window.scrollTo({ top: 0, behavior: "smooth" });

  if (n === 4) runAndRender();
}

// ─── STEP 1 LOGIC ───
$("state").addEventListener("change", e => {
  state.state = e.target.value;
  clearError("state");
  const hint = $("state-method-hint");
  const method = STATE_PRIMARY_METHOD[state.state];
  if (method) {
    hint.textContent = `Moreira 2025 ✓ + comparação com ${STATE_METHOD_NAMES[method]}`;
    hint.classList.add("visible");
  } else if (state.state) {
    hint.textContent = "Moreira 2025 ✓ (sem método estadual para comparação)";
    hint.classList.add("visible");
  } else {
    hint.classList.remove("visible");
  }
  updateContextualFields();
});

document.querySelectorAll(".crop-card").forEach(card => {
  card.addEventListener("click", () => selectCrop(card.dataset.crop));
  card.addEventListener("keydown", e => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); selectCrop(card.dataset.crop); }
  });
});

function selectCrop(crop) {
  state.crop = crop;
  clearError("crop");
  document.querySelectorAll(".crop-card").forEach(c => {
    const selected = c.dataset.crop === crop;
    c.classList.toggle("selected", selected);
    c.setAttribute("aria-checked", selected ? "true" : "false");
  });
}

document.querySelectorAll(".toggle-option").forEach(opt => {
  opt.addEventListener("click", () => selectSystem(opt.dataset.system));
  opt.addEventListener("keydown", e => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); selectSystem(opt.dataset.system); }
  });
});

function selectSystem(system) {
  state.system = system;
  document.querySelectorAll(".toggle-option").forEach(o => {
    const selected = o.dataset.system === system;
    o.classList.toggle("selected", selected);
    o.setAttribute("aria-checked", selected ? "true" : "false");
  });
}

$("btn-next-1").addEventListener("click", () => {
  let valid = true;
  if (!state.state) { showError("state", "Selecione um estado."); valid = false; }
  if (!state.crop) { showError("crop", "Selecione uma cultura."); valid = false; }
  if (valid) goToStep(2);
});

// ─── STEP 2 LOGIC ───
$("include-2040").addEventListener("change", e => {
  state.include2040 = e.target.checked;
  const col = $("soil-2040");
  const columns = $("soil-columns");
  if (state.include2040) {
    col.classList.remove("hidden");
    columns.classList.add("two-columns");
  } else {
    col.classList.add("hidden");
    columns.classList.remove("two-columns");
  }
});

function updateContextualFields() {
  const clayGroup = $("clay-group-020");
  const smpGroup = $("ph-smp-group-020");
  clayGroup.classList.remove("field-emphasized");
  smpGroup.classList.remove("field-emphasized");

  if (state.state === "MG") clayGroup.classList.add("field-emphasized");
  if (state.state === "RS" || state.state === "SC") smpGroup.classList.add("field-emphasized");
}

const soilFields020 = ["ca-020", "mg-020", "k-020", "al-020", "h-al-020"];
soilFields020.forEach(id => {
  $(id).addEventListener("input", updateLivePanel);
  $(id).addEventListener("blur", () => validateSoilField(id));
});

function updateLivePanel() {
  const ca = parseNum($("ca-020").value);
  const mg = parseNum($("mg-020").value);
  let k = parseNum($("k-020").value);
  const al = parseNum($("al-020").value);
  const hAl = parseNum($("h-al-020").value);

  if ([ca, mg, k, hAl].some(v => isNaN(v))) {
    $("live-ctc").textContent = "—";
    $("live-vpct").textContent = "—";
    $("live-t").textContent = "—";
    return;
  }

  if (k > 5) k = k / 391;

  const ctc = ca + mg + k + hAl;
  const vpct = ctc === 0 ? 0 : ((ca + mg + k) / ctc) * 100;
  const t = ca + mg + k + (isNaN(al) ? 0 : al);

  $("live-ctc").textContent = ctc.toFixed(2);
  $("live-vpct").textContent = vpct.toFixed(1) + "%";
  $("live-t").textContent = t.toFixed(2);
}

function validateSoilField(id) {
  const val = parseNum($(id).value);
  if ($(id).value.trim() !== "" && isNaN(val)) {
    showFieldError(id, "Valor numérico inválido");
    return false;
  }
  clearFieldError(id);
  return true;
}

$("btn-prev-2").addEventListener("click", () => goToStep(1));
$("btn-next-2").addEventListener("click", () => {
  if (validateStep2()) goToStep(3);
});

function validateStep2() {
  let valid = true;
  const required020 = ["ca-020", "mg-020", "k-020", "al-020", "h-al-020"];
  for (const id of required020) {
    const val = parseNum($(id).value);
    if ($(id).value.trim() === "" || isNaN(val)) {
      showFieldError(id, "Campo obrigatório"); valid = false;
    } else if (val < 0) {
      showFieldError(id, "Valor não pode ser negativo"); valid = false;
    } else {
      clearFieldError(id);
    }
  }

  if (valid) {
    const al020 = parseNum($("al-020").value);
    const hAl020 = parseNum($("h-al-020").value);
    if (!isNaN(al020) && !isNaN(hAl020) && al020 > hAl020) {
      showFieldError("al-020", "Al não pode ser maior que H+Al");
      valid = false;
    }
  }

  if (state.include2040) {
    const required2040 = ["ca-2040", "mg-2040", "k-2040", "al-2040", "h-al-2040"];
    for (const id of required2040) {
      const val = parseNum($(id).value);
      if ($(id).value.trim() === "" || isNaN(val)) {
        showFieldError(id, "Campo obrigatório"); valid = false;
      } else if (val < 0) {
        showFieldError(id, "Valor não pode ser negativo"); valid = false;
      } else {
        clearFieldError(id);
      }
    }

    if (valid) {
      const al2040 = parseNum($("al-2040").value);
      const hAl2040 = parseNum($("h-al-2040").value);
      if (!isNaN(al2040) && !isNaN(hAl2040) && al2040 > hAl2040) {
        showFieldError("al-2040", "Al não pode ser maior que H+Al");
        valid = false;
      }
    }
  }

  return valid;
}

// ─── STEP 3 LOGIC ───
$("quick-select").addEventListener("click", e => {
  const btn = e.target.closest(".quick-btn");
  if (!btn) return;
  $("cao").value = btn.dataset.cao;
  $("mgo").value = btn.dataset.mgo;
  $("prnt").value = btn.dataset.prnt;
  updateLimestoneLive();
});

["cao", "mgo", "prnt"].forEach(id => {
  $(id).addEventListener("input", updateLimestoneLive);
});

function updateLimestoneLive() {
  const cao = parseNum($("cao").value);
  const mgo = parseNum($("mgo").value);

  if (isNaN(cao) || isNaN(mgo)) {
    $("limestone-tipo").textContent = "—";
    $("limestone-sum").textContent = "—";
    return;
  }

  const tipo = mgo > 12 ? "dolomítico" : mgo >= 5 ? "magnesiano" : "calcítico";
  $("limestone-tipo").textContent = tipo;

  const sum = cao + mgo;
  const sumEl = $("limestone-sum");
  const legalEl = $("limestone-legal");
  sumEl.textContent = sum.toFixed(1) + "%";
  if (sum >= 38) {
    sumEl.className = "";
    legalEl.className = "limestone-live-item legal-ok";
  } else {
    legalEl.className = "limestone-live-item legal-fail";
  }
}

$("btn-prev-3").addEventListener("click", () => goToStep(2));
$("btn-next-3").addEventListener("click", () => {
  if (validateStep3()) goToStep(4);
});

function validateStep3() {
  let valid = true;
  for (const id of ["cao", "mgo", "prnt"]) {
    const val = parseNum($(id).value);
    if ($(id).value.trim() === "" || isNaN(val)) {
      showFieldError(id, "Campo obrigatório"); valid = false;
    } else {
      clearFieldError(id);
    }
  }
  return valid;
}

// ─── STEP 4: RUN + RENDER ───
$("btn-prev-4").addEventListener("click", () => {
  destroyChart();
  goToStep(3);
});

$("btn-print").addEventListener("click", () => window.print());

function runAndRender() {
  let request, report;
  try {
    request = buildRequest();
    report = runDiagnostic(request);
  } catch (e) {
    if (e instanceof ValidationError) {
      renderValidationError(e);
      return;
    }
    renderGenericError(e);
    return;
  }

  try {
    $("result-banner").style.display = "";
    $("chart-container").style.display = "";
    renderBanner(report);
    renderMethodCards(report);
    renderDelta(report);
    renderChart(report.results, report.primary_method);
    renderProjections(report, request);
    renderWarnings(report);
    renderInputSummary(request);
  } catch (e) {
    renderGenericError(e);
  }
}

function buildRequest() {
  const layers = [buildLayer("0-20", "020")];
  if (state.include2040) layers.push(buildLayer("20-40", "2040"));
  const limestone = createLimestone({
    cao_pct: parseNum($("cao").value),
    mgo_pct: parseNum($("mgo").value),
    prnt: parseNum($("prnt").value),
  });
  return createLimingRequest({
    state: state.state,
    crop: state.crop,
    system: state.system,
    layers,
    limestone,
  });
}

function buildLayer(depth, suffix) {
  const data = {
    depth,
    ca: parseNum($(`ca-${suffix}`).value),
    mg: parseNum($(`mg-${suffix}`).value),
    k: parseNum($(`k-${suffix}`).value),
    al: parseNum($(`al-${suffix}`).value),
    h_al: parseNum($(`h-al-${suffix}`).value),
  };
  const clay = parseNum($(`clay-${suffix}`).value);
  if (!isNaN(clay)) data.clay_pct = clay;
  const phSmp = parseNum($(`ph-smp-${suffix}`).value);
  if (!isNaN(phSmp)) data.ph_smp = phSmp;
  const ctcLab = parseNum($(`ctc-lab-${suffix}`).value);
  if (!isNaN(ctcLab)) data.ctc_lab = ctcLab;
  return createSoilLayer(data);
}

// ─── RENDERERS ───
function renderBanner(report) {
  const layers = report.moreira_result.nc_per_layer;
  const hasSubsurface = "20-40" in layers;
  const bannerLabel = $("result-banner-label");

  if (hasSubsurface) {
    bannerLabel.textContent = "Necessidade de Calagem — Moreira et al. (2025) — Perfil 0-40 cm";
  } else {
    bannerLabel.textContent = "Necessidade de Calagem — Moreira et al. (2025) — Camada 0-20 cm";
  }

  $("banner-nc").textContent = report.moreira_result.nc_tha.toFixed(2);
  const breakdown = $("banner-breakdown");
  breakdown.innerHTML = "";

  for (const [depth, nc] of Object.entries(layers)) {
    const span = document.createElement("span");
    span.className = "breakdown-item";
    span.textContent = `${depth} cm: ${nc.toFixed(2)} t/ha`;
    breakdown.appendChild(span);
  }

  if (hasSubsurface) {
    const total = document.createElement("span");
    total.className = "breakdown-item breakdown-total";
    total.textContent = `Total perfil: ${report.moreira_result.nc_tha.toFixed(2)} t/ha`;
    breakdown.appendChild(total);
  }
}

function renderMethodCards(report) {
  const grid = $("method-grid");
  grid.innerHTML = "";

  for (const result of report.results) {
    const display = METHOD_DISPLAY[result.method] || { name: result.method, desc: "" };
    const isPrimary = result.method === report.primary_method;
    const isMoreira = result.method === "moreira_2025";

    const card = document.createElement("div");
    card.className = `method-card${isPrimary ? " primary" : ""}${!result.available ? " unavailable" : ""}`;

    let badges = "";
    if (isMoreira) badges += `<span class="badge badge-green">Referência</span>`;
    if (isPrimary && result.available) badges += `<span class="badge badge-gold">Estadual</span>`;

    card.innerHTML = `
      <div class="method-name">${display.name} ${badges}</div>
      <div class="method-nc">${result.available ? result.nc_tha.toFixed(2) + " t/ha" : "—"}</div>
      <div class="method-desc">${display.desc}</div>
      ${result.reason ? `<div class="method-reason">${escapeHtml(result.reason)}</div>` : ""}
    `;
    grid.appendChild(card);
  }
}

function renderDelta(report) {
  const section = $("delta-section");
  if (report.delta_tha === null) {
    section.style.display = "none";
    return;
  }
  section.style.display = "block";

  const moreira020 = report.moreira_result.nc_per_layer["0-20"];
  const primaryResult = report.results.find(r => r.method === report.primary_method);
  const estadual = primaryResult ? primaryResult.nc_tha : 0;
  const maxVal = Math.max(moreira020, estadual, 1);

  const bars = $("delta-bars");
  bars.innerHTML = `
    <div class="delta-bar-row">
      <div class="delta-bar-label">Moreira (0-20)</div>
      <div class="delta-bar-track">
        <div class="delta-bar-fill green" style="width:${(moreira020 / maxVal * 100).toFixed(0)}%">
          <span class="delta-bar-value">${moreira020.toFixed(2)}</span>
        </div>
      </div>
    </div>
    <div class="delta-bar-row">
      <div class="delta-bar-label">${STATE_METHOD_NAMES[report.primary_method] || "Estadual"}</div>
      <div class="delta-bar-track">
        <div class="delta-bar-fill amber" style="width:${(estadual / maxVal * 100).toFixed(0)}%">
          <span class="delta-bar-value">${estadual.toFixed(2)}</span>
        </div>
      </div>
    </div>
  `;

  const interp = $("delta-interpretation");
  const delta = report.delta_tha;
  if (delta > 0.5) {
    interp.textContent = `Moreira recomenda ${Math.abs(delta).toFixed(2)} t/ha a mais que o método estadual na camada 0-20. Possível subdosagem histórica pelo método regional.`;
  } else if (delta < -0.5) {
    interp.textContent = `O método estadual recomenda ${Math.abs(delta).toFixed(2)} t/ha a mais que Moreira na 0-20. O método regional pode estar sendo conservador ou o solo tem acidez subsuperficial importante.`;
  } else {
    interp.textContent = `Os métodos convergem na camada 0-20 (diferença de apenas ${Math.abs(delta).toFixed(2)} t/ha). Boa convergência entre abordagens.`;
  }
}

function renderProjections(report, request) {
  const tbody = $("projection-tbody");
  tbody.innerHTML = "";

  const layers = request.layers;
  for (const layer of layers) {
    const d = layer.depth;
    const caProj = report.ca_projection[d];
    const mgProj = report.mg_projection[d];
    const ratio = d === "0-20" && report.ca_mg_ratio_projected !== null
      ? report.ca_mg_ratio_projected.toFixed(1) : "—";
    const mgSat = report.mg_saturation_projected[d];

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${d} cm</td>
      <td class="mono">${layer.ca.toFixed(2)}</td>
      <td class="arrow">&rarr;</td>
      <td class="mono">${caProj.toFixed(2)}</td>
      <td class="mono">${layer.mg.toFixed(2)}</td>
      <td class="arrow">&rarr;</td>
      <td class="mono">${mgProj.toFixed(2)}</td>
      <td class="mono">${ratio}</td>
      <td class="mono">${mgSat.toFixed(0)}%</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderWarnings(report) {
  const section = $("warnings-section");
  section.innerHTML = "";
  if (report.warnings.length === 0) return;

  const title = document.createElement("h3");
  title.className = "card-title";
  title.style.marginBottom = "1rem";
  title.textContent = "Avisos e Recomendações";
  section.appendChild(title);

  for (const w of report.warnings) {
    const card = document.createElement("div");
    const isError = w.includes("não atende") || w.includes("fora do range");
    const isInfo = w.includes("não informada") || w.includes("gessagem") || w.includes("Corretivo especial");
    card.className = `warning-card ${isError ? "error" : isInfo ? "info" : "warning"}`;
    card.innerHTML = `
      <span class="warning-icon">${isError ? "&#9888;" : isInfo ? "&#8505;" : "&#9888;"}</span>
      <span class="warning-text">${escapeHtml(w)}</span>
    `;
    section.appendChild(card);
  }
}

function renderInputSummary(request) {
  const grid = $("input-summary-grid");
  grid.innerHTML = "";
  const layer020 = request.layers.find(l => l.depth === "0-20");
  const items = [
    ["Estado", request.state],
    ["Cultura", request.crop],
    ["Sistema", request.system === "abertura" ? "Abertura de Área" : "SPD"],
    ["Ca (0-20)", layer020.ca.toFixed(2)],
    ["Mg (0-20)", layer020.mg.toFixed(2)],
    ["K (0-20)", layer020.k.toFixed(3)],
    ["Al (0-20)", layer020.al.toFixed(2)],
    ["H+Al (0-20)", layer020.h_al.toFixed(2)],
    ["CaO%", request.limestone.cao_pct.toFixed(1)],
    ["MgO%", request.limestone.mgo_pct.toFixed(1)],
    ["PRNT%", request.limestone.prnt.toFixed(1)],
    ["Tipo", request.limestone.tipo],
  ];
  if (layer020.clay_pct !== null) items.push(["Argila%", layer020.clay_pct.toFixed(1)]);
  if (layer020.ph_smp !== null) items.push(["pH SMP", layer020.ph_smp.toFixed(1)]);

  for (const [label, value] of items) {
    const div = document.createElement("div");
    div.className = "input-summary-item";
    div.innerHTML = `<span class="input-summary-label">${escapeHtml(label)}</span><span class="input-summary-value">${escapeHtml(String(value))}</span>`;
    grid.appendChild(div);
  }
}

function hideResultSections() {
  $("result-banner").style.display = "none";
  $("delta-section").style.display = "none";
  $("chart-container").style.display = "none";
  $("warnings-section").innerHTML = "";
  $("projection-tbody").innerHTML = "";
  $("input-summary-grid").innerHTML = "";
}

function renderValidationError(e) {
  hideResultSections();
  const grid = $("method-grid");
  grid.innerHTML = `<div class="warning-card error" style="grid-column:1/-1">
    <span class="warning-icon">&#9888;</span>
    <span class="warning-text"><strong>Erro de validação (${escapeHtml(e.field)}):</strong> ${escapeHtml(e.message)}</span>
  </div>`;
}

function renderGenericError(e) {
  hideResultSections();
  const grid = $("method-grid");
  grid.innerHTML = `<div class="warning-card error" style="grid-column:1/-1">
    <span class="warning-icon">&#9888;</span>
    <span class="warning-text"><strong>Erro:</strong> ${escapeHtml(e.message || String(e))}</span>
  </div>`;
}

// ─── HELPERS ───
function escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

function showError(field, msg) {
  const el = $(`${field}-error`);
  if (el) { el.textContent = msg; el.classList.add("visible"); }
  const input = $(field);
  if (input) input.setAttribute("aria-invalid", "true");
}

function clearError(field) {
  const el = $(`${field}-error`);
  if (el) { el.textContent = ""; el.classList.remove("visible"); }
  const input = $(field);
  if (input) input.removeAttribute("aria-invalid");
}

function showFieldError(id, msg) {
  const el = $(`${id}-error`);
  if (el) { el.textContent = msg; el.classList.add("visible"); }
  const input = $(id);
  if (input) input.setAttribute("aria-invalid", "true");
}

function clearFieldError(id) {
  const el = $(`${id}-error`);
  if (el) { el.textContent = ""; el.classList.remove("visible"); }
  const input = $(id);
  if (input) input.removeAttribute("aria-invalid");
}
