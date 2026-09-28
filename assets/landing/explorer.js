const english = document.documentElement.lang.startsWith('en');
const labels = {
  "million": [
    " mi",
    "M"
  ],
  "thousand": [
    " mil",
    "k"
  ],
  "history": [
    "Série histórica · Conab · ",
    "Historical series · Conab · "
  ],
  "pending": [
    "Conab · Ainda sem dados nesta coleta",
    "Conab · No data in this collection yet"
  ],
  "viewYear": [
    "Ver dados de ",
    "View data for "
  ],
  "historyAria": [
    "Produção histórica em toneladas. ",
    "Historical production in tonnes. "
  ],
  "historyTitle": [
    "Histórico da produção · ",
    "Production history · "
  ],
  "zeroChange": [
    "0,0%",
    "0.0%"
  ],
  "reference": [
    "Produção de referência: ",
    "Reference production: "
  ],
  "noValue": [
    "Sem valor numérico publicado",
    "No numerical value published"
  ],
  "noShortValue": [
    "Sem valor publicado",
    "No published value"
  ],
  "tonnes": [
    " toneladas",
    " tonnes"
  ],
  "stateTotal": [
    "SOMA DAS UFs",
    "STATE TOTAL"
  ],
  "smallShare": [
    "< 0,1%",
    "< 0.1%"
  ],
  "pendingData": [
    "Dados ainda não disponíveis nesta coleta",
    "Data not yet available in this collection"
  ],
  "brazil": [
    "Brasil",
    "Brazil"
  ],
  "estimate": [
    "ESTIMATIVA · CONAB",
    "ESTIMATE · CONAB"
  ],
  "historical": [
    "HISTÓRICO · CONAB",
    "HISTORY · CONAB"
  ],
  "touch": [
    "Toque em um estado · Pinça para aproximar",
    "Tap a state · Pinch to zoom"
  ],
  "drag": [
    "Arraste para girar · Clique em um estado",
    "Drag to rotate · Click a state"
  ],
  "pauseYears": [
    "Pausar evolução dos anos",
    "Pause year playback"
  ],
  "playYears": [
    "Animar evolução dos anos",
    "Play through the years"
  ],
  "availableStates": [
    "Brasil · UFs disponíveis",
    "Brazil · Available states"
  ],
  "pauseRotation": [
    "Pausar rotação automática",
    "Pause auto-rotation"
  ],
  "rotate": [
    "Girar automaticamente",
    "Auto-rotate"
  ],
  "pause": [
    "Pausar rotação",
    "Pause rotation"
  ],
  "canvas": [
    "Mapa 3D do Brasil com cores naturais do terreno. A altura dos estados representa a produção agrícola, independentemente das cores da superfície. Clique em um estado ou use o seletor para consultar os dados.",
    "3D map of Brazil with natural terrain colours. State height represents agricultural production, independently of surface colours. Click a state or use the selector to explore its data."
  ],
  "interrupted": [
    "A visualização foi interrompida. Os dados continuam disponíveis ao lado.",
    "The 3D view was interrupted. You can still explore the data in the panel."
  ],
  "preparing": [
    "Preparando o território…",
    "Preparing the map…"
  ],
  "failed3D": [
    "O 3D não carregou neste navegador. Você pode continuar explorando os dados.",
    "The 3D map could not load in this browser. You can still explore the data."
  ]
};
for (const key of Object.keys(labels)) labels[key] = labels[key][english ? 1 : 0];

let explorerData;
let explorerMaximums;
let explorerInitialized = false;
const explorerElement = id => document.getElementById(id);
const naturalMapData = {"image": "/assets/terrain/blue-marble.jpg", "bounds": [-75, -35, -32, 7]};
const explorerState = { crop: 'soja', year: null, uf: 'MT', period: null, historyYear: null };
const cropNames = english ? { soja: 'Soybean', milho: 'Corn', cafe: 'Coffee', arroz: 'Rice', trigo: 'Wheat' } : { soja: 'soja', milho: 'milho', cafe: 'café', arroz: 'arroz', trigo: 'trigo' };
const areaLabels = { 'Área plantada': 'Planted area', 'Área em produção': 'Area in production' };
const productionLabel = () => english ? cropNames[explorerState.crop] + ' production' : 'Produção de ' + cropNames[explorerState.crop];
const terrainStage = explorerElement('terrainStage');
const terrainLoading = explorerElement('terrainLoading');
const terrainLoadingText = explorerElement('terrainLoadingText');
const terrainRetry = explorerElement('terrainRetry');
const terrainStatus = explorerElement('terrainStatus');
const terrainMotion = window.matchMedia('(prefers-reduced-motion: reduce)');

let terrainRuntime;
let terrainPending = false;
let terrainVisible = false;
let terrainFrame = 0;
let terrainLastTime = 0;
let explorerTimeTimer;
let terrainReducedMotion = terrainMotion.matches;

function synchronizeTerrainMotion() {
  if (terrainReducedMotion === terrainMotion.matches) return;
  terrainReducedMotion = terrainMotion.matches;
  if (terrainRuntime) {
    terrainRuntime.controls.enableDamping = !terrainReducedMotion;
    if (terrainReducedMotion) setTerrainRotation(false);
  }
  if (terrainReducedMotion) setTimelinePlaying(false);
}

function formatNumber(value, digits = 0) {
  return value === null || value === undefined ? '—' : new Intl.NumberFormat(atlasLocale, { maximumFractionDigits: digits }).format(value);
}

function compactNumber(value) {
  if (value === null || value === undefined) return '—';
  if (value >= 1000000) return formatNumber(value / 1000000, 1) + labels.million;
  if (value >= 10000) return formatNumber(value / 1000, 1) + labels.thousand;
  return formatNumber(value);
}

function productionMeta(year = explorerState.year) {
  return explorerData.production[Object.hasOwn(explorerData.production.history, year) ? 'historyMeta' : 'currentMeta'][year]?.[explorerState.crop] || {};
}

function productionSource(year = explorerState.year) {
  return explorerData.production.sourceCalls.find(call => call.id === productionMeta(year).sourceAuditId);
}

function seasonLabel(year = explorerState.year) {
  const season = ['cafe', 'trigo'].includes(explorerState.crop) ? year : (year - 1) + '/' + String(year).slice(-2);
  return english ? season + ' season' : 'Safra ' + season;
}

function timelineYears() {
  return Object.keys(explorerData.production.history).map(Number).sort((a, b) => a - b);
}

function productionRecords(year = explorerState.year) {
  return explorerData.production[Object.hasOwn(explorerData.production.history, year) ? 'history' : 'current'][year]?.[explorerState.crop] || {};
}

function productionFor(uf = explorerState.uf, year = explorerState.year) {
  return productionRecords(year)?.[uf] || [null, null, null];
}

function sourceDate() {
  return 'Conab · ' + seasonLabel();
}

function updatePeriodControl() {
  const historical = explorerState.period === 'history';
  const years = timelineYears();
  const limit = years.at(-1);
  explorerElement('explorerPeriod').value = explorerState.period;
  explorerElement('explorerTimeline').hidden = !historical;
  const note = explorerElement('explorerPeriodNote');
  note.dataset.kind = historical ? 'historical' : 'estimate';
  note.textContent = historical ? labels.history + seasonLabel() : productionMeta().levantamento ? (english ? 'Conab · Survey ' + productionMeta().levantamento + ' · ' : 'Conab · ' + productionMeta().levantamento + 'º levantamento · ') + seasonLabel() : labels.pending;
  const range = explorerElement('explorerYear');
  range.min = years[0];
  range.max = limit;
  range.value = explorerState.year;
  explorerElement('explorerYearValue').value = explorerState.year;
  range.style.setProperty('--progress', Math.max(0, Math.min(100, (explorerState.year - years[0]) / Math.max(1, limit - years[0]) * 100)) + '%');
  const ticks = explorerElement('explorerYearTicks');
  if (ticks.dataset.limit !== String(limit)) {
    ticks.dataset.limit = limit;
    ticks.replaceChildren(...years.map(year => {
      const button = document.createElement('button');
      button.type = 'button';
      button.dataset.explorerYear = year;
      button.textContent = year;
      button.setAttribute('aria-label', labels.viewYear + year);
      button.addEventListener('click', () => changeExplorerYear(year));
      return button;
    }));
  }
  document.querySelectorAll('[data-explorer-year]').forEach(button => button.setAttribute('aria-pressed', String(Number(button.dataset.explorerYear) === explorerState.year)));
}

function drawProductionTrend() {
  const periods = timelineYears();
  const values = periods.map(year => productionFor(explorerState.uf, year)[0]);
  explorerElement('explorerTrendPanel').hidden = values.filter(value => value !== null).length < 2;
  const maximum = Math.max(1, ...values.filter(value => value !== null));
  const points = values.map((value, index) => value === null ? null : [9 + index * 270 / (periods.length - 1), 59 - value / maximum * 33]);
  let path = '';
  let connected = false;
  for (const point of points) {
    if (!point) { connected = false; continue; }
    path += (connected ? 'L' : 'M') + point.join(' ') + ' ';
    connected = true;
  }
  const currentIndex = periods.indexOf(explorerState.year);
  const dots = points.map((point, index) => point ? '<circle cx="' + point[0] + '" cy="' + point[1] + '" r="' + (index === currentIndex ? 4 : 2.5) + '" fill="var(' + (index === currentIndex ? '--palette-selected' : '--palette-trend-dot') + ')"><title>' + seasonLabel(periods[index]) + ': ' + formatNumber(values[index]) + ' t</title></circle>' : '').join('');
  explorerElement('explorerTrend').innerHTML = '<text x="9" y="13" fill="var(--palette-muted)" font-size="12">' + compactNumber(maximum) + ' t</text><text x="9" y="74" fill="var(--palette-muted)" font-size="11">0</text><path d="M9 26H279M9 59H279" stroke="var(--palette-muted)" stroke-opacity=".16" stroke-dasharray="3 4"/><path d="' + path + '" fill="none" stroke="var(--palette-trend)" stroke-width="2" stroke-linejoin="round"/>' + dots;
  explorerElement('explorerTrend').setAttribute('aria-label', labels.historyAria + values.map((value, index) => seasonLabel(periods[index]) + ': ' + formatNumber(value)).join('; '));
  explorerElement('explorerTrendHeading').textContent = labels.historyTitle + periods[0] + '–' + periods.at(-1);
  explorerElement('explorerTrendStart').textContent = periods[0];
  explorerElement('explorerTrendEnd').textContent = periods.at(-1);
}

function updateProductionChange() {
  const current = productionFor()[0];
  const previous = productionFor(explorerState.uf, explorerState.year - 1)[0];
  const currentCoverage = productionMeta().coverage?.availableUFs || [];
  const previousCoverage = productionMeta(explorerState.year - 1).coverage?.availableUFs || [];
  const sameCoverage = currentCoverage.join(',') === previousCoverage.join(',');
  const element = explorerElement('explorerChange');
  element.hidden = current === null || previous === null || previous === 0 || (explorerState.uf === 'BR' && !sameCoverage);
  if (element.hidden) return;
  const change = (current / previous - 1) * 100;
  const flat = Math.abs(change) < .05;
  element.dataset.direction = flat ? 'flat' : change > 0 ? 'up' : 'down';
  element.innerHTML = '<strong>' + (flat ? labels.zeroChange : (change > 0 ? '↑ ' : '↓ ') + formatNumber(Math.abs(change), 1) + '%') + '</strong><span>vs. ' + seasonLabel(explorerState.year - 1).toLowerCase() + '</span>';
  element.title = labels.reference + formatNumber(previous) + ' t · Conab';
}

function updateProductionPanel() {
  const [production, area, yieldValue] = productionFor();
  const national = productionFor('BR')[0];
  const share = production === null || !national ? null : production / national * 100;
  const ranking = Object.entries(productionRecords() || {}).filter(([uf, values]) => uf !== 'BR' && values[0] > 0).sort((a, b) => b[1][0] - a[1][0]);
  const rank = ranking.findIndex(([uf]) => uf === explorerState.uf);
  explorerElement('explorerMetricTitle').textContent = productionLabel();
  explorerElement('explorerProduction').innerHTML = compactNumber(production) + (production === null ? '' : '<small>t</small>');
  explorerElement('explorerExact').textContent = production === null ? labels.noValue : formatNumber(production) + labels.tonnes;
  explorerElement('explorerAreaLabel').textContent = (english ? areaLabels[productionMeta().areaLabel] : productionMeta().areaLabel) || (english ? 'Planted area' : 'Área plantada');
  explorerElement('explorerArea').textContent = formatNumber(area) + (area === null ? '' : ' ha');
  explorerElement('explorerYield').textContent = formatNumber(yieldValue) + (yieldValue === null ? '' : ' kg/ha');
  explorerElement('explorerRank').textContent = explorerState.uf === 'BR' ? labels.stateTotal : rank < 0 ? '' : (english ? 'RANK ' + (rank + 1) + ' BY STATE' : (rank + 1) + 'º ENTRE AS UFs');
  explorerElement('explorerShare').textContent = share === null ? '—' : share > 0 && share < .1 ? labels.smallShare : formatNumber(share, 1) + '%';
  explorerElement('explorerShareBar').style.width = (share || 0) + '%';
  explorerElement('explorerLatest').hidden = !!productionMeta().collection;
  if (!productionMeta().collection) explorerElement('explorerExact').textContent = labels.pendingData;
  updateProductionChange();
  drawProductionTrend();
}

function updateProductionExplorer() {
  synchronizeTerrainMotion();
  const territory = explorerState.uf === 'BR' ? labels.brazil : explorerData.states[explorerState.uf].name;
  explorerElement('explorerState').value = explorerState.uf;
  explorerElement('explorerCrop').value = explorerState.crop;
  updatePeriodControl();
  explorerElement('explorerStageTitle').textContent = productionLabel();
  explorerElement('explorerStageBadge').textContent = explorerState.period === 'history' ? labels.historical : labels.estimate;
  explorerElement('explorerScaleMax').textContent = compactNumber(explorerMaximums[explorerState.crop]) + ' t';
  explorerElement('explorerSourceText').textContent = sourceDate();
  explorerElement('explorerSourceLink').href = productionSource()?.meta.source_url || 'https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras';
  const touch = window.matchMedia('(pointer: coarse)').matches;
  explorerElement('terrainHint').textContent = touch ? labels.touch : labels.drag;
  explorerElement('explorerTooltip').hidden = true;
  explorerElement('explorerSelectedLabel').hidden = explorerState.uf === 'BR' || !terrainRuntime;
  updateProductionPanel();
  if (!explorerTimeTimer && !atlasState.playing) terrainStatus.textContent = territory + ', ' + explorerState.year + '. ' + seasonLabel() + '. ' + productionLabel() + ': ' + formatNumber(productionFor()[0]) + (english ? ' tonnes. Conab' : ' toneladas. Conab');
  if (terrainRuntime) {
    updateMapTargets();
    requestTerrainRender();
  }
}

function changeExplorerYear(year, manual = true) {
  if (manual) setTimelinePlaying(false);
  explorerState.year = Number(year);
  explorerState.historyYear = explorerState.year;
  updateExplorer();
}

function setTimelinePlaying(playing) {
  const wasPlaying = !!explorerTimeTimer;
  clearInterval(explorerTimeTimer);
  explorerTimeTimer = undefined;
  if (!explorerData) return;
  const button = explorerElement('explorerTimePlay');
  const years = timelineYears();
  button.setAttribute('aria-pressed', String(playing));
  button.setAttribute('aria-label', playing ? labels.pauseYears : labels.playYears);
  button.querySelector('use').setAttribute('href', playing ? '#i-pause' : '#i-play');
  if (playing) {
    if (explorerState.year === years.at(-1)) changeExplorerYear(years[0], false);
    explorerTimeTimer = setInterval(() => {
      if (explorerState.year === years.at(-1)) { setTimelinePlaying(false); return; }
      changeExplorerYear(explorerState.year + 1, false);
    }, 1500);
  }
  terrainStatus.setAttribute('aria-live', explorerTimeTimer || atlasState.playing ? 'off' : 'polite');
  if (!playing && wasPlaying && !landcoverActive()) updateProductionExplorer();
}

function terrainPalette() {
  const style = getComputedStyle(explorerElement('terrainViewer'));
  return Object.fromEntries(['edge', 'selected', 'ambient', 'rim', 'fill'].map(name => [name, style.getPropertyValue('--palette-' + name).trim()]));
}

function initializeExplorer() {
  const years = timelineYears();
  const currentYear = Math.max(...Object.keys(explorerData.production.current).map(Number));
  if (!years.length || !Number.isFinite(currentYear)) throw new Error('explorer-periods');
  explorerState.year = currentYear;
  explorerState.period = String(currentYear);
  explorerState.historyYear = years.at(-1);
  explorerElement('explorerPeriod').replaceChildren(new Option(currentYear, String(currentYear)), new Option((english ? 'History · ' : 'Histórico · ') + years[0] + '–' + years.at(-1), 'history'));
  explorerElement('explorerLatest').textContent = (english ? 'View history for ' : 'Ver histórico de ') + years.at(-1);
  const select = explorerElement('explorerState');
  const entries = [['BR', labels.availableStates], ...Object.entries(explorerData.states).map(([uf, item]) => [uf, item.name + ' · ' + uf]).sort((a, b) => a[1].localeCompare(b[1], atlasLocale))];
  select.replaceChildren(...entries.map(([value, label]) => new Option(label, value)));
  select.addEventListener('change', () => { explorerState.uf = select.value; updateExplorer(); });
  explorerElement('explorerCrop').addEventListener('change', event => { explorerState.crop = event.target.value; updateExplorer(); });
  explorerElement('explorerPeriod').addEventListener('change', event => {
    const period = event.target.value;
    setTimelinePlaying(false);
    explorerState.period = period;
    explorerState.year = explorerState.period === 'history' ? explorerState.historyYear : Number(explorerState.period);
    updateExplorer();
  });
  explorerElement('explorerLatest').addEventListener('click', () => {
    setTimelinePlaying(false);
    explorerState.period = 'history';
    explorerState.historyYear = timelineYears().at(-1);
    explorerState.year = explorerState.historyYear;
    updateExplorer();
  });
  explorerElement('explorerYear').addEventListener('input', event => changeExplorerYear(event.target.value));
  explorerElement('explorerTimePlay').addEventListener('click', () => setTimelinePlaying(!explorerTimeTimer));
  const fetched = explorerData.audit.verified_at.slice(0, 10);
  explorerElement('explorerFetchedDate').textContent = new Date(fetched + 'T12:00:00').toLocaleDateString(atlasLocale, { day: '2-digit', month: 'short', year: 'numeric' });
  updateExplorer();
}

function projectCoordinate(point) {
  return [(point[0] + 54) * .3478, (point[1] + 14) * .37];
}

function smoothTerrainSides(T, geometry) {
  const positions = geometry.attributes.position;
  const normals = geometry.attributes.normal;
  const shared = new Map();
  const vertices = [];
  for (const group of geometry.groups.filter(item => item.materialIndex === 1)) {
    for (let index = group.start; index < group.start + group.count; index += 1) {
      const key = [positions.getX(index), positions.getY(index), positions.getZ(index)].map(value => value.toFixed(5)).join(':');
      const normal = shared.get(key) || new T.Vector3();
      normal.add(new T.Vector3().fromBufferAttribute(normals, index));
      shared.set(key, normal);
      vertices.push([index, key]);
    }
  }
  shared.forEach(normal => normal.normalize());
  vertices.forEach(([index, key]) => {
    const normal = shared.get(key);
    normals.setXYZ(index, normal.x, normal.y, normal.z);
  });
  normals.needsUpdate = true;
}

function applyTerrainUVs(geometry) {
  const [west, south, east, north] = naturalMapData.bounds;
  const positions = geometry.attributes.position;
  const uv = geometry.attributes.uv;
  for (let index = 0; index < positions.count; index += 1) {
    const longitude = positions.getX(index) / .3478 - 54;
    const latitude = -positions.getZ(index) / .37 - 14;
    uv.setXY(index, (longitude - west) / (east - west), (latitude - south) / (north - south));
  }
  uv.needsUpdate = true;
}

function buildStateOutline(lines, polygons, material) {
  const positions = [];
  for (const rings of polygons) {
    for (const ring of rings) {
      for (let index = 0; index < ring.length; index += 1) {
        const [x, y] = projectCoordinate(ring[index]);
        const [nextX, nextY] = projectCoordinate(ring[(index + 1) % ring.length]);
        if (x === nextX && y === nextY) continue;
        positions.push(x, 1.032, -y, nextX, 1.032, -nextY);
      }
    }
  }
  const geometry = new lines.LineSegmentsGeometry();
  geometry.setPositions(positions);
  return new lines.LineSegments2(geometry, material);
}

function buildBrazil(T, surfaceTexture, lines, mapBlend) {
  const model = new T.Group();
  const states = new Map();
  const pickable = [];
  const palette = terrainPalette();
  for (const [uf, polygons] of Object.entries(explorerData.geometry)) {
    const group = new T.Group();
    const material = new T.MeshPhysicalMaterial({ color: '#ffffff', map: surfaceTexture, roughness: .95, metalness: 0, clearcoat: 0, envMapIntensity: .12 });
    configureTerrainBlend(material, mapBlend);
    const atlasMaterial = new T.MeshBasicMaterial({ map: surfaceTexture, toneMapped: false });
    configureTerrainBlend(atlasMaterial, mapBlend);
    const sideMaterial = new T.MeshStandardMaterial({ color: '#394735', roughness: .92, metalness: 0, envMapIntensity: .08 });
    const outlineMaterial = new lines.LineMaterial({ color: palette.edge, linewidth: 1.35, transparent: true, opacity: .78, depthWrite: false, alphaToCoverage: true, toneMapped: false });
    for (const rings of polygons) {
      const shape = new T.Shape(rings[0].map(point => new T.Vector2(...projectCoordinate(point))));
      rings.slice(1).forEach(ring => shape.holes.push(new T.Path(ring.map(point => new T.Vector2(...projectCoordinate(point))))));
      const geometry = new T.ExtrudeGeometry(shape, { depth: 1, bevelEnabled: true, bevelThickness: .026, bevelSize: .012, bevelSegments: 2, steps: 1, curveSegments: 1 });
      geometry.rotateX(-Math.PI / 2);
      smoothTerrainSides(T, geometry);
      applyTerrainUVs(geometry);
      const stateMesh = new T.Mesh(geometry, [material, sideMaterial]);
      stateMesh.userData.uf = uf;
      stateMesh.castShadow = true;
      stateMesh.receiveShadow = true;
      group.add(stateMesh);
      pickable.push(stateMesh);
    }
    group.add(buildStateOutline(lines, polygons, outlineMaterial));
    const center = new T.Box3().setFromObject(group).getCenter(new T.Vector3());
    group.scale.y = .22;
    model.add(group);
    states.set(uf, { group, material, atlasMaterial, sideMaterial, outlineMaterial, center, targetHeight: .22 });
  }
  return { model, states, pickable };
}

function updateMapTargets() {
  const { brazil, lights } = terrainRuntime;
  const palette = terrainPalette();
  lights.ambient.color.set(palette.ambient);
  lights.rim.color.set(palette.rim);
  lights.fill.color.set(palette.fill);
  const maximum = explorerMaximums[explorerState.crop];
  for (const [uf, state] of brazil.states) {
    const value = productionFor(uf)[0];
    const fraction = (value || 0) / maximum;
    const selected = !landcoverActive() && uf === explorerState.uf;
    state.targetHeight = landcoverActive() ? .22 : .22 + fraction * 1.15;
    state.sideMaterial.color.set(landcoverActive() ? '#213b30' : '#394735');
    state.outlineMaterial.color.set(selected ? palette.selected : palette.edge);
    state.outlineMaterial.opacity = selected ? 1 : landcoverActive() ? .45 : .78;
    state.outlineMaterial.linewidth = selected ? 2.3 : landcoverActive() ? 1 : 1.35;
    if (terrainMotion.matches) state.group.scale.y = state.targetHeight;
  }
}

function disposeModel(model) {
  const geometries = new Set();
  const materials = new Set();
  model.traverse(object => {
    if (object.geometry) geometries.add(object.geometry);
    if (Array.isArray(object.material)) object.material.forEach(material => materials.add(material));
    else if (object.material) materials.add(object.material);
  });
  geometries.forEach(geometry => geometry.dispose());
  materials.forEach(material => material.dispose());
  model.removeFromParent();
}

function terrainCameraOffset(T, camera, target) {
  const backward = new T.Vector3(.14, .84, .64).normalize();
  const right = new T.Vector3().crossVectors(new T.Vector3(0, 1, 0), backward).normalize();
  const up = new T.Vector3().crossVectors(backward, right).normalize();
  const vertical = Math.tan(camera.fov * Math.PI / 360) * .9;
  const horizontal = vertical * terrainStage.clientWidth / terrainStage.clientHeight;
  let distance = 0;
  for (const polygons of Object.values(explorerData.geometry)) {
    for (const rings of polygons) {
      for (const coordinate of rings[0]) {
        const [x, y] = projectCoordinate(coordinate);
        for (const height of [0, 1.94]) {
          const point = new T.Vector3(x, height, -y).sub(target);
          distance = Math.max(distance, point.dot(backward) + Math.max(Math.abs(point.dot(right)) / horizontal, Math.abs(point.dot(up)) / vertical));
        }
      }
    }
  }
  return backward.multiplyScalar(distance);
}

function resetTerrain() {
  if (!terrainRuntime) return;
  terrainRuntime.camera = terrainRuntime.perspectiveCamera;
  terrainRuntime.controls.object = terrainRuntime.camera;
  terrainRuntime.shadow.visible = true;
  terrainRuntime.brazil.pickable.forEach(mesh => { mesh.receiveShadow = true; });
  const { camera, controls } = terrainRuntime;
  setTerrainRotation(false);
  controls.target.set(.45, .35, .7);
  camera.position.copy(controls.target).add(terrainCameraOffset(terrainRuntime.T, camera, controls.target));
  controls.update();
  requestTerrainRender();
}

function zoomTerrain(factor) {
  const { camera, controls } = terrainRuntime;
  if (camera.isOrthographicCamera) {
    camera.zoom = Math.max(controls.minZoom, Math.min(controls.maxZoom, camera.zoom / factor));
    camera.updateProjectionMatrix();
    controls.update();
    requestTerrainRender();
    return;
  }
  const offset = camera.position.clone().sub(controls.target);
  const distance = Math.max(controls.minDistance, Math.min(controls.maxDistance, offset.length() * factor));
  camera.position.copy(controls.target).add(offset.setLength(distance));
  controls.update();
  requestTerrainRender();
}

function setTerrainRotation(enabled) {
  if (!terrainRuntime) return;
  terrainRuntime.controls.autoRotate = enabled;
  const button = document.querySelector('[data-terrain-action="rotate"]');
  button.setAttribute('aria-pressed', String(enabled));
  button.setAttribute('aria-label', enabled ? labels.pauseRotation : labels.rotate);
  button.title = enabled ? labels.pause : labels.rotate;
  requestTerrainRender();
}

function bindTerrainControls() {
  document.querySelectorAll('[data-terrain-action]').forEach(button => {
    button.onclick = () => {
      if (!terrainRuntime) return;
      const action = button.dataset.terrainAction;
      if (landcoverActive() && handleMunicipalControl(action)) return;
      if (action === 'zoom-in') zoomTerrain(.86);
      if (action === 'zoom-out') zoomTerrain(1.16);
      if (action === 'rotate') setTerrainRotation(!terrainRuntime.controls.autoRotate);
      if (action === 'reset') resetTerrain();
      if (action === 'top') {
        setTerrainRotation(false);
        const { topCamera, controls } = terrainRuntime;
        terrainRuntime.camera = topCamera;
        controls.object = topCamera;
        terrainRuntime.shadow.visible = false;
        terrainRuntime.brazil.pickable.forEach(mesh => { mesh.receiveShadow = false; });
        topCamera.zoom = 1;
        topCamera.position.copy(controls.target).add(new terrainRuntime.T.Vector3(0, 32, .02));
        topCamera.updateProjectionMatrix();
        controls.update();
        requestTerrainRender();
      }
    };
  });
  terrainRuntime.renderer.domElement.addEventListener('keydown', event => {
    const { T, camera, controls } = terrainRuntime;
    if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', '+', '=', '-', 'Home'].includes(event.key)) return;
    event.preventDefault();
    if (event.key === '+' || event.key === '=') return zoomTerrain(.86);
    if (event.key === '-') return zoomTerrain(1.16);
    if (event.key === 'Home') return landcoverActive() ? selectMunicipality('BR') : resetTerrain();
    setTerrainRotation(false);
    const spherical = new T.Spherical().setFromVector3(camera.position.clone().sub(controls.target));
    if (event.key === 'ArrowLeft') spherical.theta -= .14;
    if (event.key === 'ArrowRight') spherical.theta += .14;
    if (event.key === 'ArrowUp') spherical.phi = Math.max(controls.minPolarAngle, spherical.phi - .12);
    if (event.key === 'ArrowDown') spherical.phi = Math.min(controls.maxPolarAngle, spherical.phi + .12);
    camera.position.copy(controls.target).add(new T.Vector3().setFromSpherical(spherical));
    controls.update();
    requestTerrainRender();
  });
}

function bindMapPicking(T, canvas, camera, brazil) {
  const raycaster = new T.Raycaster();
  const pointer = new T.Vector2();
  const activePointers = new Set();
  let start;
  const hitAt = event => {
    const rect = canvas.getBoundingClientRect();
    pointer.set((event.clientX - rect.left) / rect.width * 2 - 1, -(event.clientY - rect.top) / rect.height * 2 + 1);
    raycaster.setFromCamera(pointer, terrainRuntime?.camera || camera);
    const local = terrainRuntime?.municipalDetail?.children.filter(mesh => mesh.userData.municipality) || [];
    const targets = landcoverActive() && !brazil.model.visible ? local : [...local, ...brazil.pickable];
    return raycaster.intersectObjects(targets, false)[0];
  };
  const municipalHit = hit => hit?.object.userData.municipality ? municipalRecord(hit.object.userData.municipality) : hit ? municipalAtPoint(hit.point) : null;
  canvas.addEventListener('pointerdown', event => {
    activePointers.add(event.pointerId);
    start = activePointers.size === 1 ? { x: event.clientX, y: event.clientY, id: event.pointerId } : undefined;
  });
  canvas.addEventListener('pointermove', event => {
    if (start && Math.hypot(event.clientX - start.x, event.clientY - start.y) > 6) start = undefined;
    const tooltip = explorerElement('explorerTooltip');
    if (event.buttons || event.pointerType !== 'mouse') { tooltip.hidden = true; return; }
    const hit = hitAt(event);
    const item = landcoverActive() ? municipalHit(hit) : null;
    const uf = hit?.object.userData.uf;
    const valid = landcoverActive() ? !!item : !!uf;
    tooltip.hidden = !valid;
    canvas.style.cursor = valid ? 'pointer' : 'grab';
    if (!valid) return;
    if (landcoverActive()) {
      const values = landcoverValues(item.code);
      tooltip.textContent = item.name + ' · ' + item.uf + '\n' + atlasLabels[atlasState.category] + ': ' + (values ? atlasPercentage(values.groups[atlasState.category], values.total) : atlasLabels.noData);
    } else {
      const value = productionFor(uf)[0];
      tooltip.textContent = explorerData.states[uf].name + '\n' + (value === null ? (document.documentElement.lang.startsWith('en') ? 'No published value' : labels.noShortValue) : formatNumber(value) + ' t');
    }
    const rect = terrainStage.getBoundingClientRect();
    const x = Math.max(8, Math.min(rect.width - tooltip.offsetWidth - 8, event.clientX - rect.left + 13));
    const y = Math.max(8, Math.min(rect.height - tooltip.offsetHeight - 8, event.clientY - rect.top + 13));
    tooltip.style.transform = 'translate(' + x + 'px,' + y + 'px)';
  });
  canvas.addEventListener('pointerup', event => {
    activePointers.delete(event.pointerId);
    if (start?.id === event.pointerId && Math.hypot(event.clientX - start.x, event.clientY - start.y) <= 6) {
      const hit = hitAt(event);
      if (landcoverActive()) { const item = municipalHit(hit); if (item) selectMunicipality(item.code); }
      else if (hit?.object.userData.uf) { explorerState.uf = hit.object.userData.uf; updateExplorer(); }
    }
    start = undefined;
  });
  canvas.addEventListener('pointercancel', event => { activePointers.delete(event.pointerId); start = undefined; });
  canvas.addEventListener('pointerleave', () => { explorerElement('explorerTooltip').hidden = true; });
}

function createStudioEnvironment(T, renderer) {
  const studio = new T.Scene();
  studio.background = new T.Color('#09090c');
  const panels = [
    { position: [-6, 6, 5], size: [4, 8], color: '#f5f6f0', intensity: 2.5 },
    { position: [7, 3, 2], size: [2, 6], color: '#e1ecec', intensity: 1.6 },
    { position: [0, 7, -7], size: [6, 3], color: '#e8edf0', intensity: 1.4 }
  ];
  panels.forEach(({ position, size, color, intensity }) => {
    const surface = new T.MeshBasicMaterial({ color: new T.Color(color).multiplyScalar(intensity), side: T.DoubleSide });
    const panel = new T.Mesh(new T.PlaneGeometry(...size), surface);
    panel.position.set(...position);
    panel.lookAt(0, 0, 0);
    studio.add(panel);
  });
  const generator = new T.PMREMGenerator(renderer);
  const environment = generator.fromScene(studio, .06);
  generator.dispose();
  disposeModel(studio);
  return environment;
}

function lightTerrain(T, scene) {
  const palette = terrainPalette();
  const ambient = new T.HemisphereLight(palette.ambient, '#2e3a32', 1.3);
  scene.add(ambient);
  const key = new T.DirectionalLight('#fff8ee', 2.1);
  key.position.set(-7, 16, 7);
  key.castShadow = true;
  key.shadow.mapSize.set(2048, 2048);
  Object.assign(key.shadow.camera, { left: -13, right: 13, top: 13, bottom: -13 });
  key.shadow.normalBias = .025;
  key.shadow.bias = -.00015;
  scene.add(key);
  const rim = new T.DirectionalLight(palette.rim, .6);
  rim.position.set(5, 7, -8);
  scene.add(rim);
  const fill = new T.DirectionalLight(palette.fill, .5);
  fill.position.set(8, 5, 10);
  scene.add(fill);
  return { ambient, rim, fill };
}

function createTerrain(T, OrbitControls, surfaceImage, lines) {
  const renderer = new T.WebGLRenderer({ antialias: true, alpha: true, powerPreference: 'low-power' });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 1.75));
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = T.PCFSoftShadowMap;
  renderer.outputColorSpace = T.SRGBColorSpace;
  renderer.toneMapping = T.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.1;
  const surfaceTexture = new T.Texture(surfaceImage);
  surfaceTexture.colorSpace = T.SRGBColorSpace;
  surfaceTexture.anisotropy = Math.min(8, renderer.capabilities.getMaxAnisotropy());
  surfaceTexture.needsUpdate = true;
  const mapBlend = { previous: { value: surfaceTexture }, amount: { value: 1 } };
  const scene = new T.Scene();
  const environment = createStudioEnvironment(T, renderer);
  scene.environment = environment.texture;
  const camera = new T.PerspectiveCamera(34, 1, .1, 110);
  const topCamera = new T.OrthographicCamera(-9, 9, 9, -9, .1, 110);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = !terrainMotion.matches;
  controls.dampingFactor = .09;
  controls.enablePan = false;
  controls.minDistance = 13;
  controls.maxDistance = 45;
  controls.minZoom = .6;
  controls.maxZoom = 2.6;
  controls.minPolarAngle = .015;
  controls.maxPolarAngle = Math.PI * .53;
  controls.rotateSpeed = .55;
  controls.zoomSpeed = .75;
  controls.autoRotateSpeed = .35;
  const lights = lightTerrain(T, scene);
  const brazil = buildBrazil(T, surfaceTexture, lines, mapBlend);
  scene.add(brazil.model);
  const shadow = new T.Mesh(new T.PlaneGeometry(60, 60), new T.ShadowMaterial({ opacity: .22 }));
  shadow.rotation.x = -Math.PI / 2;
  shadow.position.y = -.8;
  shadow.receiveShadow = true;
  scene.add(shadow);
  const canvas = renderer.domElement;
  canvas.tabIndex = 0;
  canvas.setAttribute('role', 'img');
  canvas.setAttribute('aria-label', labels.canvas);
  canvas.setAttribute('aria-describedby', 'terrainHint terrainKeyboardHelp');
  canvas.addEventListener('pointerdown', () => canvas.classList.add('pointer-focus'), { capture: true });
  canvas.addEventListener('keydown', () => canvas.classList.remove('pointer-focus'));
  canvas.addEventListener('blur', () => canvas.classList.remove('pointer-focus'));
  canvas.addEventListener('wheel', event => { if (document.activeElement !== canvas) event.stopImmediatePropagation(); }, { capture: true, passive: true });
  terrainStage.appendChild(canvas);
  controls.addEventListener('change', requestTerrainRender);
  controls.addEventListener('start', () => {
    if (landcoverActive()) { atlasCameraTween = undefined; setAtlasPlaying(false); }
    canvas.focus({ preventScroll: true });
    if (terrainRuntime?.controls.autoRotate) setTerrainRotation(false);
  });
  bindMapPicking(T, canvas, camera, brazil);
  let previousSize = '';
  const resize = new ResizeObserver(() => {
    const width = terrainStage.clientWidth;
    const height = terrainStage.clientHeight;
    if (!width || !height) return;
    renderer.setSize(width, height, false);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();
    const span = Math.max(8.5, 8.1 * height / width);
    topCamera.left = -span * width / height;
    topCamera.right = span * width / height;
    topCamera.top = span;
    topCamera.bottom = -span;
    topCamera.updateProjectionMatrix();
    const size = width + ':' + height;
    if (size !== previousSize) { previousSize = size; if (landcoverActive()) focusMunicipality(false); else resetTerrain(); }
    requestTerrainRender();
  });
  resize.observe(terrainStage);
  canvas.addEventListener('webglcontextlost', event => {
    event.preventDefault();
    showTerrainError(labels.interrupted);
  });
  return { T, renderer, scene, camera, perspectiveCamera: camera, topCamera, controls, brazil, shadow, environment, surfaceTexture, mapTexture: surfaceTexture, mapBlend, lines, lights, resize };
}

function updateMapAnimation(delta) {
  let moving = advanceTerrainBlend(delta);
  if (advanceAtlasCamera(delta)) moving = true;
  if (advanceMunicipalDetailBlend(delta)) moving = true;
  const amount = terrainMotion.matches ? 1 : Math.min(1, delta * 7);
  for (const state of terrainRuntime.brazil.states.values()) {
    if (Math.abs(state.group.scale.y - state.targetHeight) > .001) {
      state.group.scale.y += (state.targetHeight - state.group.scale.y) * amount;
      moving = true;
    } else state.group.scale.y = state.targetHeight;
  }
  return moving;
}

function positionSelectedLabel() {
  if (landcoverActive()) { positionMunicipalLabel(); return; }
  const label = explorerElement('explorerSelectedLabel');
  if (explorerState.uf === 'BR') { label.hidden = true; return; }
  const state = terrainRuntime.brazil.states.get(explorerState.uf);
  const point = state.center.clone();
  point.y = state.group.scale.y * 1.026 + .25;
  point.project(terrainRuntime.camera);
  label.hidden = Math.abs(point.x) > .96 || Math.abs(point.y) > .92 || point.z > 1;
  label.textContent = explorerState.uf;
  label.style.transform = 'translate(' + ((point.x + 1) * terrainStage.clientWidth / 2 - 15) + 'px,' + ((1 - point.y) * terrainStage.clientHeight / 2 - 12) + 'px)';
}

function requestTerrainRender() {
  if (!terrainRuntime || !terrainVisible || document.hidden || terrainFrame || terrainStage.dataset.state === 'error') return;
  terrainFrame = requestAnimationFrame(time => {
    terrainFrame = 0;
    if (!terrainVisible || document.hidden || !terrainRuntime) return;
    synchronizeTerrainMotion();
    const delta = Math.min(.05, Math.max(.005, (time - terrainLastTime) / 1000));
    terrainLastTime = time;
    const controlsMoving = terrainRuntime.controls.update(delta);
    const mapMoving = updateMapAnimation(delta);
    terrainRuntime.renderer.render(terrainRuntime.scene, terrainRuntime.camera);
    positionSelectedLabel();
    if (controlsMoving || mapMoving || terrainRuntime.controls.autoRotate) requestTerrainRender();
  });
}

function disposeTerrain() {
  if (!terrainRuntime) return;
  setAtlasPlaying(false);
  removeMunicipalDetail();
  terrainRuntime.brazil.states.forEach(state => { state.atlasMaterial.dispose(); state.material.dispose(); });
  atlasState.request += 1;
  atlasState.pending = atlasState.applied = '';
  new Set([terrainRuntime.mapTexture, terrainRuntime.mapBlend.previous.value]).forEach(disposeAtlasTexture);
  cancelAnimationFrame(terrainFrame);
  terrainFrame = 0;
  terrainRuntime.resize.disconnect();
  terrainRuntime.controls.dispose();
  disposeModel(terrainRuntime.scene);
  terrainRuntime.environment.dispose();
  terrainRuntime.surfaceTexture.dispose();
  terrainRuntime.renderer.dispose();
  terrainRuntime.renderer.domElement.remove();
  terrainRuntime = undefined;
}

function showTerrainError(message) {
  setAtlasPlaying(false);
  explorerElement('atlasMapStatus').hidden = true;
  terrainStage.dataset.state = 'error';
  terrainLoading.hidden = false;
  terrainLoadingText.textContent = message;
  terrainRetry.hidden = false;
  terrainStage.querySelector('canvas')?.setAttribute('hidden', '');
  explorerElement('explorerSelectedLabel').hidden = true;
  explorerElement('explorerTooltip').hidden = true;
  document.querySelectorAll('[data-terrain-action]').forEach(button => { button.disabled = true; });
  if (terrainRuntime) setTerrainRotation(false);
}

async function loadTerrainImage() {
  const image = new Image();
  image.src = naturalMapData.image;
  await image.decode();
  return image;
}

async function loadTerrain() {
  if (terrainPending || ['ready', 'error'].includes(terrainStage.dataset.state)) return;
  terrainPending = true;
  terrainStage.dataset.state = 'loading';
  terrainLoadingText.textContent = labels.preparing;
  terrainRetry.hidden = true;
  let timeout;
  try {
    const modules = await Promise.race([
      Promise.all([
        import('three'),
        import('three/addons/controls/OrbitControls.js'),
        loadTerrainImage(),
        import('three/addons/lines/LineSegments2.js'),
        import('three/addons/lines/LineSegmentsGeometry.js'),
        import('three/addons/lines/LineMaterial.js')
      ]),
      new Promise((_, reject) => { timeout = setTimeout(() => reject(new Error('loading-timeout')), 18000); })
    ]);
    const lines = { LineSegments2: modules[3].LineSegments2, LineSegmentsGeometry: modules[4].LineSegmentsGeometry, LineMaterial: modules[5].LineMaterial };
    terrainRuntime = createTerrain(modules[0], modules[1].OrbitControls, modules[2], lines);
    bindTerrainControls();
    resetTerrain();
    terrainLoading.hidden = true;
    terrainStage.dataset.state = 'ready';
    document.querySelectorAll('[data-terrain-action]').forEach(button => { button.disabled = false; });
    updateExplorer();
  } catch {
    disposeTerrain();
    showTerrainError(labels.failed3D);
  } finally {
    clearTimeout(timeout);
    terrainPending = false;
  }
}

let atlasData;
let atlasMetadataPromise;
const detailAssetsBase = explorerElement('terrainViewer').dataset.assetsBase || '';
const atlasEnglish = english;
const atlasLocale = atlasEnglish ? 'en-GB' : 'pt-BR';
let atlasClasses;
const atlasState = { mode: 'production', year: 2025, category: 'agriculture', municipality: 'BR', detail: false, playing: false, request: 0, applied: '', pending: '' };
const atlasLabels = atlasEnglish ? {
  native: 'Native vegetation', agriculture: 'Agriculture', pasture: 'Pasture', forestry: 'Planted forests', water: 'Water', other: 'Other land cover',
  brazil: 'Brazil', municipality: 'Municipality', panorama: 'Municipal overview', loading: 'Preparing the municipalities…', detailLoading: 'Loading local land cover…', error: 'The map could not load. Try again.', detailError: 'Local land cover could not load. The municipal proportions remain visible.',
  noData: 'No published data', collection: 'Collection', source: 'View MapBiomas source ↗', conab: 'View Conab source ↗',
  share: 'of mapped area', national: 'of municipalities with data', since: 'since 2018', baseline: '2018 · baseline', unchanged: 'No change since 2018',
  play: 'Play years', pause: 'Pause years', replay: 'Replay from 2018', scale: 'Share of mapped area', local: 'Local land cover',
  hint: 'Select a municipality · Use the search for smaller places', localHint: 'Drag to explore · Scroll to zoom', empty: 'No municipalities found',
  canvas: 'Map of Brazil by municipality. Colour intensity represents the selected land cover share on a fixed 0 to 100 percent scale. Select a municipality or use the search to inspect local land cover.',
  credit: 'MapBiomas · Collection 11 · IBGE 2024 boundaries', note: 'Select a land cover group. The colour scale stays fixed across all years.', localNote: 'Geographic preview of the selected land cover. Areas use the published municipal statistics.',
} : {
  native: 'Vegetação nativa', agriculture: 'Agricultura', pasture: 'Pastagem', forestry: 'Florestas plantadas', water: 'Água', other: 'Outras coberturas',
  brazil: labels.brazil, municipality: 'Município', panorama: 'Panorama municipal', loading: 'Preparando os municípios…', detailLoading: 'Carregando a cobertura local…', error: 'O mapa não carregou. Tente novamente.', detailError: 'A cobertura local não carregou. As proporções municipais continuam visíveis.',
  noData: 'Sem dados publicados', collection: 'Coleção', source: 'Consultar MapBiomas ↗', conab: 'Consultar Conab ↗',
  share: 'da área mapeada', national: 'dos municípios com dados', since: 'desde 2018', baseline: '2018 · ponto de partida', unchanged: 'Sem variação desde 2018',
  play: 'Reproduzir anos', pause: 'Pausar anos', replay: 'Reproduzir desde 2018', scale: 'Percentual da área mapeada', local: 'Cobertura local',
  hint: 'Selecione um município · Use a busca para locais menores', localHint: 'Arraste para explorar · Role para aproximar', empty: 'Nenhum município encontrado',
  canvas: 'Mapa do Brasil por município. A intensidade da cor representa o percentual da cobertura selecionada, em uma escala fixa de 0 a 100 por cento. Selecione um município ou use a busca para explorar sua cobertura local.',
  credit: 'MapBiomas · Coleção 11 · Malha IBGE 2024', note: 'Selecione uma cobertura. A escala de cores permanece fixa em todos os anos.', localNote: 'Recorte geográfico da cobertura selecionada. As áreas vêm das estatísticas municipais publicadas.',
};
let municipalData;
let municipalPromise;
let atlasTimer;
let atlasCameraTween;
let atlasSurfacePromise = Promise.resolve();
const atlasDetailPacks = new Map();
const atlasDetailImages = new Map();
const atlasDecodedRings = new Map();
const atlasSearchState = { results: [], active: -1 };

function landcoverActive() { return atlasState.mode === 'landcover'; }

async function fetchAsset(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(18000) });
  if (!response.ok) throw new Error('asset-http-' + response.status);
  return response;
}

async function loadAtlasMetadata() {
  if (atlasData) return atlasData;
  if (!atlasMetadataPromise) atlasMetadataPromise = fetchAsset('/assets/mapbiomas/atlas.json').then(response => response.json()).then(data => {
    if (!data.colors || !data.count || !data.records || !data.areas) throw new Error('atlas-metadata');
    atlasClasses = Object.keys(data.colors);
    atlasData = data;
    return data;
  }).catch(error => { atlasMetadataPromise = undefined; throw error; });
  return atlasMetadataPromise;
}

async function inflateAtlas(url) {
  const response = await fetchAsset(url);
  const buffer = await response.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  if (bytes[0] !== 31 || bytes[1] !== 139) return buffer;
  const stream = new Blob([buffer]).stream().pipeThrough(new DecompressionStream('gzip'));
  return new Response(stream).arrayBuffer();
}

async function loadMunicipalData() {
  if (municipalData) return municipalData;
  if (municipalPromise) return municipalPromise;
  municipalPromise = (async () => {
    await loadAtlasMetadata();
    const [records, rawAreas] = await Promise.all([inflateAtlas(atlasData.records), inflateAtlas(atlasData.areas)]);
    const items = JSON.parse(new TextDecoder().decode(records));
    const areas = new Float64Array(rawAreas);
    if (items.length !== atlasData.count || areas.length !== items.length * 48) throw new Error('municipal-data-shape');
    const image = new Image();
    image.src = atlasData.indexImage;
    await image.decode();
    const canvas = document.createElement('canvas');
    canvas.width = atlasData.width;
    canvas.height = atlasData.height;
    const context = canvas.getContext('2d', { willReadFrequently: true });
    context.drawImage(image, 0, 0);
    const rgba = context.getImageData(0, 0, canvas.width, canvas.height).data;
    const ids = new Uint16Array(canvas.width * canvas.height);
    for (let i = 0; i < ids.length; i++) ids[i] = rgba[i * 4] + rgba[i * 4 + 1] * 256;
    const totals = new Float64Array(48);
    items.forEach((item, index) => {
      item.index = index;
      item.search = normalizeMunicipalName(item.name + ' ' + item.uf + ' ' + item.code);
      if (item.available) for (let i = 0; i < 48; i++) totals[i] += areas[index * 48 + i];
    });
    canvas.width = canvas.height = 1;
    municipalData = { items, areas, ids, totals, byCode: new Map(items.map(item => [item.code, item])) };
    return municipalData;
  })().catch(error => { municipalPromise = undefined; throw error; });
  return municipalPromise;
}

function municipalRecord(code = atlasState.municipality) { return municipalData?.byCode.get(code); }

function landcoverValues(code = atlasState.municipality, year = atlasState.year) {
  if (!municipalData) return null;
  const item = municipalRecord(code);
  if (code !== 'BR' && !item?.available) return null;
  const start = (code === 'BR' ? 0 : item.index * 48) + (year - 2018) * 6;
  const data = code === 'BR' ? municipalData.totals : municipalData.areas;
  const groups = Object.fromEntries(atlasClasses.map((key, index) => [key, data[start + index]]));
  return { groups, total: Object.values(groups).reduce((sum, value) => sum + value, 0) };
}

function atlasArea(value) {
  if (value >= 1000000) return formatNumber(value / 1000000, 2) + (atlasEnglish ? 'M ha' : ' mi ha');
  if (value >= 1000) return formatNumber(value / 1000, 1) + (atlasEnglish ? 'k ha' : ' mil ha');
  return formatNumber(value, 1) + ' ha';
}

function atlasPercentage(area, total) {
  const share = total > 0 ? area / total * 100 : 0;
  if (share > 0 && share < .1) return atlasEnglish ? '< 0.1%' : labels.smallShare;
  return formatNumber(share, 1) + '%';
}

function configureExplorerMode() {
  const coverage = landcoverActive();
  const viewer = explorerElement('terrainViewer');
  const changed = viewer.dataset.topic !== atlasState.mode;
  viewer.dataset.topic = atlasState.mode;
  for (const id of ['productionToolbar', 'productionTerritory', 'explorerProductionPanel', 'explorerMapLegend']) explorerElement(id).hidden = coverage;
  for (const id of ['atlasToolbar', 'atlasTimeline', 'municipalTerritory', 'atlasPanel', 'atlasMapLegend']) explorerElement(id).hidden = !coverage;
  document.querySelectorAll('[data-explorer-mode]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.explorerMode === atlasState.mode)));
  document.querySelector('[data-terrain-action="rotate"]').hidden = coverage;
  if (coverage) explorerElement('explorerTimeline').hidden = true;
  else {
    explorerElement('atlasMapStatus').hidden = true;
    terrainStage.removeAttribute('data-atlas-state');
    terrainStage.removeAttribute('aria-busy');
  }
  if (!terrainRuntime) return;
  const canvas = terrainRuntime.renderer.domElement;
  canvas.dataset.productionLabel ||= canvas.getAttribute('aria-label');
  canvas.setAttribute('aria-label', coverage ? atlasLabels.canvas : canvas.dataset.productionLabel);
  terrainRuntime.brazil.states.forEach(state => state.group.children.forEach(mesh => { if (mesh.isMesh && Array.isArray(mesh.material)) mesh.material[0] = coverage ? state.atlasMaterial : state.material; }));
  if (coverage && !terrainRuntime.municipalControls) {
    const controls = terrainRuntime.controls;
    terrainRuntime.municipalControls = { minDistance: controls.minDistance, maxDistance: controls.maxDistance, minZoom: controls.minZoom, maxZoom: controls.maxZoom, enablePan: controls.enablePan };
    Object.assign(controls, { minDistance: .01, maxDistance: 100, minZoom: .6, maxZoom: 4096, enablePan: true });
    focusMunicipality(false);
  } else if (!coverage && terrainRuntime.municipalControls) {
    Object.assign(terrainRuntime.controls, terrainRuntime.municipalControls);
    terrainRuntime.municipalControls = undefined;
    atlasCameraTween = undefined;
    removeMunicipalDetail();
    terrainRuntime.brazil.model.visible = true;
    resetTerrain();
  }
  if (changed) setTerrainRotation(false);
}

function updateExplorer() {
  configureExplorerMode();
  if (landcoverActive()) {
    if (!atlasData) {
      showAtlasStatus(atlasLabels.loading);
      loadAtlasMetadata().then(() => { if (landcoverActive()) updateExplorer(); }).catch(() => {
        if (landcoverActive()) showAtlasStatus(atlasLabels.error, true);
      });
      return;
    }
    updateLandcoverExplorer();
  }
  else {
    setAtlasPlaying(false);
    atlasState.request += 1;
    atlasState.applied = '';
    atlasState.pending = '';
    const credit = explorerElement('explorerGeographyCredit');
    if (credit.dataset.productionMarkup) credit.innerHTML = credit.dataset.productionMarkup;
    explorerElement('explorerSourceLink').textContent = atlasLabels.conab;
    const fetched = explorerData.audit.verified_at.slice(0, 10);
    explorerElement('explorerFetchedDate').textContent = new Date(fetched + 'T12:00:00').toLocaleDateString(atlasLocale, { day: '2-digit', month: 'short', year: 'numeric' });
    updateProductionExplorer();
    if (terrainRuntime) transitionTerrainTexture(terrainRuntime.surfaceTexture);
  }
}

function updateLandcoverExplorer() {
  const item = municipalRecord();
  const local = !!item;
  const name = local ? item.name + ' · ' + item.uf : atlasLabels.brazil;
  explorerElement('atlasYear').value = atlasState.year;
  explorerElement('atlasYearValue').textContent = atlasState.year;
  explorerElement('atlasCategory').value = atlasState.category;
  explorerElement('municipalName').textContent = name;
  explorerElement('municipalName').hidden = local;
  explorerElement('municipalSearch').disabled = !municipalData;
  explorerElement('atlasBack').hidden = !local;
  explorerElement('atlasViewSwitch').hidden = !detailAssetsBase || !local || !item.available;
  document.querySelectorAll('[data-atlas-view]').forEach(button => button.setAttribute('aria-pressed', String((button.dataset.atlasView === 'detail') === atlasState.detail)));
  explorerElement('explorerStageTitle').textContent = local ? name : atlasLabels[atlasState.category] + ' · ' + atlasLabels.panorama;
  explorerElement('explorerPeriodNote').textContent = atlasState.year + ' · ' + (local && atlasState.detail ? atlasLabels.local : atlasLabels.scale);
  explorerElement('explorerStageBadge').textContent = local ? item.code : 'MAPBIOMAS';
  explorerElement('explorerSourceText').textContent = 'MapBiomas · ' + atlasLabels.collection + ' 11 · ' + atlasState.year;
  explorerElement('explorerSourceLink').href = atlasData.sourceUrl;
  explorerElement('explorerSourceLink').textContent = atlasLabels.source;
  explorerElement('terrainHint').textContent = local ? atlasLabels.localHint : atlasLabels.hint;
  const credit = explorerElement('explorerGeographyCredit');
  credit.dataset.productionMarkup ||= credit.innerHTML;
  const link = document.createElement('a');
  link.href = atlasData.sourceUrl;
  link.target = '_blank';
  link.rel = 'noopener noreferrer';
  link.textContent = atlasLabels.credit;
  link.title = atlasEnglish ? 'Same 2024 municipal boundaries for the entire series. Simplified geographic previews; areas from published municipal statistics. CC BY 4.0.' : 'Mesma malha municipal de 2024 em toda a série. Recortes geográficos simplificados; áreas das estatísticas municipais publicadas. CC BY 4.0.';
  credit.replaceChildren(link, document.createTextNode(' · CC BY 4.0'));
  explorerElement('explorerFetchedDate').textContent = new Date(atlasData.fetchedAt).toLocaleDateString(atlasLocale, { day: '2-digit', month: 'short', year: 'numeric' });
  explorerElement('explorerTooltip').hidden = true;
  updateLandcoverPanel();
  updateAtlasPlayButton();
  if (!explorerTimeTimer && !atlasState.playing) terrainStatus.textContent = name + ', ' + atlasState.year + '. ' + atlasLabels[atlasState.category] + ': ' + explorerElement('atlasValue').textContent + '.';
  if (terrainRuntime && terrainStage.dataset.state === 'ready') {
    updateMapTargets();
    requestAtlasSurface();
    requestTerrainRender();
  } else if (!municipalData) {
    showAtlasStatus(atlasLabels.loading);
    loadMunicipalData().then(() => {
      if (!landcoverActive()) return;
      explorerElement('atlasMapStatus').hidden = true;
      terrainStage.setAttribute('aria-busy', 'false');
      updateLandcoverExplorer();
    }).catch(() => { if (landcoverActive()) showAtlasStatus(atlasLabels.error, true); });
  }
}

function updateLandcoverPanel() {
  const values = landcoverValues();
  const baseline = landcoverValues(atlasState.municipality, 2018);
  const selected = atlasState.category;
  const area = values?.groups[selected];
  const share = values?.total ? area / values.total * 100 : null;
  const color = atlasData.colors[selected];
  explorerElement('atlasMetricLabel').textContent = atlasLabels[selected];
  explorerElement('atlasMetricSwatch').style.setProperty('--coverage-color', color);
  explorerElement('atlasValue').innerHTML = share === null ? '—' : formatNumber(share, 1) + '<small>%</small>';
  explorerElement('atlasExact').textContent = share === null ? (municipalData ? atlasLabels.noData : '—') : atlasArea(area) + ' · ' + (atlasState.municipality === 'BR' ? atlasLabels.national : atlasLabels.share);
  const delta = explorerElement('atlasDelta');
  if (!values || !baseline) delta.textContent = '';
  else if (atlasState.year === 2018) delta.textContent = atlasLabels.baseline;
  else {
    const change = area - baseline.groups[selected];
    const points = share - baseline.groups[selected] / baseline.total * 100;
    const sign = value => value > 0 ? '+' : value < 0 ? '−' : '';
    delta.textContent = sign(change) + atlasArea(Math.abs(change)) + ' · ' + sign(points) + formatNumber(Math.abs(points), 2) + ' p.p. ' + atlasLabels.since;
  }
  const summary = [];
  for (const group of atlasClasses) {
    const row = document.querySelector('[data-atlas-group="' + group + '"]');
    const amount = values?.groups[group];
    const percentage = values ? atlasPercentage(amount, values.total) : '—';
    row.setAttribute('aria-pressed', String(selected === group));
    row.querySelector('strong').textContent = percentage;
    row.querySelector('small').textContent = values ? atlasArea(amount) : '—';
    row.title = atlasLabels[group] + ': ' + (values ? formatNumber(amount, 1) + ' ha' : atlasLabels.noData);
    const bar = document.querySelector('[data-atlas-bar="' + group + '"]');
    bar.style.flexGrow = values ? amount : 0;
    bar.style.opacity = selected === group ? 1 : .38;
    summary.push(atlasLabels[group] + ': ' + percentage);
  }
  explorerElement('atlasComposition').setAttribute('aria-label', summary.join('; '));
  explorerElement('atlasPanelNote').textContent = atlasState.detail ? atlasLabels.localNote : atlasLabels.note;
  explorerElement('atlasScale').hidden = atlasState.detail;
  explorerElement('atlasLocalLegend').hidden = !atlasState.detail;
  explorerElement('atlasLocalLegend').textContent = atlasLabels[selected] + ' · ' + atlasLabels.local;
  explorerElement('atlasScaleGradient').style.background = 'linear-gradient(90deg, ' + atlasColor(0) + ', ' + atlasColor(.25) + ', ' + atlasColor(.5) + ', ' + atlasColor(.75) + ', ' + atlasColor(1) + ')';
}

function atlasColor(share) {
  const high = atlasData.colors[atlasState.category].slice(1).match(/../g).map(value => parseInt(value, 16));
  const low = [31, 38, 37];
  return 'rgb(' + low.map((value, i) => Math.round(value + (high[i] - value) * Math.pow(Math.max(0, Math.min(1, share)), .7))).join(',') + ')';
}

function paintMunicipalTexture() {
  const { T, renderer } = terrainRuntime;
  const palette = new Uint8ClampedArray((municipalData.items.length + 1) * 4);
  const high = atlasData.colors[atlasState.category].slice(1).match(/../g).map(value => parseInt(value, 16));
  palette.set([24, 29, 29, 255]);
  municipalData.items.forEach((item, index) => {
    const values = landcoverValues(item.code);
    const share = values?.total ? values.groups[atlasState.category] / values.total : null;
    const selected = atlasState.municipality === 'BR' || atlasState.municipality === item.code;
    for (let channel = 0; channel < 3; channel++) {
      const base = share === null ? [70, 70, 78][channel] : [31, 38, 37][channel] + (high[channel] - [31, 38, 37][channel]) * Math.pow(Math.max(0, Math.min(1, share)), .7);
      palette[(index + 1) * 4 + channel] = selected ? base : base * .45 + [15, 20, 20][channel] * .55;
    }
    palette[(index + 1) * 4 + 3] = 255;
  });
  const canvas = document.createElement('canvas');
  canvas.width = atlasData.width;
  canvas.height = atlasData.height;
  const context = canvas.getContext('2d');
  const data = context.createImageData(canvas.width, canvas.height);
  const source = new Uint32Array(palette.buffer);
  const destination = new Uint32Array(data.data.buffer);
  const { ids } = municipalData;
  for (let i = 0; i < ids.length; i++) {
    destination[i] = source[ids[i]];
    if (ids[i] && (ids[i] !== ids[i + 1] || ids[i] !== ids[i + canvas.width])) {
      data.data[i * 4] *= .57;
      data.data[i * 4 + 1] *= .57;
      data.data[i * 4 + 2] *= .57;
    }
  }
  context.putImageData(data, 0, 0);
  const texture = new T.CanvasTexture(canvas);
  texture.colorSpace = T.SRGBColorSpace;
  texture.anisotropy = Math.min(4, renderer.capabilities.getMaxAnisotropy());
  texture.userData.atlas = true;
  return texture;
}

function showAtlasStatus(message, error = false) {
  explorerElement('atlasMapStatus').hidden = false;
  explorerElement('atlasMapStatusText').textContent = message;
  explorerElement('atlasRetry').hidden = !error;
  terrainStage.setAttribute('aria-busy', String(!error));
  if (error) setAtlasPlaying(false);
}

function requestAtlasSurface() {
  const key = [atlasState.year, atlasState.category, atlasState.municipality, atlasState.detail].join(':');
  if (atlasState.pending === key || (atlasState.applied === key && !atlasState.pending)) return;
  const request = ++atlasState.request;
  atlasState.pending = key;
  const runtime = terrainRuntime;
  const state = { ...atlasState };
  if (!municipalData) { terrainStage.dataset.atlasState = 'loading'; showAtlasStatus(atlasLabels.loading); }
  atlasSurfacePromise = (async () => {
    if (request !== atlasState.request || !landcoverActive()) return;
    try {
      await loadMunicipalData();
      if (request !== atlasState.request || !landcoverActive() || runtime !== terrainRuntime) return;
      updateLandcoverPanel();
      explorerElement('municipalSearch').disabled = false;
      terrainRuntime.brazil.model.visible = state.municipality === 'BR';
      if (state.municipality === 'BR') transitionTerrainTexture(paintMunicipalTexture());
      const previous = terrainRuntime.municipalDetail;
      const sameDetail = previous?.userData.code === state.municipality && previous.userData.blend && state.detail;
      if (!sameDetail) removeMunicipalDetail();
      if (state.municipality !== 'BR') {
        if (!sameDetail) addMunicipalOutline(municipalRecord());
        if (state.detail && municipalRecord()?.available) {
          if (!atlasDetailImages.has(state.municipality)) showAtlasStatus(atlasLabels.detailLoading);
          const tile = await loadMunicipalDetail(municipalRecord());
          if (request !== atlasState.request || !landcoverActive() || runtime !== terrainRuntime) return;
          addMunicipalDetail(municipalRecord(), tile, state);
        }
      }
      atlasState.applied = key;
      atlasState.pending = '';
      terrainStage.dataset.atlasState = 'ready';
      terrainStage.dataset.atlasYear = state.year;
      terrainStage.dataset.atlasMunicipality = state.municipality;
      terrainStage.dataset.atlasCategory = state.category;
      terrainStage.setAttribute('aria-busy', 'false');
      explorerElement('atlasMapStatus').hidden = true;
      requestTerrainRender();
    } catch {
      if (request !== atlasState.request || !landcoverActive()) return;
      atlasState.pending = '';
      terrainStage.dataset.atlasState = municipalData ? 'detail-error' : 'error';
      showAtlasStatus(municipalData ? atlasLabels.detailError : atlasLabels.error, true);
      if (municipalData) { explorerElement('atlasScale').hidden = false; explorerElement('atlasLocalLegend').hidden = true; }
    }
  })();
}

function loadDetailPack(pack) {
  if (window.agroMunicipalDetails?.[pack]) return Promise.resolve(window.agroMunicipalDetails[pack]);
  if (atlasDetailPacks.has(pack)) return atlasDetailPacks.get(pack);
  const promise = new Promise((resolve, reject) => {
    const script = document.createElement('script');
    let timeout;
    const finish = error => {
      clearTimeout(timeout);
      script.remove();
      if (error) { atlasDetailPacks.delete(pack); reject(error); }
      else resolve(window.agroMunicipalDetails[pack]);
    };
    if (!detailAssetsBase) { reject(new Error('detail-not-configured')); return; }
    script.src = detailAssetsBase.replace(/\/$/, '') + '/' + pack + '.js';
    script.onload = () => finish(window.agroMunicipalDetails?.[pack] ? null : new Error('detail-payload'));
    script.onerror = () => finish(new Error('detail-download'));
    timeout = setTimeout(() => finish(new Error('detail-timeout')), 15000);
    document.head.appendChild(script);
  });
  atlasDetailPacks.set(pack, promise);
  return promise;
}

async function loadMunicipalDetail(item) {
  if (atlasDetailImages.has(item.code)) return atlasDetailImages.get(item.code);
  const data = await loadDetailPack(Math.floor(item.index / 40));
  const tile = data[item.code];
  if (!tile) throw new Error('detail-missing');
  const image = new Image();
  image.src = tile.image;
  await image.decode();
  const result = { ...tile, decoded: image };
  atlasDetailImages.set(item.code, result);
  while (atlasDetailImages.size > 3) atlasDetailImages.delete(atlasDetailImages.keys().next().value);
  return result;
}

function municipalityRings(item) {
  if (atlasDecodedRings.has(item.code)) return atlasDecodedRings.get(item.code);
  const polygons = item.geometry.map(polygon => polygon.map(ring => {
    const points = [];
    let x = 0;
    let y = 0;
    for (let i = 0; i < ring.length; i += 2) { x += ring[i]; y += ring[i + 1]; points.push([x / 10000, y / 10000]); }
    return points;
  }));
  atlasDecodedRings.set(item.code, polygons);
  return polygons;
}

function addMunicipalOutline(item) {
  const { T, scene, lines } = terrainRuntime;
  const material = new lines.LineMaterial({ color: '#f6dda5', linewidth: 2, transparent: true, opacity: .95, depthWrite: false, toneMapped: false });
  const outline = buildStateOutline(lines, municipalityRings(item), material);
  outline.scale.y = .245;
  outline.renderOrder = 4;
  const group = new T.Group();
  group.userData.code = item.code;
  group.add(outline);
  const values = landcoverValues(item.code);
  const fill = new T.MeshBasicMaterial({ color: values ? atlasColor(values.groups[atlasState.category] / values.total) : '#46464e', toneMapped: false });
  for (const geometry of municipalGeometries(item)) {
    const mesh = new T.Mesh(geometry, fill);
    mesh.position.y = .236;
    mesh.userData.municipality = item.code;
    mesh.renderOrder = 3;
    group.add(mesh);
  }
  scene.add(group);
  terrainRuntime.municipalDetail = group;
}

function municipalGeometries(item) {
  const { T } = terrainRuntime;
  return municipalityRings(item).map(rings => {
    const shape = new T.Shape(rings[0].map(point => new T.Vector2(...projectCoordinate(point))));
    rings.slice(1).forEach(ring => shape.holes.push(new T.Path(ring.map(point => new T.Vector2(...projectCoordinate(point))))));
    const geometry = new T.ShapeGeometry(shape);
    geometry.rotateX(-Math.PI / 2);
    return geometry;
  });
}

function addMunicipalDetail(item, tile, state) {
  const { T, municipalDetail } = terrainRuntime;
  const canvas = document.createElement('canvas');
  canvas.width = tile.width;
  canvas.height = tile.height;
  const context = canvas.getContext('2d', { willReadFrequently: true });
  context.drawImage(tile.decoded, (state.year - 2018) * tile.width, 0, tile.width, tile.height, 0, 0, tile.width, tile.height);
  const image = context.getImageData(0, 0, canvas.width, canvas.height);
  const palette = atlasClasses.map(group => atlasData.colors[group].slice(1).match(/../g).map(value => parseInt(value, 16)));
  const selected = atlasClasses.indexOf(state.category);
  for (let i = 0; i < image.data.length; i += 4) {
    const group = image.data[i];
    for (let channel = 0; channel < 3; channel++) image.data[i + channel] = group === 255 ? [24, 29, 29][channel] : group === selected ? palette[group][channel] : palette[group][channel] * .2 + [19, 29, 25][channel] * .8;
    image.data[i + 3] = 255;
  }
  context.putImageData(image, 0, 0);
  const texture = new T.CanvasTexture(canvas);
  texture.colorSpace = T.SRGBColorSpace;
  texture.magFilter = T.LinearFilter;
  texture.userData.atlas = true;
  const meshes = municipalDetail.children.filter(object => object.userData.municipality === item.code);
  const material = meshes[0].material;
  const previous = material.map;
  const blend = municipalDetail.userData.blend || { previous: { value: texture }, amount: { value: 1 } };
  if (blend.previous.value !== previous && blend.previous.value !== texture) disposeAtlasTexture(blend.previous.value);
  blend.previous.value = previous || texture;
  blend.amount.value = previous && !terrainMotion.matches ? 0 : 1;
  material.color.set('#ffffff');
  material.map = texture;
  if (!municipalDetail.userData.blend) { configureTerrainBlend(material, blend); material.needsUpdate = true; }
  municipalDetail.userData.blend = blend;
  if (blend.amount.value === 1 && previous) { disposeAtlasTexture(previous); blend.previous.value = texture; }
  const [west, south, east, north] = tile.bounds;
  for (const mesh of meshes) {
    const geometry = mesh.geometry;
    const positions = geometry.attributes.position;
    const uv = geometry.attributes.uv;
    for (let i = 0; i < positions.count; i++) uv.setXY(i, (positions.getX(i) / .3478 - 54 - west) / (east - west), (-positions.getZ(i) / .37 - 14 - south) / (north - south));
    uv.needsUpdate = true;
  }
}

function advanceMunicipalDetailBlend(delta) {
  const group = terrainRuntime?.municipalDetail;
  const blend = group?.userData.blend;
  if (!blend || blend.amount.value >= 1) return false;
  blend.amount.value = terrainMotion.matches ? 1 : Math.min(1, blend.amount.value + delta * 2.8);
  if (blend.amount.value === 1) {
    const texture = group.children.find(mesh => mesh.userData.municipality).material.map;
    if (blend.previous.value !== texture) disposeAtlasTexture(blend.previous.value);
    blend.previous.value = texture;
  }
  return blend.amount.value < 1;
}

function removeMunicipalDetail() {
  const group = terrainRuntime?.municipalDetail;
  if (!group) return;
  const textures = new Set();
  if (group.userData.blend) textures.add(group.userData.blend.previous.value);
  group.traverse(object => { if (object.material?.map) textures.add(object.material.map); });
  textures.forEach(disposeAtlasTexture);
  disposeModel(group);
  terrainRuntime.municipalDetail = undefined;
}

function municipalAtPoint(point) {
  if (!municipalData) return null;
  const [west, south, east, north] = atlasData.bounds;
  const x = Math.floor((point.x / .3478 - 54 - west) / (east - west) * atlasData.width);
  const y = Math.floor((north - (-point.z / .37 - 14)) / (north - south) * atlasData.height);
  if (x < 0 || y < 0 || x >= atlasData.width || y >= atlasData.height) return null;
  return municipalData.items[municipalData.ids[y * atlasData.width + x] - 1] || null;
}

function selectMunicipality(code) {
  if (code !== 'BR' && !municipalRecord(code)) return;
  setAtlasPlaying(false);
  atlasState.municipality = code;
  atlasState.detail = !!detailAssetsBase && code !== 'BR' && municipalRecord(code).available;
  explorerElement('municipalSearch').value = code === 'BR' ? '' : municipalRecord(code).name + ' · ' + municipalRecord(code).uf;
  closeMunicipalSearch();
  updateExplorer();
  focusMunicipality(true);
  revealMunicipalMap();
}

function focusMunicipality(animate = false) {
  if (!terrainRuntime || !landcoverActive()) return;
  const { T, topCamera, controls } = terrainRuntime;
  const item = municipalRecord();
  const bounds = item?.bounds || [-74, -34, -34, 6];
  const [west, south, east, north] = bounds;
  const [x, z] = projectCoordinate([(west + east) / 2, (south + north) / 2]);
  const target = new T.Vector3(x, .22, -z);
  const width = (east - west) * .3478;
  const height = (north - south) * .37;
  const zoom = Math.min(4096, Math.min((topCamera.right - topCamera.left) / width, (topCamera.top - topCamera.bottom) / height) * (item ? .74 : .88));
  const cameraChanged = terrainRuntime.camera !== topCamera;
  terrainRuntime.camera = topCamera;
  controls.object = topCamera;
  terrainRuntime.shadow.visible = false;
  terrainRuntime.brazil.pickable.forEach(mesh => { mesh.receiveShadow = false; });
  setTerrainRotation(false);
  const destination = target.clone().add(new T.Vector3(0, 32, .02));
  if (!animate || terrainMotion.matches || cameraChanged) {
    atlasCameraTween = undefined;
    controls.target.copy(target);
    topCamera.position.copy(destination);
    topCamera.zoom = zoom;
    topCamera.updateProjectionMatrix();
    controls.update();
  } else atlasCameraTween = { elapsed: 0, from: topCamera.position.clone(), targetFrom: controls.target.clone(), zoomFrom: topCamera.zoom, position: destination, target, zoom };
  requestTerrainRender();
}

function advanceAtlasCamera(delta) {
  if (!atlasCameraTween || !terrainRuntime || !landcoverActive()) return false;
  const tween = atlasCameraTween;
  tween.elapsed += delta;
  const progress = terrainMotion.matches ? 1 : Math.min(1, tween.elapsed / .65);
  const amount = progress * progress * (3 - 2 * progress);
  const { camera, controls } = terrainRuntime;
  camera.position.lerpVectors(tween.from, tween.position, amount);
  controls.target.lerpVectors(tween.targetFrom, tween.target, amount);
  camera.zoom = Math.exp(Math.log(tween.zoomFrom) + (Math.log(tween.zoom) - Math.log(tween.zoomFrom)) * amount);
  camera.updateProjectionMatrix();
  controls.update();
  if (progress === 1) atlasCameraTween = undefined;
  return progress < 1;
}

function positionMunicipalLabel() {
  const label = explorerElement('explorerSelectedLabel');
  const item = municipalRecord();
  if (!item || !terrainRuntime) { label.hidden = true; return; }
  const [x, y] = projectCoordinate(item.point);
  const point = new terrainRuntime.T.Vector3(x, .3, -y).project(terrainRuntime.camera);
  label.hidden = atlasState.detail || Math.abs(point.x) > .9 || Math.abs(point.y) > .9;
  label.textContent = item.name;
  label.style.transform = 'translate(' + (point.x + 1) * terrainStage.clientWidth / 2 + 'px,' + (1 - point.y) * terrainStage.clientHeight / 2 + 'px) translate(-50%, -50%)';
}

function handleMunicipalControl(action) {
  if (action === 'reset') { selectMunicipality('BR'); return true; }
  if (action === 'top') { focusMunicipality(false); return true; }
  return false;
}

function revealMunicipalMap() {
  if (window.matchMedia('(max-width: 800px)').matches && terrainStage.getBoundingClientRect().bottom < 80) terrainStage.scrollIntoView({ block: 'center', behavior: terrainMotion.matches ? 'instant' : 'smooth' });
}

function updateAtlasPlayButton() {
  const button = explorerElement('atlasPlay');
  button.setAttribute('aria-pressed', String(atlasState.playing));
  const label = atlasState.playing ? atlasLabels.pause : atlasState.year === 2025 ? atlasLabels.replay : atlasLabels.play;
  button.setAttribute('aria-label', label);
  button.title = label;
  button.querySelector('use').setAttribute('href', atlasState.playing ? '#i-pause' : '#i-play');
}

function setAtlasPlaying(playing) {
  const wasPlaying = atlasState.playing;
  clearTimeout(atlasTimer);
  atlasTimer = undefined;
  atlasState.playing = playing;
  updateAtlasPlayButton();
  terrainStatus.setAttribute('aria-live', explorerTimeTimer || playing ? 'off' : 'polite');
  if (!playing) {
    if (wasPlaying && landcoverActive() && atlasData) {
      const item = municipalRecord();
      const name = item ? item.name + ' · ' + item.uf : atlasLabels.brazil;
      terrainStatus.textContent = name + ', ' + atlasState.year + '. ' + atlasLabels[atlasState.category] + ': ' + explorerElement('atlasValue').textContent + '.';
    }
    return;
  }
  setTerrainRotation(false);
  if (atlasCameraTween) focusMunicipality(false);
  if (atlasState.year === 2025) { atlasState.year = 2018; updateExplorer(); }
  scheduleAtlasYear();
}

function scheduleAtlasYear() {
  clearTimeout(atlasTimer);
  atlasTimer = setTimeout(async () => {
    if (!atlasState.playing || !landcoverActive() || document.hidden) return;
    await atlasSurfacePromise;
    if (!atlasState.playing || !landcoverActive()) return;
    if (atlasState.year >= 2025) { setAtlasPlaying(false); return; }
    atlasState.year += 1;
    updateExplorer();
    scheduleAtlasYear();
  }, 2000);
}

function normalizeMunicipalName(value) { return value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim(); }

function closeMunicipalSearch() {
  explorerElement('municipalResults').hidden = true;
  explorerElement('municipalSearch').setAttribute('aria-expanded', 'false');
  explorerElement('municipalSearch').removeAttribute('aria-activedescendant');
  atlasSearchState.active = -1;
}

function renderMunicipalSearch() {
  const query = normalizeMunicipalName(explorerElement('municipalSearch').value);
  const terms = query.split(/\s+/).filter(Boolean);
  const results = !municipalData || !terms.length ? [] : municipalData.items.filter(item => terms.every(term => item.search.includes(term))).sort((a, b) => Number(b.search.startsWith(query)) - Number(a.search.startsWith(query)) || a.name.localeCompare(b.name, atlasLocale)).slice(0, 8);
  atlasSearchState.results = results;
  atlasSearchState.active = results.length ? 0 : -1;
  const list = explorerElement('municipalResults');
  list.replaceChildren(...results.map((item, index) => {
    const option = document.createElement('li');
    option.id = 'municipal-option-' + index;
    option.setAttribute('role', 'option');
    option.setAttribute('aria-selected', String(index === 0));
    option.textContent = item.name + ' · ' + item.uf;
    option.addEventListener('pointerdown', event => event.preventDefault());
    option.addEventListener('click', () => selectMunicipality(item.code));
    return option;
  }));
  if (!results.length && query) { const message = document.createElement('li'); message.textContent = atlasLabels.empty; message.setAttribute('role', 'presentation'); list.appendChild(message); }
  list.hidden = !query;
  const input = explorerElement('municipalSearch');
  input.setAttribute('aria-expanded', String(!!query));
  if (results.length) input.setAttribute('aria-activedescendant', 'municipal-option-0');
  else input.removeAttribute('aria-activedescendant');
}

function initializeAtlas() {
  document.querySelectorAll('[data-explorer-mode]').forEach(button => button.addEventListener('click', () => {
    if (atlasState.mode === button.dataset.explorerMode) return;
    setTimelinePlaying(false);
    setAtlasPlaying(false);
    atlasState.mode = button.dataset.explorerMode;
    updateExplorer();
  }));
  explorerElement('atlasYear').addEventListener('input', event => { setAtlasPlaying(false); atlasState.year = Number(event.target.value); updateExplorer(); });
  explorerElement('atlasPlay').addEventListener('click', () => setAtlasPlaying(!atlasState.playing));
  const category = value => { atlasState.category = value; updateExplorer(); };
  explorerElement('atlasCategory').addEventListener('change', event => category(event.target.value));
  document.querySelectorAll('[data-atlas-group]').forEach(button => button.addEventListener('click', () => { category(button.dataset.atlasGroup); revealMunicipalMap(); }));
  document.querySelectorAll('[data-atlas-view]').forEach(button => button.addEventListener('click', () => { atlasState.detail = !!detailAssetsBase && button.dataset.atlasView === 'detail'; updateExplorer(); }));
  explorerElement('atlasBack').addEventListener('click', () => selectMunicipality('BR'));
  explorerElement('atlasRetry').addEventListener('click', () => { atlasState.applied = ''; updateExplorer(); });
  const search = explorerElement('municipalSearch');
  search.addEventListener('input', renderMunicipalSearch);
  search.addEventListener('focus', () => search.select());
  search.addEventListener('blur', closeMunicipalSearch);
  search.addEventListener('keydown', event => {
    if (event.key === 'Escape') { closeMunicipalSearch(); return; }
    if (event.key === 'Enter' && atlasSearchState.active >= 0 && !explorerElement('municipalResults').hidden) { event.preventDefault(); selectMunicipality(atlasSearchState.results[atlasSearchState.active].code); return; }
    if (!['ArrowDown', 'ArrowUp'].includes(event.key) || !atlasSearchState.results.length) return;
    event.preventDefault();
    const direction = event.key === 'ArrowDown' ? 1 : -1;
    atlasSearchState.active = (atlasSearchState.active + direction + atlasSearchState.results.length) % atlasSearchState.results.length;
    explorerElement('municipalResults').hidden = false;
    search.setAttribute('aria-expanded', 'true');
    search.setAttribute('aria-activedescendant', 'municipal-option-' + atlasSearchState.active);
    [...explorerElement('municipalResults').children].forEach((option, index) => option.setAttribute('aria-selected', String(index === atlasSearchState.active)));
  });
  document.addEventListener('visibilitychange', () => { if (document.hidden) setAtlasPlaying(false); });
  new IntersectionObserver(entries => { if (!entries[0].isIntersecting) setAtlasPlaying(false); }).observe(explorerElement('terrainViewer'));
}

function configureTerrainBlend(material, blend) {
  material.onBeforeCompile = shader => {
    shader.uniforms.terrainPreviousMap = blend.previous;
    shader.uniforms.terrainMapBlend = blend.amount;
    shader.fragmentShader = 'uniform sampler2D terrainPreviousMap;\nuniform float terrainMapBlend;\n' + shader.fragmentShader;
    shader.fragmentShader = shader.fragmentShader.replace('#include <map_fragment>', '#ifdef USE_MAP\nvec4 sampledDiffuseColor = mix(texture2D(terrainPreviousMap, vMapUv), texture2D(map, vMapUv), smoothstep(0.0, 1.0, terrainMapBlend));\ndiffuseColor *= sampledDiffuseColor;\n#endif');
  };
  material.customProgramCacheKey = () => 'agrobr-atlas-blend-v1';
}

function disposeAtlasTexture(texture) {
  if (!texture?.userData.atlas) return;
  texture.dispose();
  texture.image.width = texture.image.height = 1;
}

function transitionTerrainTexture(texture) {
  const runtime = terrainRuntime;
  if (!runtime || runtime.mapTexture === texture) return;
  const previous = runtime.mapBlend.previous.value;
  if (previous !== runtime.mapTexture) disposeAtlasTexture(previous);
  runtime.mapBlend.previous.value = runtime.mapTexture;
  runtime.mapBlend.amount.value = terrainMotion.matches ? 1 : 0;
  runtime.mapTexture = texture;
  runtime.brazil.states.forEach(state => { state.material.map = texture; state.atlasMaterial.map = texture; });
  if (terrainMotion.matches) finishTerrainBlend();
  requestTerrainRender();
}

function finishTerrainBlend() {
  const runtime = terrainRuntime;
  if (!runtime) return;
  if (runtime.mapBlend.previous.value !== runtime.mapTexture) disposeAtlasTexture(runtime.mapBlend.previous.value);
  runtime.mapBlend.previous.value = runtime.mapTexture;
  runtime.mapBlend.amount.value = 1;
}

function advanceTerrainBlend(delta) {
  const blend = terrainRuntime.mapBlend.amount;
  if (blend.value >= 1) return false;
  blend.value = terrainMotion.matches ? 1 : Math.min(1, blend.value + delta * 2.8);
  if (blend.value >= 1) finishTerrainBlend();
  return blend.value < 1;
}


async function startExplorer() {
  try {
    if (!explorerData) {
      explorerData = await (await fetchAsset('/assets/explorer/conab.json')).json();
      if (!explorerData.production || !explorerData.states || !explorerData.geometry) throw new Error('explorer-data');
    }
    if (!explorerInitialized) {
      explorerMaximums = Object.fromEntries(Object.keys(cropNames).map(crop => [crop, Math.max(1, ...Object.values({ ...explorerData.production.history, ...explorerData.production.current }).flatMap(period => Object.entries(period[crop] || {}).filter(([uf]) => uf !== 'BR').map(([, values]) => values[0] || 0)))]));
      initializeExplorer();
      initializeAtlas();
      explorerInitialized = true;
    }
    await loadTerrain();
  } catch {
    explorerData = undefined;
    showTerrainError(english ? 'The data could not load. Check your connection and try again.' : 'Os dados não carregaram. Confira a conexão e tente novamente.');
  }
}
const terrainLoadObserver = new IntersectionObserver(entries => {
  if (entries[0].isIntersecting) { startExplorer(); terrainLoadObserver.disconnect(); }
}, { rootMargin: '250px' });
terrainLoadObserver.observe(terrainStage);
const terrainObserver = new IntersectionObserver(entries => {
  terrainVisible = entries[0].isIntersecting;
  if (terrainVisible) requestTerrainRender();
  else {
    cancelAnimationFrame(terrainFrame);
    terrainFrame = 0;
  }
});
terrainObserver.observe(terrainStage);
const explorerVisibilityObserver = new IntersectionObserver(entries => {
  if (!entries[0].isIntersecting) setTimelinePlaying(false);
});
explorerVisibilityObserver.observe(explorerElement('terrainViewer'));
terrainRetry.addEventListener('click', () => {
  terrainStage.dataset.state = 'idle';
  terrainLoading.hidden = false;
  startExplorer();
});
document.addEventListener('visibilitychange', () => {
  if (document.hidden) setTimelinePlaying(false);
  else requestTerrainRender();
});
terrainMotion.addEventListener('change', synchronizeTerrainMotion);

  
