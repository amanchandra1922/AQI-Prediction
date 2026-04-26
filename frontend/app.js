/* ── IOC AQI Dashboard — app.js ── */
const API = 'http://127.0.0.1:8000';
const MONTH_LABELS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const AQI_CAT_COLORS = {'Good':'#55a868','Satisfactory':'#a8bb5a','Moderate':'#f5c518','Poor':'#f28e2b','Very Poor':'#d95f02','Severe':'#b22222','Unknown':'#888'};

let overviewChart = null, catDistChart = null, seasonalChart = null;
let zoneChart = null, yoyChart = null, zoneCatChart = null;
let forecastChart = null;
let overviewData = null, allZonesTs = null;
let currentZone = null, currentPollZone = null;
let zoneDays = 365, overviewDays = 90;
const pollCharts = {};

/* ── Clock ── */
function clock() {
  const el = document.getElementById('headerTime');
  if (el) el.textContent = new Date().toLocaleString('en-IN',{dateStyle:'medium',timeStyle:'short'});
}
clock(); setInterval(clock, 30000);

/* ── Fetch helper ── */
async function api(path) {
  const r = await fetch(API + path);
  if (!r.ok) throw new Error(r.statusText);
  return r.json();
}

/* ── Chart defaults ── */
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = 'rgba(99,102,241,.12)';
Chart.defaults.font.family = 'Inter, sans-serif';
Chart.defaults.plugins.tooltip.backgroundColor = '#1d2540';
Chart.defaults.plugins.tooltip.borderColor = 'rgba(99,102,241,.3)';
Chart.defaults.plugins.tooltip.borderWidth = 1;
Chart.defaults.plugins.tooltip.padding = 10;
Chart.defaults.plugins.tooltip.titleColor = '#f1f5f9';
Chart.defaults.plugins.tooltip.bodyColor = '#94a3b8';
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.pointStyleWidth = 10;

function lineOpts(label, color, data, labels) {
  return {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label, data,
        borderColor: color,
        backgroundColor: color + '18',
        fill: true,
        tension: 0.4,
        pointRadius: 0,
        borderWidth: 2,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: true,
      animation: { duration: 600 },
      scales: {
        x: { grid: { color: 'rgba(99,102,241,.08)' }, ticks: { maxTicksLimit: 8 } },
        y: { grid: { color: 'rgba(99,102,241,.08)' }, beginAtZero: false }
      },
      plugins: { legend: { display: false } }
    }
  };
}

/* ══ INIT ══ */
async function init() {
  try {
    overviewData = await api('/api/overview');
    renderSummaryCards(overviewData);
    renderZoneCards(overviewData.zones);
    buildZonePills(overviewData.zones);
    buildPollPills(overviewData.zones);
    updateAlertCount(overviewData.zones);

    const badge = document.getElementById('datasetInfo');
    if (badge) badge.textContent =
      `${overviewData.dataset.raw_station_rows.toLocaleString()} station-day rows · ${overviewData.dataset.zones_count} zones`;

    await loadOverviewCharts();
    hideLoader();
    if (overviewData.zones.length > 0) {
      currentZone = overviewData.zones[0].zone;
      currentPollZone = currentZone;
      activatePill('zonePills', currentZone);
      activatePill('pollPills', currentPollZone);
      await loadZoneDetail(currentZone);
      await loadPollutants(currentPollZone);
    }
    await loadAlerts();
    await loadForecasts(overviewData.zones);
  } catch(e) {
    console.error(e);
    hideLoader();
    alert('Cannot reach API at ' + API + '. Start the backend first:\n\ncd backend && uvicorn app:app --reload');
  }
}

/* ── Loader ── */
function hideLoader() {
  document.getElementById('loader').classList.add('hidden');
}

/* ── Summary cards ── */
function renderSummaryCards(d) {
  const ds = d.dataset;
  const cards = [
    { icon:'🗃️', label:'Station-Day Rows', value: ds.raw_station_rows.toLocaleString(), sub:'2015–2019 (excl. 2020)' },
    { icon:'🗺️', label:'Zones Monitored', value: ds.zones_count, sub:'All-India coverage' },
    { icon:'📡', label:'Unique Stations', value: ds.unique_stations, sub:'CPCB monitoring network' },
    { icon:'📅', label:'Date Range', value: ds.date_from.slice(0,4)+'–'+ds.date_to.slice(0,4), sub: ds.date_from + ' → ' + ds.date_to },
  ];
  const g = document.getElementById('summaryCards');
  g.innerHTML = cards.map(c => `
    <div class="summary-card">
      <div class="card-icon">${c.icon}</div>
      <div class="card-label">${c.label}</div>
      <div class="card-value" style="background:linear-gradient(135deg,#6366f1,#0ea5e9);-webkit-background-clip:text;-webkit-text-fill-color:transparent">${c.value}</div>
      <div class="card-sub">${c.sub}</div>
    </div>`).join('') +
    d.zones.map(z => `
    <div class="zone-card summary-card" style="--zone-color:${z.color}" onclick="switchToZone('${z.zone}')">
      <div class="card-label">${z.zone}</div>
      <div class="card-value" style="color:${z.color}">${z.mean_aqi}</div>
      <div class="card-sub">Mean AQI · Max ${z.max_aqi}</div>
    </div>`).join('');
}

/* ── Zone cards ── */
function renderZoneCards(zones) {}

/* ── Zone pills ── */
function buildZonePills(zones) {
  const c = document.getElementById('zonePills');
  c.innerHTML = zones.map(z => `
    <button class="zone-pill" style="--zone-color:${z.color}" id="zpill-${z.zone.replace(/\s/g,'-')}"
      onclick="selectZone('${z.zone}')">${z.zone}</button>`).join('');
}
function buildPollPills(zones) {
  const c = document.getElementById('pollPills');
  c.innerHTML = zones.map(z => `
    <button class="zone-pill" style="--zone-color:${z.color}" id="ppill-${z.zone.replace(/\s/g,'-')}"
      onclick="selectPollZone('${z.zone}')">${z.zone}</button>`).join('');
}

function activatePill(containerId, zone) {
  document.querySelectorAll(`#${containerId} .zone-pill`).forEach(p => {
    p.classList.remove('active');
    p.style.background = '';
    p.style.borderColor = '';
    p.style.color = '';
  });
  const key = zone.replace(/\s/g,'-');
  const pill = document.getElementById(containerId === 'zonePills' ? `zpill-${key}` : `ppill-${key}`);
  if (pill) {
    pill.classList.add('active');
    const col = pill.style.getPropertyValue('--zone-color');
    pill.style.background = col;
    pill.style.borderColor = col;
    pill.style.color = '#fff';
  }
}

async function selectZone(zone) {
  currentZone = zone;
  activatePill('zonePills', zone);
  await loadZoneDetail(zone);
}
async function selectPollZone(zone) {
  currentPollZone = zone;
  activatePill('pollPills', zone);
  await loadPollutants(zone);
}
function switchToZone(zone) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelector('[data-tab="zones"]').classList.add('active');
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('panel-zones').classList.add('active');
  selectZone(zone);
}

function updateAlertCount(zones) {
  const bad = zones.filter(z => ['Poor','Very Poor','Severe'].includes(z.category)).length;
  const el = document.getElementById('alertCount');
  if (el) { el.textContent = bad || '0'; el.style.background = bad > 0 ? '#ef4444' : '#10b981'; }
}

/* ══ OVERVIEW CHARTS ══ */
async function loadOverviewCharts() {
  allZonesTs = await api(`/api/allzones/timeseries?days=${overviewDays || 9999}`);
  renderAllZonesChart();
  await renderCatDistChart();
  await renderSeasonalityChart();
}

function renderAllZonesChart() {
  const ctx = document.getElementById('allZonesChart');
  if (!ctx || !allZonesTs) return;
  if (overviewChart) overviewChart.destroy();
  const datasets = Object.entries(allZonesTs).map(([zone, d]) => ({
    label: zone, data: d.dates.map((dt,i) => ({ x: dt, y: d.aqi[i] })),
    borderColor: d.color, backgroundColor: d.color+'14',
    fill: false, tension: 0.4, pointRadius: 0, borderWidth: 2,
  }));
  overviewChart = new Chart(ctx, {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true, maintainAspectRatio: true,
      animation: { duration: 500 },
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: { type: 'time', time: { unit: 'month' }, grid: { color: 'rgba(99,102,241,.08)' }, ticks: { maxTicksLimit: 12 } },
        y: { grid: { color: 'rgba(99,102,241,.08)' }, title: { display: true, text: 'AQI' } }
      },
      plugins: {
        legend: { position: 'bottom', labels: { font: { size: 11 } } },
        tooltip: { callbacks: {
          label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y?.toFixed(1)} AQI`
        }}
      }
    }
  });
}

window.setOverviewDays = async function(days, btn) {
  overviewDays = days;
  document.querySelectorAll('#panel-overview .ctrl-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  allZonesTs = await api(`/api/allzones/timeseries?days=${days || 9999}`);
  renderAllZonesChart();
};

async function renderCatDistChart() {
  const ctx = document.getElementById('catDistChart');
  if (!ctx || !overviewData) return;
  if (catDistChart) catDistChart.destroy();
  const cats = ['Good','Satisfactory','Moderate','Poor','Very Poor','Severe'];
  const zones = overviewData.zones.map(z => z.zone);
  const allData = await Promise.all(overviewData.zones.map(z => api(`/api/zone/${encodeURIComponent(z.zone)}/category_distribution`)));
  const datasets = cats.map((cat, ci) => ({
    label: cat,
    data: allData.map(d => d.percentages[ci]),
    backgroundColor: Object.values(AQI_CAT_COLORS)[ci],
  }));
  catDistChart = new Chart(ctx, {
    type: 'bar',
    data: { labels: zones, datasets },
    options: {
      responsive: true, maintainAspectRatio: true,
      animation: { duration: 600 },
      scales: {
        x: { stacked: true, grid: { color: 'rgba(99,102,241,.08)' } },
        y: { stacked: true, max: 100, grid: { color: 'rgba(99,102,241,.08)' }, title: { display: true, text: '% of Days' } }
      },
      plugins: {
        legend: { position: 'bottom', labels: { font: { size: 10 } } },
        tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y?.toFixed(1)}%` } }
      }
    }
  });
}

async function renderSeasonalityChart() {
  const ctx = document.getElementById('seasonalityChart');
  if (!ctx || !overviewData) return;
  if (seasonalChart) seasonalChart.destroy();
  const allMon = await Promise.all(overviewData.zones.map(z => api(`/api/zone/${encodeURIComponent(z.zone)}/monthly`)));
  const datasets = overviewData.zones.map((z, i) => ({
    label: z.zone,
    data: allMon[i].aqi,
    borderColor: z.color,
    backgroundColor: z.color + '18',
    fill: false, tension: 0.5, pointRadius: 3, borderWidth: 2,
  }));
  seasonalChart = new Chart(ctx, {
    type: 'line',
    data: { labels: MONTH_LABELS, datasets },
    options: {
      responsive: true, maintainAspectRatio: true,
      animation: { duration: 600 },
      scales: {
        x: { grid: { color: 'rgba(99,102,241,.08)' } },
        y: { grid: { color: 'rgba(99,102,241,.08)' }, title: { display: true, text: 'Mean AQI' } }
      },
      plugins: { legend: { position: 'bottom', labels: { font: { size: 10 } } } }
    }
  });
}

/* ══ ZONE DETAIL ══ */
async function loadZoneDetail(zone) {
  const zObj = overviewData?.zones.find(z2 => z2.zone === zone);
  const color = zObj?.color || '#6366f1';
  document.getElementById('zoneDetailTitle').textContent = `${zone} Zone — AQI Time Series`;

  const [ts, yoy, cat] = await Promise.all([
    api(`/api/zone/${encodeURIComponent(zone)}/timeseries?days=${zoneDays || 9999}`),
    api(`/api/zone/${encodeURIComponent(zone)}/yearly`),
    api(`/api/zone/${encodeURIComponent(zone)}/category_distribution`),
  ]);

  // Timeline
  const ctx1 = document.getElementById('zoneTimelineChart');
  if (zoneChart) zoneChart.destroy();
  zoneChart = new Chart(ctx1, {
    type: 'line',
    data: {
      datasets: [{
        label: `${zone} AQI`, data: ts.dates.map((d,i) => ({ x:d, y:ts.aqi[i] })),
        borderColor: color, backgroundColor: color+'18',
        fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: true,
      animation: { duration: 500 },
      scales: {
        x: { type:'time', time:{unit:'month'}, grid:{color:'rgba(99,102,241,.08)'}, ticks:{maxTicksLimit:10} },
        y: { grid:{color:'rgba(99,102,241,.08)'}, title:{display:true,text:'AQI'} }
      },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: c => `AQI: ${c.parsed.y?.toFixed(1)}` } }
      }
    }
  });

  // YoY
  const ctx2 = document.getElementById('yoyChart');
  if (yoyChart) yoyChart.destroy();
  yoyChart = new Chart(ctx2, {
    type: 'bar',
    data: {
      labels: yoy.years,
      datasets: [{ label:'Mean AQI', data: yoy.aqi, backgroundColor: color+'cc', borderColor: color, borderWidth: 1, borderRadius: 6 }]
    },
    options: {
      responsive: true, maintainAspectRatio: true,
      animation: { duration: 600 },
      scales: {
        x: { grid:{color:'rgba(99,102,241,.08)'} },
        y: { grid:{color:'rgba(99,102,241,.08)'}, title:{display:true,text:'Mean AQI'} }
      },
      plugins: { legend:{display:false} }
    }
  });

  // Category donut
  const ctx3 = document.getElementById('zoneCatChart');
  if (zoneCatChart) zoneCatChart.destroy();
  zoneCatChart = new Chart(ctx3, {
    type: 'doughnut',
    data: {
      labels: cat.categories,
      datasets: [{ data: cat.percentages, backgroundColor: cat.colors, borderWidth: 2, borderColor: '#111827' }]
    },
    options: {
      responsive: true, maintainAspectRatio: true,
      animation: { duration: 600 },
      cutout: '65%',
      plugins: {
        legend: { position:'right', labels:{font:{size:10}} },
        tooltip: { callbacks: { label: c => `${c.label}: ${c.parsed?.toFixed(1)}%` } }
      }
    }
  });
}

window.setZoneDays = async function(days, btn) {
  zoneDays = days;
  document.querySelectorAll('#panel-zones .ctrl-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  if (currentZone) await loadZoneDetail(currentZone);
};

/* ══ FORECASTS ══ */
async function loadForecasts(zones) {
  const preds = await Promise.all(zones.map(z => api(`/api/zone/${encodeURIComponent(z.zone)}/forecast?horizon=30`)));
  const latestAqis = {};
  zones.forEach(z => { latestAqis[z.zone] = z.latest_aqi; });

  const ctx = document.getElementById('forecastChart');
  if (forecastChart) forecastChart.destroy();
  forecastChart = new Chart(ctx, {
    type: 'line',
    data: {
      datasets: preds.map((p,i) => ({
        label: p.zone,
        data: p.dates.map((d,j) => ({ x:d, y:p.predicted[j] })),
        borderColor: zones[i].color,
        backgroundColor: zones[i].color + '12',
        fill: false, tension: 0.4, borderWidth: 2,
        borderDash: [6,3], pointRadius: 3, pointHoverRadius: 5,
      }))
    },
    options: {
      responsive: true, maintainAspectRatio: true,
      animation: { duration: 600 },
      interaction: { mode:'index', intersect:false },
      scales: {
        x: { type:'time', time:{unit:'day'}, grid:{color:'rgba(99,102,241,.08)'}, ticks:{maxTicksLimit:10} },
        y: { grid:{color:'rgba(99,102,241,.08)'}, title:{display:true,text:'Predicted AQI'} }
      },
      plugins: { legend:{position:'bottom',labels:{font:{size:11}}} }
    }
  });

  // Forecast cards
  const fc = document.getElementById('forecastCards');
  fc.innerHTML = preds.map((p,i) => {
    const avg30 = p.predicted.reduce((a,b)=>a+b,0)/p.predicted.length;
    const curr  = latestAqis[p.zone] || avg30;
    const delta = avg30 - curr;
    const dir   = delta > 3 ? '▲' : delta < -3 ? '▼' : '→';
    const cls   = delta > 3 ? 'up' : delta < -3 ? 'down' : 'flat';
    const cat   = aqiCategory(avg30);
    const catCol= AQI_CAT_COLORS[cat] || '#888';
    return `<div class="forecast-card" style="--zone-color:${zones[i].color}">
      <div class="fc-zone">${p.zone}</div>
      <div class="fc-aqi" style="color:${zones[i].color}">${avg30.toFixed(0)}</div>
      <div class="fc-delta ${cls}">${dir} ${Math.abs(delta).toFixed(1)} from current</div>
      <div class="fc-cat tag" style="background:${catCol}22;color:${catCol}">${cat}</div>
    </div>`;
  }).join('');
}

/* ══ ALERTS ══ */
async function loadAlerts() {
  const data = await api('/api/alerts');
  const g = document.getElementById('alertsGrid');
  g.innerHTML = data.alerts.map(a => `
    <div class="alert-card" style="--alert-color:${a.color}">
      <span class="alert-emoji">${a.emoji}</span>
      <div class="alert-zone">${a.zone} Zone</div>
      <div class="alert-aqi" style="color:${a.color}">${a.aqi ?? '—'}</div>
      <div class="alert-cat" style="color:${a.color}">${a.category}</div>
      <div class="alert-action">⚙️ ${a.action}</div>
      <div class="alert-date">Last reading: ${a.date}</div>
    </div>`).join('');
}

/* ══ POLLUTANTS ══ */
async function loadPollutants(zone) {
  const data = await api(`/api/pollutants/${encodeURIComponent(zone)}?days=180`);
  const polls = data.pollutants;
  const g = document.getElementById('pollutantsGrid');
  g.innerHTML = '';
  Object.entries(polls).forEach(([name, p]) => {
    const maxAll = p.max || 1;
    const pct = Math.min(100, (p.mean / maxAll) * 100);
    const card = document.createElement('div');
    card.className = 'poll-card';
    card.innerHTML = `
      <div class="poll-name">${name}</div>
      <div class="poll-mean">${p.mean.toFixed(1)}<span class="poll-unit">µg/m³</span></div>
      <div class="poll-max">Max: ${p.max.toFixed(1)}</div>
      <div class="poll-bar-wrap"><div class="poll-bar" style="width:${pct}%;background:var(--primary)"></div></div>
      <div class="poll-chart-wrap"><canvas id="pchart-${name.replace('.','')}" height="60"></canvas></div>`;
    g.appendChild(card);
    // Mini sparkline
    const ctx = card.querySelector(`#pchart-${name.replace('.','')}`);
    const vals = p.values.slice(-90);
    if (pollCharts[name]) pollCharts[name].destroy();
    pollCharts[name] = new Chart(ctx, {
      type: 'line',
      data: {
        labels: Array(vals.length).fill(''),
        datasets: [{ data:vals, borderColor:'#6366f1', backgroundColor:'#6366f120', fill:true, tension:0.4, pointRadius:0, borderWidth:1.5 }]
      },
      options: {
        responsive:true, maintainAspectRatio:false,
        animation:{duration:400},
        scales:{x:{display:false},y:{display:false}},
        plugins:{legend:{display:false},tooltip:{enabled:false}}
      }
    });
  });
}

/* ── AQI category helper (client-side) ── */
function aqiCategory(v) {
  if (v <= 50)  return 'Good';
  if (v <= 100) return 'Satisfactory';
  if (v <= 200) return 'Moderate';
  if (v <= 300) return 'Poor';
  if (v <= 400) return 'Very Poor';
  return 'Severe';
}

/* ── Tab switching ── */
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('panel-' + btn.dataset.tab).classList.add('active');
  });
});

/* ── Boot ── */
init();
