"""
HGEM MVP Dashboard — HTML builder

SUMMARY
-------
Takes the processed `summary` dict from pipeline.py and renders a single-file
HTML dashboard with embedded JSON. No server needed — just open the HTML in
any browser.
"""
from __future__ import annotations
import json
from datetime import datetime


HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Flight Club — MVP Recognition Dashboard</title>
<style>
  :root {
    --bg: #0a0e1a;
    --bg-2: #131826;
    --bg-3: #1c2238;
    --card: #161b2c;
    --border: #232940;
    --text: #e8ecf4;
    --muted: #8993a8;
    --accent: #ff3b3b;       /* Flight Club red (bullseye) */
    --accent-2: #ffb800;     /* gold */
    --accent-3: #00d4aa;     /* teal — used for Q1/Q2 pills */
    --silver: #c0c5d3;
    --bronze: #cd7f32;
    --pos: #1ad08a;
    --neu: #ffb800;
    --neg: #ff5454;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background:
      radial-gradient(1200px 600px at 20% -10%, rgba(255,59,59,0.06), transparent 60%),
      radial-gradient(1000px 500px at 100% 0%, rgba(0,212,170,0.05), transparent 50%),
      var(--bg);
    color: var(--text);
    font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen,
           Ubuntu, sans-serif;
    min-height: 100vh;
  }
  header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 22px 32px;
    border-bottom: 1px solid var(--border);
    background: rgba(10,14,26,0.6);
    backdrop-filter: blur(8px);
    position: sticky; top: 0; z-index: 10;
  }
  .brand { display: flex; align-items: center; gap: 14px; }
  .bullseye {
    width: 34px; height: 34px; flex-shrink: 0;
    background: radial-gradient(circle, var(--accent) 0 22%, #fff 22% 38%,
                                var(--accent) 38% 54%, #fff 54% 70%,
                                var(--accent) 70% 100%);
    border-radius: 50%;
    box-shadow: 0 0 18px rgba(255,59,59,0.4);
  }
  .brand h1 { margin: 0; font-size: 18px; font-weight: 700; letter-spacing: 0.5px; }
  .brand .sub { color: var(--muted); font-size: 12px; }
  .meta { color: var(--muted); font-size: 12px; text-align: right; }
  .meta strong { color: var(--text); }

  main { padding: 24px 32px 80px; max-width: 1500px; margin: 0 auto; }

  /* anomalies banner */
  .anomalies {
    background: linear-gradient(180deg, rgba(255,184,0,0.12), rgba(255,184,0,0.04));
    border: 1px solid rgba(255,184,0,0.3);
    border-radius: 10px;
    padding: 14px 18px;
    margin-bottom: 18px;
  }
  .anomalies h3 { margin: 0 0 6px; color: var(--accent-2); font-size: 13px;
                   text-transform: uppercase; letter-spacing: 1px; }
  .anomalies ul { margin: 0; padding-left: 18px; }
  .anomalies li { font-size: 13px; margin: 3px 0; }
  .anomalies .sev-high { color: #ff8a8a; }
  .anomalies .sev-medium { color: #ffd072; }
  .anomalies .sev-low { color: var(--muted); }

  /* filter row */
  .filters {
    display: flex; flex-wrap: wrap; gap: 18px; align-items: center;
    padding: 14px 0; margin-bottom: 18px;
    border-bottom: 1px solid var(--border);
  }
  .filter-group { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .filter-group .label {
    font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
    color: var(--muted); margin-right: 4px;
  }
  .pill {
    padding: 6px 12px;
    border: 1px solid var(--border);
    border-radius: 18px;
    background: var(--bg-2);
    color: var(--muted);
    cursor: pointer;
    font-size: 12px;
    font-weight: 500;
    transition: all 0.15s ease;
  }
  .pill:hover { color: var(--text); border-color: var(--bg-3); }
  .pill.active {
    background: var(--accent);
    color: white;
    border-color: var(--accent);
    box-shadow: 0 0 12px rgba(255,59,59,0.4);
  }
  .pill.quarter.active { background: var(--accent-3); border-color: var(--accent-3); color: #062; }
  .pill.quarter { border-color: rgba(0,212,170,0.3); color: rgba(0,212,170,0.85); }

  /* stat strip */
  .stat-strip { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px;
                margin-bottom: 18px; }
  .stat {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 14px 16px;
  }
  .stat .lbl { font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
                color: var(--muted); margin-bottom: 4px; }
  .stat .val { font-size: 26px; font-weight: 700; }
  .stat .sub { font-size: 11px; color: var(--muted); margin-top: 2px; }

  /* main grid */
  .grid { display: grid; grid-template-columns: 1fr 1.2fr; gap: 18px; }
  @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }

  .card {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px 18px;
  }
  .card h2 { margin: 0 0 12px; font-size: 14px; text-transform: uppercase;
              letter-spacing: 1px; color: var(--muted); font-weight: 600; }
  .card h2 .count { color: var(--text); }

  /* leaderboard */
  .lb-search {
    width: 100%; padding: 8px 12px; border-radius: 6px;
    background: var(--bg-2); border: 1px solid var(--border); color: var(--text);
    font-size: 13px; margin-bottom: 10px;
  }
  .lb-row {
    display: grid; grid-template-columns: 30px 1fr 60px; gap: 10px;
    align-items: center; padding: 7px 4px;
    border-radius: 6px; cursor: pointer;
    transition: background 0.1s ease;
  }
  .lb-row:hover { background: var(--bg-3); }
  .lb-row.selected { background: rgba(255,59,59,0.08); border-left: 2px solid var(--accent); padding-left: 6px; }
  .lb-row .rank {
    text-align: center; font-weight: 700; font-size: 13px;
    color: var(--muted);
  }
  .lb-row.r1 .rank { color: var(--accent-2); }
  .lb-row.r2 .rank { color: var(--silver); }
  .lb-row.r3 .rank { color: var(--bronze); }
  .lb-row .nm { font-weight: 500; }
  .lb-row .venue-tag {
    display: inline-block; margin-left: 6px;
    font-size: 10px; padding: 1px 6px;
    border-radius: 8px;
    background: var(--bg-3); color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.5px;
  }
  .lb-row .pts { text-align: right; font-weight: 700; font-variant-numeric: tabular-nums; }
  .lb-row .bar {
    grid-column: 2 / 3; height: 3px; background: var(--bg-3);
    border-radius: 2px; overflow: hidden; margin-top: 3px;
  }
  .lb-row .bar > div {
    height: 100%; background: linear-gradient(90deg, var(--accent), var(--accent-2));
    border-radius: 2px;
  }

  /* drill-down */
  .dd-header { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 6px; }
  .dd-name { font-size: 22px; font-weight: 700; }
  .dd-venue { color: var(--muted); font-size: 13px; }
  .dd-stats { display: flex; gap: 18px; margin: 8px 0 16px; }
  .dd-stats div { font-size: 13px; }
  .dd-stats .pos { color: var(--pos); font-weight: 700; }
  .dd-stats .neu { color: var(--neu); }
  .dd-stats .neg { color: var(--neg); }
  .dd-chart { margin: 12px 0 18px; }
  .dd-chart h3, .dd-quotes h3, .dd-venues h3 {
    font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
    color: var(--muted); margin: 14px 0 8px;
  }
  .dd-bars { display: flex; gap: 8px; align-items: flex-end; height: 100px; }
  .dd-bars .bar {
    flex: 1; background: var(--bg-3); border-radius: 4px 4px 0 0;
    position: relative; min-height: 4px;
    display: flex; align-items: flex-end; justify-content: center;
  }
  .dd-bars .bar > div {
    width: 100%; background: linear-gradient(180deg, var(--accent), var(--accent-2));
    border-radius: 4px 4px 0 0;
    min-height: 4px;
  }
  .dd-bars .bar .lbl {
    position: absolute; bottom: -22px; left: 50%; transform: translateX(-50%);
    font-size: 11px; color: var(--muted);
  }
  .dd-bars .bar .v {
    font-size: 11px; font-weight: 600; padding: 2px;
  }
  .dd-quotes { margin-top: 20px; }
  .dd-period-group { margin-bottom: 14px; }
  .dd-period-group .ptag {
    display: inline-block; font-size: 10px; font-weight: 700;
    padding: 2px 8px; border-radius: 8px;
    background: var(--bg-3); color: var(--accent-3);
    margin-bottom: 6px;
  }
  .dd-quote {
    border-left: 2px solid var(--accent);
    padding: 6px 10px; margin: 6px 0;
    background: var(--bg-2);
    border-radius: 0 6px 6px 0;
    font-size: 13px;
  }
  .dd-quote .meta { font-size: 11px; color: var(--muted); margin-top: 3px; }
  .dd-bare {
    font-size: 12px; color: var(--muted); font-style: italic;
    margin-top: 8px;
  }

  /* venue tally */
  .venues-card { margin-top: 18px; }
  .vt-row { display: grid; grid-template-columns: 1fr 60px; padding: 6px 0;
            border-bottom: 1px solid var(--border); font-size: 13px; }
  .vt-row:last-child { border-bottom: none; }
  .vt-row .v { text-align: right; font-weight: 700; }

  .placeholder { color: var(--muted); padding: 30px; text-align: center;
                  border: 1px dashed var(--border); border-radius: 8px; }

  footer { padding: 20px 32px; color: var(--muted); font-size: 11px;
            text-align: center; border-top: 1px solid var(--border); margin-top: 40px; }
</style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="bullseye"></div>
      <div>
        <h1>Flight Club — MVP Recognition Dashboard</h1>
        <div class="sub">Guest feedback × employee leaderboard</div>
      </div>
    </div>
    <div class="meta">
      Updated <strong id="meta-updated"></strong><br>
      <span id="meta-coverage"></span>
    </div>
  </header>

  <main>
    <div id="anomalies-banner"></div>

    <div class="filters">
      <div class="filter-group">
        <span class="label">Period</span>
        <div id="period-pills"></div>
      </div>
      <div class="filter-group">
        <span class="label">Venue</span>
        <div id="venue-pills"></div>
      </div>
    </div>

    <div class="stat-strip" id="stat-strip"></div>

    <div class="grid">
      <div class="card">
        <h2>Leaderboard <span class="count" id="lb-count"></span></h2>
        <input type="text" class="lb-search" id="lb-search" placeholder="Search names…">
        <div id="leaderboard"></div>
      </div>
      <div class="card" id="drill-down">
        <div class="placeholder">Select someone from the leaderboard to see their recognition feed.</div>
      </div>
    </div>

    <div class="card venues-card">
      <h2>Venue tally</h2>
      <div id="venue-tally"></div>
    </div>
  </main>

  <footer>
    Built for Flight Club Darts · Data dedupe by Visit Id · Sentiment scored on name-containing sentences only · No bots were paid in the making of this leaderboard.
  </footer>

<script>
const DATA = __DATA_JSON__;

// --- state ---
const state = {
  period: "Overall",
  venue: "All",
  search: "",
  selected: null,
};

// --- helpers ---
function periodInScope(period) {
  if (state.period === "Overall") return true;
  if (state.period === "Q1") return DATA.quarters.Q1.includes(period);
  if (state.period === "Q2") return DATA.quarters.Q2.includes(period);
  return period === state.period;
}
function venueInScope(venue) {
  return state.venue === "All" || venue === state.venue;
}

// --- compute filtered counts from raw mentions array ---
function filteredCounts() {
  const tally = {};
  let total = 0;
  for (const m of DATA.mentions) {
    if (m.s !== "p") continue;            // positives only
    if (!periodInScope(m.p)) continue;
    if (!venueInScope(m.b)) continue;
    tally[m.e] = (tally[m.e] || 0) + 1;
    total++;
  }
  return { tally, total };
}

// --- render ---
function renderHeader() {
  document.getElementById("meta-updated").textContent =
    new Date(DATA.generated_at).toLocaleString("en-GB", {
      day: "numeric", month: "short", year: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  document.getElementById("meta-coverage").textContent =
    `${DATA.visit_count.toLocaleString()} visits · ${DATA.date_range[0]} → ${DATA.date_range[1]}`;
}

function renderAnomalies() {
  const el = document.getElementById("anomalies-banner");
  if (!DATA.anomalies || DATA.anomalies.length === 0) {
    el.innerHTML = "";
    return;
  }
  const items = DATA.anomalies.map(a =>
    `<li class="sev-${a.severity}"><strong>[${a.type}]</strong> ${a.detail}</li>`
  ).join("");
  el.innerHTML = `<div class="anomalies"><h3>Today's update — needs review</h3><ul>${items}</ul></div>`;
}

function renderFilters() {
  // Periods
  const periodPills = [
    {v: "Overall", label: "Overall", cls: "quarter"},
    {v: "Q1", label: "Q1", cls: "quarter", title: "P1+P2+P3"},
    {v: "Q2", label: "Q2", cls: "quarter", title: "P4+P5+P6"},
  ];
  for (const p of DATA.periods) {
    const tot = DATA.totals.by_period[p.id] || 0;
    if (tot > 0) {
      periodPills.push({v: p.id, label: p.id, title: `${p.start} → ${p.end}`});
    }
  }
  const pp = document.getElementById("period-pills");
  pp.innerHTML = periodPills.map(p =>
    `<button class="pill ${p.cls || ''} ${p.v === state.period ? 'active' : ''}"
             data-value="${p.v}" title="${p.title || ''}">${p.label}</button>`
  ).join("");
  pp.querySelectorAll(".pill").forEach(b => b.onclick = () => {
    state.period = b.dataset.value;
    renderAll();
  });

  // Venues
  const venues = ["All", ...DATA.venues];
  const vp = document.getElementById("venue-pills");
  vp.innerHTML = venues.map(v =>
    `<button class="pill ${v === state.venue ? 'active' : ''}" data-value="${v}">${v}</button>`
  ).join("");
  vp.querySelectorAll(".pill").forEach(b => b.onclick = () => {
    state.venue = b.dataset.value;
    renderAll();
  });
}

function renderStats(counts) {
  const top = Object.entries(counts.tally).sort((a, b) => b[1] - a[1])[0];
  const venueT = {};
  for (const m of DATA.mentions) {
    if (m.s !== "p" || !periodInScope(m.p) || !venueInScope(m.b)) continue;
    venueT[m.b] = (venueT[m.b] || 0) + 1;
  }
  const topVenue = Object.entries(venueT).sort((a, b) => b[1] - a[1])[0];
  document.getElementById("stat-strip").innerHTML = `
    <div class="stat">
      <div class="lbl">Positive mentions</div>
      <div class="val">${counts.total.toLocaleString()}</div>
      <div class="sub">${state.period} · ${state.venue === 'All' ? 'all sites' : state.venue}</div>
    </div>
    <div class="stat">
      <div class="lbl">Staff named</div>
      <div class="val">${Object.keys(counts.tally).length}</div>
      <div class="sub">in current view</div>
    </div>
    <div class="stat">
      <div class="lbl">Top venue</div>
      <div class="val">${topVenue ? topVenue[0].replace('FC ', '') : '—'}</div>
      <div class="sub">${topVenue ? topVenue[1] + ' mentions' : ''}</div>
    </div>
    <div class="stat">
      <div class="lbl">Top performer</div>
      <div class="val">${top ? top[0].split(' · ')[0] : '—'}</div>
      <div class="sub">${top ? top[1] + ' mentions' : ''}</div>
    </div>
  `;
}

function renderLeaderboard(counts) {
  const ranked = Object.entries(counts.tally)
    .map(([name, pts]) => {
      const emp = DATA.employees.find(e => e.name === name);
      return {
        name,
        pts,
        venue: emp ? emp.primary_venue : "—",
      };
    })
    .sort((a, b) => b.pts - a.pts)
    .filter(e => e.name.toLowerCase().includes(state.search.toLowerCase()));

  document.getElementById("lb-count").textContent = `(${ranked.length})`;
  const maxPts = ranked[0] ? ranked[0].pts : 1;

  document.getElementById("leaderboard").innerHTML = ranked.map((e, i) => {
    const rank = i + 1;
    const rankCls = rank <= 3 ? `r${rank}` : '';
    const sel = e.name === state.selected ? 'selected' : '';
    const widthPct = (e.pts / maxPts) * 100;
    return `
      <div class="lb-row ${rankCls} ${sel}" data-name="${escapeAttr(e.name)}">
        <div class="rank">${rank}</div>
        <div class="nm">${escapeHtml(e.name)} <span class="venue-tag">${escapeHtml(e.venue.replace('FC ', ''))}</span></div>
        <div class="pts">${e.pts}</div>
        <div class="bar"><div style="width:${widthPct}%"></div></div>
      </div>
    `;
  }).join("");

  document.querySelectorAll(".lb-row").forEach(r => {
    r.onclick = () => { state.selected = r.dataset.name; renderAll(); };
  });
}

function renderDrillDown() {
  const dd = document.getElementById("drill-down");
  if (!state.selected) {
    dd.innerHTML = `<div class="placeholder">Select someone from the leaderboard to see their recognition feed.</div>`;
    return;
  }
  const emp = DATA.employees.find(e => e.name === state.selected);
  if (!emp) {
    dd.innerHTML = `<div class="placeholder">No data for ${escapeHtml(state.selected)}.</div>`;
    return;
  }

  // Period chart — periods that have data
  const periodIds = DATA.periods.map(p => p.id).filter(pid => DATA.totals.by_period[pid] > 0);
  const maxPC = Math.max(1, ...periodIds.map(p => emp.period_counts[p] || 0));
  const bars = periodIds.map(p => {
    const v = emp.period_counts[p] || 0;
    const h = (v / maxPC) * 100;
    return `<div class="bar">
              <div style="height:${h}%"><div class="v">${v || ''}</div></div>
              <div class="lbl">${p}</div>
            </div>`;
  }).join("");

  // Venue breakdown
  const venueRows = Object.entries(emp.venue_counts)
    .sort((a, b) => b[1] - a[1])
    .map(([v, c]) => `<div class="vt-row"><div>${escapeHtml(v)}</div><div class="v">${c}</div></div>`)
    .join("");

  // Quotes — filtered by current period scope
  const quotesByPeriod = {};
  for (const q of emp.quotes) {
    if (!periodInScope(q.period)) continue;
    if (!venueInScope(q.venue)) continue;
    quotesByPeriod[q.period] = quotesByPeriod[q.period] || [];
    quotesByPeriod[q.period].push(q);
  }
  const orderedPeriods = DATA.periods.map(p => p.id).reverse();
  let quotesHtml = "";
  for (const p of orderedPeriods) {
    if (!quotesByPeriod[p]) continue;
    const items = quotesByPeriod[p].map(q =>
      `<div class="dd-quote">${escapeHtml(q.text)}
        <div class="meta">${q.date} · ${escapeHtml(q.venue)}</div></div>`
    ).join("");
    const periodInfo = DATA.periods.find(pp => pp.id === p);
    quotesHtml += `<div class="dd-period-group">
                     <span class="ptag">${p} · ${periodInfo.start} → ${periodInfo.end}</span>
                     ${items}
                   </div>`;
  }
  if (!quotesHtml) quotesHtml = `<div class="placeholder">No quotes in the current view.</div>`;

  const filteredPos = DATA.mentions.filter(m =>
    m.e === emp.name && m.s === "p" && periodInScope(m.p) && venueInScope(m.b)
  ).length;

  dd.innerHTML = `
    <div class="dd-header">
      <div>
        <div class="dd-name">${escapeHtml(emp.name)}</div>
        <div class="dd-venue">${escapeHtml(emp.primary_venue)}</div>
      </div>
    </div>
    <div class="dd-stats">
      <div><span class="pos">${filteredPos}</span> positive (in view)</div>
      <div><span class="neu">${emp.neutral}</span> neutral (excluded)</div>
      <div><span class="neg">${emp.negative}</span> negative (excluded)</div>
    </div>

    <div class="dd-chart">
      <h3>Mentions by period</h3>
      <div class="dd-bars">${bars}</div>
    </div>

    <div class="dd-venues">
      <h3>Venue breakdown</h3>
      ${venueRows || '<div class="placeholder">No data</div>'}
    </div>

    <div class="dd-quotes">
      <h3>Recognition feed (${filteredPos} mentions${emp.bare_mentions ? ` · ${emp.bare_mentions} bare nominations` : ''})</h3>
      ${quotesHtml}
    </div>
  `;
}

function renderVenueTally() {
  const venueT = {};
  for (const m of DATA.mentions) {
    if (m.s !== "p" || !periodInScope(m.p) || !venueInScope(m.b)) continue;
    venueT[m.b] = (venueT[m.b] || 0) + 1;
  }
  const rows = Object.entries(venueT)
    .sort((a, b) => b[1] - a[1])
    .map(([v, c]) => `<div class="vt-row"><div>${escapeHtml(v)}</div><div class="v">${c}</div></div>`)
    .join("");
  document.getElementById("venue-tally").innerHTML = rows || `<div class="placeholder">No data in current view</div>`;
}

function renderAll() {
  const counts = filteredCounts();
  renderFilters();
  renderStats(counts);
  renderLeaderboard(counts);
  renderDrillDown();
  renderVenueTally();
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
}
function escapeAttr(s) { return escapeHtml(s); }

document.getElementById("lb-search").oninput = (e) => {
  state.search = e.target.value;
  renderLeaderboard(filteredCounts());
};

// Init
renderHeader();
renderAnomalies();
renderAll();
</script>
</body>
</html>
"""


def build_dashboard(summary: dict) -> str:
    """Render the dashboard HTML with the summary JSON embedded."""
    data_json = json.dumps(summary, default=str, separators=(",", ":"))
    return HTML_TEMPLATE.replace("__DATA_JSON__", data_json)
