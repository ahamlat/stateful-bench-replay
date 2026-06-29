"use strict";

/* ======================================================================
 * Stateful Replay — Benchmark Console (vanilla JS SPA)
 * ==================================================================== */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const state = {
  tests: [],            // [{name, dims}]
  facets: {},           // {dim: {value: count}}
  metrics: {},          // {name: {mgas, lat, gas, run, kind}}
  metricRange: null,    // {min, max}
  selected: new Set(),
  activeFacets: {},     // {dim: Set(values)}
  search: "",
  threshold: 0,
  heatSort: "order",
  tableSort: { col: "name", dir: 1 },
  config: null,
  source: "fallback",
  warning: null,
  jobsCache: [],
  activeRun: null,
  activeJob: null,
  jobTimer: null,
};

/* ---------------------------------------------------------------- API */
async function api(path, opts) {
  const res = await fetch(path, opts);
  const ctype = res.headers.get("content-type") || "";
  if (ctype.includes("application/json")) {
    const data = await res.json();
    if (!res.ok && data && data.error) throw new Error(data.error);
    return data;
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.text();
}
const postJSON = (path, body) =>
  api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });

function toast(msg, kind = "") {
  const t = $("#toast");
  t.textContent = msg;
  t.className = "toast " + kind;
  t.hidden = false;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => (t.hidden = true), 3800);
}

/* ---------------------------------------------------------------- init */
async function init() {
  bindTabs();
  bindGlobal();
  try {
    const st = await api("/api/state");
    state.config = st.config;
    renderConfigLine();
    $("#conn-dot").className = "conn ok";
  } catch (e) {
    $("#conn-dot").className = "conn bad";
    $("#config-line").textContent = "cannot reach server: " + e.message;
  }
  await Promise.all([loadTests(), loadMetrics()]);
  renderAll();
  refreshJobs();
  setInterval(refreshJobs, 4000);
}

function renderConfigLine() {
  const c = state.config;
  if (!c) return;
  if (!c.ok) {
    $("#config-line").innerHTML = `<span style="color:var(--red)">config error: ${esc(c.error)}</span>`;
    return;
  }
  $("#config-line").innerHTML =
    `image <b>${esc(c.image)}</b> · backend <b>${esc(c.reset_backend)}</b>` +
    (c.skip_gas_bump ? " · <b>skip-gas-bump</b>" : "") +
    ` · input <span class="dim">${esc(c.input_dir)}</span>`;
}

async function loadTests() {
  const data = await api("/api/tests");
  state.tests = data.tests || [];
  state.facets = data.facets || {};
  state.source = data.source;
  state.warning = data.warning;
}
async function loadMetrics() {
  try {
    const m = await api("/api/metrics");
    state.metrics = m.tests || {};
    state.metricRange = m.range;
  } catch (e) { /* metrics optional */ }
}

/* ---------------------------------------------------------------- tabs */
function bindTabs() {
  $$(".tab").forEach((t) =>
    t.addEventListener("click", () => switchTab(t.dataset.tab)));
}
function switchTab(name) {
  $$(".tab").forEach((t) => t.classList.toggle("active", t.dataset.tab === name));
  $$(".view").forEach((v) => (v.hidden = v.id !== "view-" + name));
  if (name === "runs") refreshRuns();
  if (name === "jobs") refreshJobs(true);
}

function bindGlobal() {
  $("#search").addEventListener("input", (e) => { state.search = e.target.value; renderList(); renderHeatmap(); renderFacets(); });
  const setThresh = (v) => { state.threshold = +v || 0; $("#threshold").value = v; $("#threshold-num").value = v; renderList(); renderHeatmap(); };
  $("#threshold").addEventListener("input", (e) => setThresh(e.target.value));
  $("#threshold-num").addEventListener("input", (e) => setThresh(e.target.value));
  $("#clear-facets").addEventListener("click", () => { state.activeFacets = {}; renderAll(); });
  $("#clear-sel").addEventListener("click", () => { state.selected.clear(); renderAll(); });
  $("#select-all").addEventListener("change", (e) => {
    const list = filteredTests();
    if (e.target.checked) list.forEach((t) => state.selected.add(t.name));
    else list.forEach((t) => state.selected.delete(t.name));
    renderAll();
  });
  $$("#heat-sort button").forEach((b) =>
    b.addEventListener("click", () => {
      state.heatSort = b.dataset.v;
      $$("#heat-sort button").forEach((x) => x.classList.toggle("active", x === b));
      renderHeatmap();
    }));
  $$("#tests-table thead th[data-sort]").forEach((th) =>
    th.addEventListener("click", () => {
      const col = th.dataset.sort;
      if (state.tableSort.col === col) state.tableSort.dir *= -1;
      else state.tableSort = { col, dir: col === "name" ? 1 : -1 };
      renderList();
    }));
  $("#open-run").addEventListener("click", () => openLaunchModal("run"));
  $("#open-compare").addEventListener("click", () => openLaunchModal("compare"));
  $("#modal-close").addEventListener("click", closeModal);
  $("#modal").addEventListener("click", (e) => { if (e.target.id === "modal") closeModal(); });
  $("#reload-config").addEventListener("click", async () => {
    try { state.config = await postJSON("/api/reload-config", {}); renderConfigLine(); toast("config reloaded", "ok"); }
    catch (e) { toast(e.message, "err"); }
  });
  $("#refresh-runs").addEventListener("click", () => refreshRuns());
  $("#refresh-jobs").addEventListener("click", () => refreshJobs(true));
}

/* ---------------------------------------------------------------- filtering */
function parsedSearch() {
  const tokens = state.search.trim().split(/\s+/).filter(Boolean);
  const facetTok = {}; const text = [];
  for (const tok of tokens) {
    const m = tok.match(/^([A-Za-z_]+)=(.+)$/);
    if (m) (facetTok[m[1].toLowerCase()] ||= []).push(m[2].toLowerCase());
    else text.push(tok.toLowerCase());
  }
  return { facetTok, text };
}

function matchesFacets(t, facetTok) {
  // chips (state.activeFacets): AND across dims, OR within a dim
  for (const [dim, vals] of Object.entries(state.activeFacets)) {
    if (vals.size === 0) continue;
    if (!vals.has(t.dims[dim])) return false;
  }
  // search facet tokens key=value (substring, OR within key, AND across keys)
  for (const [dim, vals] of Object.entries(facetTok)) {
    const dv = (t.dims[dim] || "").toLowerCase();
    if (!vals.some((v) => dv.includes(v))) return false;
  }
  return true;
}

function filteredTests() {
  const { facetTok, text } = parsedSearch();
  const th = state.threshold;
  return state.tests.filter((t) => {
    if (!matchesFacets(t, facetTok)) return false;
    if (text.length) {
      const hay = t.name.toLowerCase();
      if (!text.every((w) => hay.includes(w))) return false;
    }
    if (th > 0) {
      const mg = metricOf(t.name).mgas;
      if (!(typeof mg === "number" && mg <= th)) return false; // show only slow (<= threshold)
    }
    return true;
  });
}

const metricOf = (name) => state.metrics[name] || {};

/* ---------------------------------------------------------------- render */
function renderAll() {
  renderFacets();
  renderHeatmap();
  renderList();
  renderActionBar();
}

function renderFacets() {
  const wrap = $("#facets");
  const dims = Object.keys(state.facets).sort(facetOrder);
  $("#facets-summary").textContent =
    `(${dims.length} dimensions, ${Object.values(state.activeFacets).filter((s) => s && s.size).length} active)`;
  if (state.warning) {
    let banner = $("#facet-warn");
    if (!banner) { banner = document.createElement("div"); banner.id = "facet-warn"; banner.className = "warn-banner"; wrap.before(banner); }
    banner.textContent = `Test list source: ${state.source}. ${state.warning}`;
  }
  // recompute counts on the currently-filtered set (minus the dim itself) — spamoor-style
  wrap.innerHTML = dims.map((dim) => {
    const counts = facetCounts(dim);
    const active = state.activeFacets[dim] || new Set();
    const values = Object.entries(counts).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    const visible = values.slice(0, 14);
    const hidden = values.slice(14);
    const chip = ([v, c]) =>
      `<span class="chip ${active.has(v) ? "on" : ""}" data-dim="${esc(dim)}" data-val="${esc(v)}">${esc(v)} <span class="c">${c}</span></span>`;
    return `<div class="facet-dim">
      <div class="facet-name">${esc(dim)} <span class="cnt">(${values.length})</span></div>
      <div class="facet-values">
        ${visible.map(chip).join("")}
        ${hidden.length ? `<span class="chip ${hidden.map(h=>h[0]).some(v=>active.has(v))?"on":""}" data-more="${esc(dim)}">+${hidden.length} more</span>` : ""}
        <span class="facets-hidden" data-hidden="${esc(dim)}">${hidden.map(chip).join("")}</span>
      </div>
    </div>`;
  }).join("");

  $$(".chip[data-dim]", wrap).forEach((ch) =>
    ch.addEventListener("click", () => toggleFacet(ch.dataset.dim, ch.dataset.val)));
  $$(".chip[data-more]", wrap).forEach((ch) =>
    ch.addEventListener("click", () => {
      const box = $(`.facets-hidden[data-hidden="${CSS.escape(ch.dataset.more)}"]`, wrap);
      if (box) { box.classList.toggle("facets-hidden"); ch.style.display = "none"; }
    }));
}

const FACET_PRIORITY = ["file", "test", "opcode", "gas", "value_sent", "account_mode", "cache_strategy", "fork"];
function facetOrder(a, b) {
  const ia = FACET_PRIORITY.indexOf(a), ib = FACET_PRIORITY.indexOf(b);
  if (ia !== -1 || ib !== -1) return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib);
  return a.localeCompare(b);
}

function facetCounts(dimToCount) {
  // count values of dimToCount over tests filtered by everything EXCEPT that dim
  const { facetTok, text } = parsedSearch();
  const counts = {};
  for (const t of state.tests) {
    // apply chip facets except dimToCount
    let ok = true;
    for (const [dim, vals] of Object.entries(state.activeFacets)) {
      if (dim === dimToCount || !vals.size) continue;
      if (!vals.has(t.dims[dim])) { ok = false; break; }
    }
    if (ok) for (const [dim, vals] of Object.entries(facetTok)) {
      if (dim === dimToCount) continue;
      const dv = (t.dims[dim] || "").toLowerCase();
      if (!vals.some((v) => dv.includes(v))) { ok = false; break; }
    }
    if (ok && text.length) { const hay = t.name.toLowerCase(); if (!text.every((w) => hay.includes(w))) ok = false; }
    if (!ok) continue;
    const v = t.dims[dimToCount];
    if (v != null) counts[v] = (counts[v] || 0) + 1;
  }
  return counts;
}

function toggleFacet(dim, val) {
  const set = (state.activeFacets[dim] ||= new Set());
  if (set.has(val)) set.delete(val); else set.add(val);
  if (!set.size) delete state.activeFacets[dim];
  renderAll();
}

/* ---- heatmap ---- */
function heatColor(mgas) {
  if (typeof mgas !== "number" || !state.metricRange) return null;
  const { min, max } = state.metricRange;
  let t = max > min ? (mgas - min) / (max - min) : 0.5;
  t = Math.max(0, Math.min(1, t));
  // red(0) -> amber(.5) -> green(1)
  const lerp = (a, b, k) => Math.round(a + (b - a) * k);
  let r, g, b;
  if (t < 0.5) { const k = t / 0.5; r = lerp(192, 224, k); g = lerp(57, 168, k); b = lerp(43, 0, k); }
  else { const k = (t - 0.5) / 0.5; r = lerp(224, 46, k); g = lerp(168, 204, k); b = lerp(0, 113, k); }
  return `rgb(${r},${g},${b})`;
}

function renderHeatmap() {
  const list = filteredTests().slice();
  if (state.heatSort === "mgas") list.sort((a, b) => (metricOf(b.name).mgas ?? -1) - (metricOf(a.name).mgas ?? -1));
  else if (state.heatSort === "gas") list.sort((a, b) => (metricOf(b.name).gas ?? -1) - (metricOf(a.name).gas ?? -1));

  const withData = list.filter((t) => typeof metricOf(t.name).mgas === "number").length;
  $("#heat-meta").textContent = `${list.length} tests · ${withData} with metrics`;
  if (state.metricRange)
    $("#heat-range").textContent = `${state.metricRange.min.toFixed(1)} – ${state.metricRange.max.toFixed(1)} MGas/s`;

  const hm = $("#heatmap");
  hm.innerHTML = list.map((t) => {
    const m = metricOf(t.name);
    const col = heatColor(m.mgas);
    const cls = "cell" + (col ? "" : " nodata") + (state.selected.has(t.name) ? " sel" : "");
    const style = col ? `background:${col}` : "";
    const title = `${t.name}\n${typeof m.mgas === "number" ? m.mgas.toFixed(1) + " MGas/s" : "no metric"}`;
    return `<div class="${cls}" style="${style}" data-name="${esc(t.name)}" title="${esc(title)}"></div>`;
  }).join("");
  $$(".cell", hm).forEach((c) =>
    c.addEventListener("click", () => { toggleSelect(c.dataset.name); }));
}

/* ---- table ---- */
function renderList() {
  const list = filteredTests();
  const s = state.tableSort;
  const key = (t) => {
    if (s.col === "name") return t.name;
    const m = metricOf(t.name);
    return (typeof m[s.col] === "number") ? m[s.col] : (s.dir === 1 ? Infinity : -Infinity);
  };
  list.sort((a, b) => { const ka = key(a), kb = key(b); return ka < kb ? -s.dir : ka > kb ? s.dir : 0; });

  $("#list-count").textContent = `(${list.length} of ${state.tests.length})`;
  $("#search-meta").textContent =
    `${list.length} tests match · ${state.selected.size} selected` + (state.source === "fallback" ? " · (fallback list — input dir not mounted)" : "");

  const body = $("#tests-body");
  body.innerHTML = list.map((t) => {
    const m = metricOf(t.name);
    const sel = state.selected.has(t.name);
    const col = heatColor(m.mgas);
    const dims = Object.entries(t.dims).map(([k, v]) => `<span class="d">${esc(k)}=<b>${esc(v)}</b></span>`).join("");
    return `<tr class="${sel ? "sel" : ""}" data-name="${esc(t.name)}">
      <td class="c-check"><input type="checkbox" ${sel ? "checked" : ""}></td>
      <td class="tname">
        <span class="mini-dot" style="background:${col || "#1d2433"}"></span><code>${esc(t.name)}</code>
        <div class="tdims">${dims}</div>
      </td>
      <td class="num">${typeof m.mgas === "number" ? m.mgas.toFixed(1) : "·"}</td>
      <td class="num">${typeof m.lat === "number" ? m.lat.toFixed(1) : "·"}</td>
      <td class="num">${typeof m.gas === "number" ? (m.gas / 1e6).toFixed(1) + "M" : "·"}</td>
    </tr>`;
  }).join("");
  $$("tr[data-name]", body).forEach((tr) => {
    tr.addEventListener("click", (e) => {
      if (e.target.tagName === "INPUT") return; // checkbox handles itself below
      toggleSelect(tr.dataset.name);
    });
    $("input", tr).addEventListener("change", () => toggleSelect(tr.dataset.name));
  });
  // sync select-all checkbox
  const allSelected = list.length && list.every((t) => state.selected.has(t.name));
  $("#select-all").checked = !!allSelected;
}

function toggleSelect(name) {
  if (state.selected.has(name)) state.selected.delete(name); else state.selected.add(name);
  renderActionBar();
  // light refresh of affected rows + heatmap selection
  renderList(); renderHeatmap();
}

function renderActionBar() {
  const n = state.selected.size;
  $("#actionbar").hidden = n === 0;
  $("#sel-count").textContent = n;
}

/* ---------------------------------------------------------------- launch modal */
function openLaunchModal(kind) {
  const tests = [...state.selected];
  if (!tests.length) { toast("select at least one test"); return; }
  const c = state.config || {};
  const backend = c.reset_backend || "overlayfs";
  const preview = tests.slice(0, 30).map((t) => `<code>${esc(t)}</code>`).join("") +
    (tests.length > 30 ? `<div class="dim">… and ${tests.length - 30} more</div>` : "");

  const commonOpts = `
    <div class="field">
      <label>Reset backend</label>
      <select id="m-backend">
        <option value="overlayfs" ${backend === "overlayfs" ? "selected" : ""}>overlayfs</option>
        <option value="schelk" ${backend === "schelk" ? "selected" : ""}>schelk</option>
      </select>
    </div>
    <div class="opts">
      <label><input type="checkbox" id="m-skip" ${c.skip_gas_bump ? "checked" : ""}> skip gas-bump</label>
      <label><input type="checkbox" id="m-persist"> persist prelude (overlayfs)</label>
      <label><input type="checkbox" id="m-profile"> profile</label>
      <label><input type="checkbox" id="m-dry"> dry-run</label>
    </div>`;

  let body;
  if (kind === "run") {
    body = `
      <p class="dim">Run <b>${tests.length}</b> selected test(s) once on the configured image
        (<code>${esc(c.image || "?")}</code>).</p>
      <div class="sel-preview">${preview}</div>
      ${commonOpts}
      <div class="modal-actions">
        <button class="btn ghost" id="m-cancel">Cancel</button>
        <button class="btn primary" id="m-go">▶ Launch run</button>
      </div>`;
  } else {
    body = `
      <p class="dim">Compare two Besu images on the <b>${tests.length}</b> selected test(s).</p>
      <div class="sel-preview">${preview}</div>
      <div class="field-row">
        <div class="field">
          <label>Image X (baseline)</label>
          <input type="text" id="m-imgx" placeholder="${esc(c.image || "besu.image")}">
          <div class="hint">blank = config besu.image</div>
        </div>
        <div class="field">
          <label>Image Y (candidate) *</label>
          <input type="text" id="m-imgy" placeholder="ethpandaops/besu:my-branch">
        </div>
      </div>
      <div class="field-row">
        <div class="field"><label>Label X</label><input type="text" id="m-lblx" placeholder="auto"></div>
        <div class="field"><label>Label Y</label><input type="text" id="m-lbly" placeholder="auto"></div>
      </div>
      ${commonOpts}
      <div class="modal-actions">
        <button class="btn ghost" id="m-cancel">Cancel</button>
        <button class="btn primary" id="m-go">⇄ Launch comparison</button>
      </div>`;
  }
  $("#modal-title").textContent = kind === "run" ? "Run selected tests" : "Compare versions";
  $("#modal-body").innerHTML = body;
  $("#modal").hidden = false;
  $("#m-cancel").addEventListener("click", closeModal);
  $("#m-go").addEventListener("click", () => launch(kind, tests));
}

function commonPayload() {
  return {
    reset_backend: $("#m-backend").value,
    skip_gas_bump: $("#m-skip").checked,
    persist_prelude: $("#m-persist").checked,
    profile: $("#m-profile").checked,
    dry_run: $("#m-dry").checked,
  };
}

async function launch(kind, tests) {
  const go = $("#m-go"); go.disabled = true;
  try {
    let resp;
    if (kind === "run") {
      resp = await postJSON("/api/run", { tests, ...commonPayload() });
    } else {
      const imgy = $("#m-imgy").value.trim();
      if (!imgy) { toast("Image Y is required", "err"); go.disabled = false; return; }
      resp = await postJSON("/api/compare", {
        tests, image_x: $("#m-imgx").value.trim(), image_y: imgy,
        label_x: $("#m-lblx").value.trim(), label_y: $("#m-lbly").value.trim(),
        ...commonPayload(),
      });
    }
    closeModal();
    toast(`${kind} launched (${resp.job.n_tests} tests)`, "ok");
    switchTab("jobs");
    refreshJobs(true);
    selectJob(resp.job.id);
  } catch (e) {
    toast(e.message, "err"); go.disabled = false;
  }
}
function closeModal() { $("#modal").hidden = true; }

/* ---------------------------------------------------------------- jobs */
async function refreshJobs(force) {
  const prevRunning = (state.jobsCache || []).filter((j) => j.status === "running").length;
  try {
    const data = await api("/api/jobs");
    state.jobsCache = data.jobs || [];
  } catch (e) { return; }
  const running = state.jobsCache.filter((j) => j.status === "running").length;
  if (running < prevRunning) {
    // A job just finished: refresh the explorer's metrics from new results.
    loadMetrics().then(() => { renderHeatmap(); renderList(); });
  }
  const badge = $("#jobs-badge");
  badge.hidden = running === 0; badge.textContent = running;
  if (!$("#view-jobs").hidden || force) renderJobsList();
  if (state.activeJob) {
    const j = state.jobsCache.find((x) => x.id === state.activeJob);
    if (j && j.status === "running") pollJobLog(state.activeJob);
  }
}

function renderJobsList() {
  const wrap = $("#jobs-list");
  if (!state.jobsCache.length) { wrap.innerHTML = `<div class="empty dim">No jobs yet.</div>`; return; }
  wrap.innerHTML = state.jobsCache.map((j) => `
    <div class="run-card ${state.activeJob === j.id ? "active" : ""}" data-id="${esc(j.id)}">
      <div class="rc-top">
        <span class="rid">${esc(j.kind)} · ${esc(j.id)}</span>
        <span class="tag ${esc(j.status)}">${esc(j.status)}</span>
      </div>
      <div class="rc-meta">${j.n_tests} tests · ${fmtDur(j.elapsed_s)}${j.run_dir ? " · " + esc(j.run_dir) : ""}</div>
    </div>`).join("");
  $$(".run-card", wrap).forEach((c) => c.addEventListener("click", () => selectJob(c.dataset.id)));
}

function selectJob(id) {
  state.activeJob = id;
  renderJobsList();
  pollJobLog(id);
}

async function pollJobLog(id) {
  let data;
  try { data = await api(`/api/jobs/${id}/log`); } catch (e) { return; }
  const j = data.job || {};
  const det = $("#job-detail");
  const canCancel = j.status === "running";
  const runLink = j.run_dir
    ? `<a class="filelink" href="#" data-run="${esc(j.run_dir)}">view results →</a>` : "";
  det.innerHTML = `
    <div class="rd-head">
      <h2>${esc(j.kind || "job")} <span class="dim">${esc(id)}</span></h2>
      <span class="tag ${esc(j.status)}">${esc(j.status)}</span>
      ${canCancel ? `<button class="btn danger sm" id="cancel-job">■ cancel</button>` : ""}
      ${runLink}
    </div>
    <div class="kv">
      <span><span class="k">tests</span> <span class="v">${j.n_tests ?? "?"}</span></span>
      <span><span class="k">elapsed</span> <span class="v">${fmtDur(j.elapsed_s || 0)}</span></span>
      <span><span class="k">exit</span> <span class="v">${j.returncode == null ? "—" : j.returncode}</span></span>
    </div>
    <div class="log" id="job-log">${esc(data.log || "")}</div>`;
  const logEl = $("#job-log");
  logEl.scrollTop = logEl.scrollHeight;
  const cb = $("#cancel-job");
  if (cb) cb.addEventListener("click", async () => {
    try { await postJSON(`/api/jobs/${id}/cancel`, {}); toast("cancel signal sent"); refreshJobs(true); }
    catch (e) { toast(e.message, "err"); }
  });
  const rl = det.querySelector("[data-run]");
  if (rl) rl.addEventListener("click", (e) => { e.preventDefault(); switchTab("runs"); refreshRuns().then(() => selectRun(rl.dataset.run)); });
}

/* ---------------------------------------------------------------- runs */
async function refreshRuns() {
  let data;
  try { data = await api("/api/runs"); } catch (e) { toast(e.message, "err"); return; }
  const wrap = $("#runs-list");
  const runs = data.runs || [];
  if (!runs.length) { wrap.innerHTML = `<div class="empty dim">No runs yet.</div>`; return; }
  wrap.innerHTML = runs.map((r) => {
    let extra = "";
    if (r.kind === "compare" && r.compare_summary) {
      const cs = r.compare_summary;
      extra = `y faster ${cs.y_faster ?? "?"} / slower ${cs.y_slower ?? "?"}`;
    } else if (r.totals) {
      extra = `${r.totals.ok ?? 0} ok / ${r.totals.fail ?? 0} fail`;
    }
    return `<div class="run-card ${state.activeRun === r.id ? "active" : ""}" data-id="${esc(r.id)}">
      <div class="rc-top"><span class="rid">${esc(r.id)}</span><span class="tag ${esc(r.kind)}">${esc(r.kind)}</span></div>
      <div class="rc-meta">${r.selected} tests${extra ? " · " + esc(extra) : ""}</div>
    </div>`;
  }).join("");
  $$(".run-card", wrap).forEach((c) => c.addEventListener("click", () => selectRun(c.dataset.id)));
}

async function selectRun(id) {
  state.activeRun = id;
  $$("#runs-list .run-card").forEach((c) => c.classList.toggle("active", c.dataset.id === id));
  let d;
  try { d = await api(`/api/runs/${id}`); } catch (e) { toast(e.message, "err"); return; }
  renderRunDetail(d);
}

function renderRunDetail(d) {
  const det = $("#run-detail");
  const files = (d.files || []).map((f) =>
    `<a class="filelink" href="/runs/${encodeURIComponent(d.id)}/${encodeURIComponent(f.name)}" target="_blank">${esc(f.name)}</a>`).join("");
  let html = `<div class="rd-head"><h2>${esc(d.id)}</h2><span class="tag ${esc(d.kind)}">${esc(d.kind)}</span>`;
  if (d.fail_fast_tripped) html += `<span class="tag failed">fail-fast</span>`;
  html += `</div>`;

  if (d.comparison) html += renderComparison(d.comparison);
  else html += renderSweep(d);

  html += `<div class="section-title">Files</div><div class="filelinks">${files || "<span class='dim'>none</span>"}</div>`;
  if (d.events && d.events.length) {
    html += `<div class="section-title">Events (tail)</div><div class="log">${esc(d.events.join("\n"))}</div>`;
  }
  det.innerHTML = html;
}

function renderComparison(cmp) {
  const s = cmp.summary || {};
  const vx = cmp.version_x || {}, vy = cmp.version_y || {};
  const pct = s.overall_mgas_pct;
  const cards = `
    <div class="cards">
      <div class="mcard"><div class="k">X</div><div class="v">${esc(vx.label || "x")}</div></div>
      <div class="mcard"><div class="k">Y</div><div class="v">${esc(vy.label || "y")}</div></div>
      <div class="mcard"><div class="k">Overall Δ</div><div class="v ${pct > 0 ? "good" : pct < 0 ? "bad" : ""}">${fmtPct(pct)}</div></div>
      <div class="mcard"><div class="k">Y faster</div><div class="v good">${s.y_faster ?? 0}</div></div>
      <div class="mcard"><div class="k">Y slower</div><div class="v bad">${s.y_slower ?? 0}</div></div>
      <div class="mcard"><div class="k">Gas mism.</div><div class="v ${s.gas_mismatches ? "bad" : ""}">${s.gas_mismatches ?? 0}</div></div>
    </div>`;
  const rows = (cmp.rows || []).slice().sort((a, b) =>
    (a.delta_mgas_pct ?? Infinity) - (b.delta_mgas_pct ?? Infinity));
  const body = rows.map((r) => {
    const cls = r.delta_mgas_pct > 1 ? "faster" : r.delta_mgas_pct < -1 ? "slower" : "";
    return `<tr>
      <td class="tname"><code>${esc(r.test)}</code></td>
      <td class="num">${fmtM(r.gas_used)}${r.gas_match === false ? " ⚠" : ""}</td>
      <td class="num">${fmtN(r.x_mgas)}</td>
      <td class="num">${fmtN(r.y_mgas)}</td>
      <td class="num ${cls}">${fmtPct(r.delta_mgas_pct)}</td>
      <td class="num">${fmtN(r.x_lat_ms, 1)}</td>
      <td class="num">${fmtN(r.y_lat_ms, 1)}</td>
      <td class="num">${r.delta_lat_ms == null ? "·" : (r.delta_lat_ms > 0 ? "+" : "") + r.delta_lat_ms.toFixed(1)}</td>
    </tr>`;
  }).join("");
  return cards + `<div class="section-title">Per-test comparison (worst regression first)</div>
    <div class="table-wrap"><table class="cmp-table">
      <thead><tr><th>Test</th><th class="num">Gas</th>
        <th class="num">${esc(vx.label || "X")} MGas/s</th><th class="num">${esc(vy.label || "Y")} MGas/s</th>
        <th class="num">Δ%</th><th class="num">X lat</th><th class="num">Y lat</th><th class="num">Δ lat</th></tr></thead>
      <tbody>${body}</tbody></table></div>`;
}

function renderSweep(d) {
  const sel = d.selected_tests || [];
  const metrics = d.metrics || {};      // parsed from THIS run's Besu logs
  const fails = d.failures || [];
  const totals = (d.summary && d.summary.totals) || {};
  const cards = `<div class="cards">
      <div class="mcard"><div class="k">Selected</div><div class="v">${sel.length || d.selected}</div></div>
      <div class="mcard"><div class="k">OK</div><div class="v good">${totals.ok ?? "?"}</div></div>
      <div class="mcard"><div class="k">Fail</div><div class="v ${totals.fail ? "bad" : ""}">${totals.fail ?? "?"}</div></div>
    </div>`;
  let rows = "";
  if (sel.length) {
    rows = sel.map((name) => {
      const m = metrics[name] || {};
      const failed = m.ok === false;
      const gas = typeof m.gas === "number" ? (m.gas / 1e6).toFixed(1) + "M" : "·";
      return `<tr>
        <td class="tname"><code>${esc(name)}</code></td>
        <td class="num">${typeof m.mgas === "number" ? m.mgas.toFixed(1) : "·"}</td>
        <td class="num">${typeof m.lat === "number" ? m.lat.toFixed(1) : "·"}</td>
        <td class="num">${gas}</td>
        <td>${failed ? `<span class="tag failed">fail</span>` : `<span class="tag done">ok</span>`}</td>
      </tr>`;
    }).join("");
  }
  let html = cards;
  if (rows) html += `<div class="section-title">Selected tests</div>
    <div class="table-wrap"><table>
      <thead><tr><th>Test</th><th class="num">MGas/s</th><th class="num">Lat (ms)</th><th class="num">Gas</th><th>Status</th></tr></thead>
      <tbody>${rows}</tbody></table></div>`;
  if (fails.length) {
    html += `<div class="section-title">Failures (${fails.length})</div><div class="log">${esc(fails.map((f) => JSON.stringify(f)).join("\n"))}</div>`;
  }
  return html;
}

/* ---------------------------------------------------------------- utils */
const fmtN = (v, d = 1) => (typeof v === "number" ? v.toFixed(d) : "·");
const fmtM = (v) => (typeof v === "number" ? (v / 1e6).toFixed(1) + "M" : "·");
const fmtPct = (v) => (typeof v === "number" ? (v > 0 ? "+" : "") + v.toFixed(1) + "%" : "·");
function fmtDur(s) {
  s = Math.round(s || 0);
  if (s < 60) return s + "s";
  const m = Math.floor(s / 60), r = s % 60;
  if (m < 60) return `${m}m${r ? r + "s" : ""}`;
  const h = Math.floor(m / 60);
  return `${h}h${m % 60}m`;
}

window.addEventListener("DOMContentLoaded", init);
