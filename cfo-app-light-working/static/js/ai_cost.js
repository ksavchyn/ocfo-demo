/* AI Cost & Governance tab — fetches gold_ai_cost_* via /api/ai-cost/*, renders
   Chart.js charts + tables, wires toggle / forecast sliders / nudges / Genie chips. */
(function () {
  "use strict";

  const DB = {
    orange: "#ff5f46", teal: "#4a9cb8", green: "#00a972", red: "#ff6b7a",
    amber: "#ffab00", gray: "#8fa3aa", blue: "#4299e0", ink: "#1b2f36",
  };
  // Data-series accent: the app's true "orange" (amber #ffab00, the --db-warning used on
  // the at-risk table badges). DB.orange (#ff5f46) is the coral brand-primary — it reads
  // as red and clashed with the red reprice/over-budget badges, so we use amber on % lines
  // and over-threshold bars, reserving DB.red only for genuine breach/over states.
  const ACCENT = DB.amber;
  const fmtUSD = (v) => {
    v = +v || 0;
    if (Math.abs(v) >= 1e9) return "$" + (v / 1e9).toFixed(2) + "B";
    if (Math.abs(v) >= 1e6) return "$" + (v / 1e6).toFixed(1) + "M";
    if (Math.abs(v) >= 1e3) return "$" + (v / 1e3).toFixed(0) + "K";
    return "$" + v.toFixed(0);
  };
  const fmtPct = (v, d = 2) => (v == null ? "—" : (+v).toFixed(d) + "%");
  const el = (id) => document.getElementById(id);

  // Draws text labels floating just ABOVE horizontal reference lines, pinned to the right
  // edge. Chart.js 3.9 has no annotation plugin, so this inline plugin renders them directly.
  // Config: options.plugins.thresholdLabel.lines = [{ value, text, axis, color }].
  const thresholdLabelPlugin = {
    id: "thresholdLabel",
    afterDatasetsDraw(chart, args, opts) {
      if (!opts || !opts.lines) return;
      const { ctx, chartArea } = chart;
      opts.lines.forEach((ln) => {
        const scale = chart.scales[ln.axis || "y"];
        if (!scale || ln.value == null) return;
        const y = scale.getPixelForValue(ln.value);
        ctx.save();
        ctx.font = "700 13px -apple-system, Segoe UI, sans-serif";
        ctx.fillStyle = ln.color || "#ff5f46";
        ctx.textAlign = "right";
        ctx.textBaseline = "bottom";
        ctx.fillText(ln.text, chartArea.right - 6, y - 6);   // ABOVE the line
        ctx.restore();
      });
    },
  };

  // Makes a horizontal threshold line DRAGGABLE. Grabs the flat line dataset whose label
  // starts with "Threshold", lets the user drag it up/down on its axis, and calls
  // opts.onChange(newValue) so the chart can recolor bars live. Config:
  // options.plugins.dragThreshold = { datasetIndex, axis, min, max, onChange }.
  const dragThresholdPlugin = {
    id: "dragThreshold",
    afterEvent(chart, args) {
      const opts = chart.options.plugins.dragThreshold;
      if (!opts) return;
      const e = args.event;
      const scale = chart.scales[opts.axis || "y1"];
      const ds = chart.data.datasets[opts.datasetIndex];
      if (!ds || !scale) return;
      const lineY = scale.getPixelForValue(ds.data[0]);
      const near = Math.abs(e.y - lineY) < 10 && e.x >= chart.chartArea.left && e.x <= chart.chartArea.right;
      if (e.type === "mousemove") {
        if (chart._draggingThreshold) {
          let v = scale.getValueForPixel(e.y);
          v = Math.max(opts.min, Math.min(opts.max, v));
          const rounded = +v.toFixed(1);
          // Mutate the threshold line FIRST...
          ds.data = ds.data.map(() => rounded);
          // ...then let onChange mutate label + bar colors (WITHOUT its own chart.update)...
          if (opts.onChange) opts.onChange(rounded);
          // ...then a SINGLE update paints everything. (Calling update() twice re-entrantly
          // — once here, again inside onChange — makes Chart.js drop the second, so the label
          // and bar recolor never rendered. One update at the end fixes that.)
          chart.update("none");
        }
        chart.canvas.style.cursor = (near || chart._draggingThreshold) ? "ns-resize" : "default";
      } else if (e.type === "mousedown" && near) {
        chart._draggingThreshold = true;
      } else if (e.type === "mouseup" || e.type === "mouseout") {
        chart._draggingThreshold = false;
      }
    },
  };

  // Custom legend label generator: renders line-type datasets as an actual LINE glyph
  // (dashed if the series is dashed) instead of Chart.js's default filled box; bars stay
  // as boxes. Requires legend.labels.usePointStyle = true. Pass as
  // options.plugins.legend.labels.generateLabels = lineAwareLegend.
  function lineAwareLegend(chart) {
    return chart.data.datasets.map((ds, i) => {
      const isLine = ds.type === "line";
      const meta = chart.getDatasetMeta(i);
      const swatch = Array.isArray(ds.backgroundColor) ? ds.backgroundColor[0] : ds.backgroundColor;
      return {
        text: ds.label,
        // Line series → 'line' point style draws a horizontal line honoring lineDash/width;
        // bar series → 'rect' box.
        pointStyle: isLine ? "line" : "rect",
        fillStyle: isLine ? ds.borderColor : swatch,
        strokeStyle: isLine ? ds.borderColor : swatch,
        lineWidth: isLine ? (ds.borderWidth || 2) : 0,
        lineDash: isLine ? (ds.borderDash || []) : [],
        hidden: !meta.visible,
        datasetIndex: i,
      };
    });
  }

  // Value-Lens quadrant plugin: paints the four decision zones + median separators +
  // an action label per quadrant. Drawn BEFORE datasets (beforeDatasetsDraw) so bubbles
  // sit on top. Config via options.plugins.valueQuadrants { xMid, yMid }.
  //   TR (high AI $, high margin) = green  → "Maintain — AI is paying off"
  //   BR (high AI $, low margin)  = red    → "Evaluate ROI — spend without return"
  //   TL (low AI $,  high margin) = yellow → "Deploy more AI to lift margin"
  //   BL (low AI $,  low margin)  = yellow → "Identify value-creating AI use"
  const valueQuadrantsPlugin = {
    id: "valueQuadrants",
    beforeDatasetsDraw(chart, args, opts) {
      if (!opts || opts.xMid == null) return;
      const { ctx, chartArea: a, scales } = chart;
      const xm = scales.x.getPixelForValue(opts.xMid);
      const ym = scales.y.getPixelForValue(opts.yMid);
      const GREEN = "rgba(0,169,114,.10)", RED = "rgba(255,107,122,.12)", YEL = "rgba(255,171,0,.10)";
      ctx.save();
      // quadrant fills
      ctx.fillStyle = YEL; ctx.fillRect(a.left, a.top, xm - a.left, ym - a.top);       // TL
      ctx.fillStyle = GREEN; ctx.fillRect(xm, a.top, a.right - xm, ym - a.top);         // TR
      ctx.fillStyle = YEL; ctx.fillRect(a.left, ym, xm - a.left, a.bottom - ym);        // BL
      ctx.fillStyle = RED; ctx.fillRect(xm, ym, a.right - xm, a.bottom - ym);           // BR
      // median separators
      ctx.strokeStyle = "#c0ccd0"; ctx.lineWidth = 1; ctx.setLineDash([5, 4]);
      ctx.beginPath(); ctx.moveTo(xm, a.top); ctx.lineTo(xm, a.bottom); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(a.left, ym); ctx.lineTo(a.right, ym); ctx.stroke();
      ctx.setLineDash([]);
      // action labels (one per quadrant, tucked into the outer corner)
      ctx.font = "700 11px -apple-system, Segoe UI, sans-serif";
      const pad = 8;
      const lab = (text, x, y, align, baseline, color) => {
        ctx.fillStyle = color; ctx.textAlign = align; ctx.textBaseline = baseline;
        ctx.fillText(text, x, y);
      };
      lab("Deploy AI to lift margin", a.left + pad, a.top + pad, "left", "top", "#8a6d00");     // TL
      lab("Maintain — AI paying off", a.right - pad, a.top + pad, "right", "top", "#00734a");    // TR
      lab("Identify value-creating AI use", a.left + pad, a.bottom - pad, "left", "bottom", "#8a6d00"); // BL
      lab("Evaluate ROI — spend w/o return", a.right - pad, a.bottom - pad, "right", "bottom", "#c0344a"); // BR
      ctx.restore();
    },
  };

  let ENG = [];            // cached engagement rows (drive several sections)
  let TREND = [];          // cached trend rows (month × region × location × practice × industry)
  let TREND_PCT = {};      // firmwide %-of-revenue by month (fallback)
  let TREND_REV = [];      // revenue by month × dims — denominator for the filtered % line
  let KPI = {};            // raw kpis payload (incl. by_dim + revenue_by_dim for recompute)
  let charts = {};
  let clientThreshold = 15;   // client-chart AI-%-of-invoice threshold (draggable, persists)

  // Active filter values (shared by every section so the WHOLE page filters uniformly).
  function activeFilters() {
    return {
      region: el("fRegion").value, location: el("fLocation").value,
      practice: el("fPractice").value, industry: el("fIndustry").value,
    };
  }
  // Generic dim-match: a row matches when each active filter is "All" or equals the row's value.
  function dimMatch(r, f) {
    return (f.region === "All" || r.region === f.region) &&
           (f.location === "All" || r.location === f.location) &&
           (f.practice === "All" || r.practice === f.practice) &&
           (f.industry === "All" || r.industry === f.industry);
  }

  async function getJSON(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(url + " -> " + r.status);
    return r.json();
  }

  // ─── KPIs ───────────────────────────────────────────────────────────────
  async function loadKPIs() {
    try { KPI = await getJSON("/api/ai-cost/kpis"); renderKPIs(); }
    catch (e) { console.error("[ai-cost] kpis", e); }
  }
  // Recompute every KPI tile from the filtered per-dimension breakouts (by_dim +
  // revenue_by_dim). Ratios (%rev, cost/identity) = SUM(numerator)/SUM(denominator)
  // over the filtered subset — never averaged. When no filter is active this equals
  // the firmwide figures the API also returns. Everything traces to real sources:
  // spend/governed/identities = AI telemetry; revenue = the customer's own P&L.
  function renderKPIs() {
    const k = KPI || {};
    const f = activeFilters();
    const anyFilter = f.region !== "All" || f.location !== "All" || f.practice !== "All" || f.industry !== "All";
    const dims = (k.by_dim || []).filter((r) => dimMatch(r, f));
    const revs = (k.revenue_by_dim || []).filter((r) => dimMatch(r, f));

    if (!anyFilter || !dims.length) {
      // firmwide (or nothing matched) → use the API's headline figures directly
      el("kpiSpend").textContent = fmtUSD(k.ttm_ai_spend);
      el("kpiPct").textContent = fmtPct(k.ttm_pct_revenue, 2);
      el("kpiPctOpex").textContent = fmtPct(k.ttm_pct_opex, 2);
      el("kpiCov").textContent = fmtPct(k.governance_coverage_pct, 0);
      el("kpiMix").textContent = Math.round(k.frontier_pct) + " / " + Math.round(k.oss_pct);
      el("kpiMixSub").textContent = (k.frontier_pct >= 50 ? "frontier-heavy" : "OSS-heavy");
    } else {
      const spend = dims.reduce((s, r) => s + (r.spend || 0), 0);
      const governed = dims.reduce((s, r) => s + (r.governed || 0), 0);
      const frontier = dims.reduce((s, r) => s + (r.frontier || 0), 0);
      const revenue = revs.reduce((s, r) => s + (r.revenue || 0), 0);
      const opex = revs.reduce((s, r) => s + (r.opex || 0), 0);
      el("kpiSpend").textContent = fmtUSD(spend);
      el("kpiPct").textContent = revenue > 0 ? fmtPct(spend / revenue * 100, 2) : "—";
      el("kpiPctOpex").textContent = opex > 0 ? fmtPct(spend / opex * 100, 2) : "—";
      el("kpiCov").textContent = spend > 0 ? fmtPct(governed / spend * 100, 0) : "—";
      const frPct = spend > 0 ? Math.round(frontier / spend * 100) : 0;
      el("kpiMix").textContent = frPct + " / " + (100 - frPct);
      el("kpiMixSub").textContent = frPct >= 50 ? "frontier-heavy" : "OSS-heavy";
    }
    el("kpiSpendDelta").textContent = "▲ 40% YoY";
  }

  // ─── Trend chart ──────────────────────────────────────────────────────────
  async function loadTrend() {
    try {
      const payload = await getJSON("/api/ai-cost/trend");
      TREND = payload.rows || [];
      TREND_PCT = payload.pct_by_month || {};
      TREND_REV = payload.rev_rows || [];
      renderTrend();
    } catch (e) { console.error("[ai-cost] trend", e); }
  }
  // Re-sum the trend by month over the active filter (spend is additive across dims). The
  // %-of-revenue line is recomputed for the SAME filter = Σ(filtered AI cost) / Σ(filtered
  // revenue) per month — a real, meaningful ratio at any dimension (region/location/practice/
  // industry). Both numerator (attribution spine) and denominator (enterprise_metrics) exist
  // at that grain, so the % line stays live under filters instead of dropping out.
  function renderTrend() {
    const f = activeFilters();
    const byMonth = {};
    TREND.filter((r) => dimMatch(r, f)).forEach((r) => {
      const m = (r.month || "").slice(0, 7);
      byMonth[m] = (byMonth[m] || 0) + (r.ai_cost || 0);
    });
    const revByMonth = {};
    TREND_REV.filter((r) => dimMatch(r, f)).forEach((r) => {
      const m = (r.month || "").slice(0, 7);
      revByMonth[m] = (revByMonth[m] || 0) + (r.revenue || 0);
    });
    const labels = Object.keys(byMonth).sort();
    const spend = labels.map((m) => byMonth[m]);
    // % of revenue per month for the active slice (null-guard divide-by-zero).
    const pct = labels.map((m) => (revByMonth[m] > 0 ? +(byMonth[m] / revByMonth[m] * 100).toFixed(3) : null));
    const datasets = [
      { type: "bar", label: "AI Spend", data: spend, backgroundColor: DB.teal, yAxisID: "y", order: 2 },
    ];
    if (pct) datasets.push({ type: "line", label: "% of Revenue", data: pct, borderColor: ACCENT, backgroundColor: ACCENT, borderWidth: 1.5, yAxisID: "y1", tension: 0.3, order: 1, pointRadius: 2 });
    if (charts.trend) charts.trend.destroy();
    charts.trend = new Chart(el("trendChart"), {
      data: { labels, datasets },
      options: {
        maintainAspectRatio: false, interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { labels: { boxWidth: 12, font: { size: 13 } } },
          // House style: colored label text, no color boxes.
          tooltip: {
            displayColors: false,
            callbacks: {
              label: (ctx) => ctx.dataset.label + ": " + (ctx.dataset.yAxisID === "y1" ? ctx.parsed.y.toFixed(2) + "%" : fmtUSD(ctx.parsed.y)),
              labelTextColor: (ctx) => (ctx.dataset.label === "AI Spend" ? DB.teal : ACCENT),
            },
          },
        },
        scales: {
          y: { position: "left", ticks: { callback: (v) => fmtUSD(v), font: { size: 12 } }, grid: { color: "#f0f3f4" } },
          y1: { display: !!pct, position: "right", ticks: { callback: (v) => v.toFixed(2) + "%", font: { size: 12 } }, grid: { drawOnChartArea: false } },
          x: {
            grid: { display: false },
            ticks: {
              font: { size: 12 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 8,
              // "2023-08" → "Aug ’23" — cleaner + fewer ticks so labels don't collide.
              callback: function (val) {
                const raw = this.getLabelForValue(val);           // "YYYY-MM"
                const [y, m] = (raw || "").split("-");
                if (!y || !m) return raw;
                const mon = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][+m - 1] || m;
                return mon + " ’" + y.slice(2);
              },
            },
          },
        },
      },
    });
  }

  // Friendly labels for the real billing_origin_product enum (system-table values).
  // Used by the "by service type" toggle of the unified breakdown chart below.
  const SERVICE_LABELS = {
    MODEL_SERVING: "Model Serving", AI_FUNCTIONS: "AI Functions",
    VECTOR_SEARCH: "Vector Search (AI Search)", GENIE: "Genie / AI-BI",
  };

  // ─── AI Spend breakdown — ONE chart, three toggles (office / model provider / service) ──
  let OFFICE = [];             // spine rows: region × location × provider × service_type
  let officeMode = "office";   // office | model | service
  async function loadOffice() {
    try { OFFICE = await getJSON("/api/ai-cost/by-office"); renderOffice(); }
    catch (e) { console.error("[ai-cost] by-office", e); }
  }
  // Provider brand-ish colors for the "by model type" cut (Meta = OSS grey-teal).
  const PROVIDER_COLORS = { Anthropic: "#d97757", OpenAI: "#10a37f", Google: "#4285f4", Meta: "#8fa3aa", Alibaba: "#ff6a00", Databricks: "#ff5f46", Other: "#c0ccd0" };
  function renderOffice() {
    const f = activeFilters();
    const rows = OFFICE.filter((r) => dimMatch(
      { region: r.region, location: r.location, practice: r.practice, industry: r.industry }, f));
    // group key depends on the active toggle
    const keyOf = (r) => officeMode === "office" ? r.location
                       : officeMode === "model"  ? r.provider
                       : r.service_type;
    const labelOf = (k) => officeMode === "service" ? (SERVICE_LABELS[k] || k) : k;
    const totals = {};
    rows.forEach((r) => { const k = keyOf(r) || "Unattributed"; totals[k] = (totals[k] || 0) + (r.cost || 0); });
    let entries = Object.entries(totals).sort((a, b) => b[1] - a[1]);
    if (officeMode === "office") entries = entries.slice(0, 15);   // top offices only; keep readable
    const labels = entries.map((e) => labelOf(e[0]));

    let datasets, showLegend = false;
    if (officeMode === "model") {
      // MODEL cut: keep every PROVIDER bar, but split each into Databricks-hosted (FMAPI,
      // blue) vs External-via-Gateway (amber) segments — so one chart shows both the maker
      // mix AND where each provider's spend actually runs. cost_leg drives the split.
      const provKeys = entries.map((e) => e[0]);
      const hosted = provKeys.map((p) => rows.filter((r) => (r.provider || "Unattributed") === p && r.cost_leg === "HOSTED").reduce((s, r) => s + (r.cost || 0), 0));
      const external = provKeys.map((p) => rows.filter((r) => (r.provider || "Unattributed") === p && r.cost_leg === "EXTERNAL").reduce((s, r) => s + (r.cost || 0), 0));
      datasets = [
        { label: "Databricks-hosted (FMAPI)", data: hosted, backgroundColor: DB.blue, stack: "s" },
        { label: "External (via Gateway)", data: external, backgroundColor: ACCENT, stack: "s" },
      ];
      showLegend = true;
    } else {
      // office / service = single teal series.
      datasets = [{ label: "AI Spend (TTM)", data: entries.map((e) => e[1]), backgroundColor: DB.teal }];
    }
    if (charts.office) charts.office.destroy();
    charts.office = new Chart(el("officeChart"), {
      type: "bar",
      data: { labels, datasets },
      options: {
        maintainAspectRatio: false, indexAxis: "y",
        plugins: {
          legend: { display: showLegend, labels: { boxWidth: 12, font: { size: 12 } } },
          tooltip: { displayColors: false, callbacks: { label: (c) => c.dataset.label + ": " + fmtUSD(c.parsed.x) } },
        },
        scales: {
          x: { stacked: officeMode === "model", ticks: { callback: (v) => fmtUSD(v), font: { size: 12 } }, grid: { color: "#f0f3f4" } },
          y: { stacked: officeMode === "model", ticks: { font: { size: 12 } } },
        },
      },
    });
  }
  function setOfficeMode(mode, btn) {
    officeMode = mode;
    document.querySelectorAll("#officeToggle button").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    renderOffice();
  }

  // ─── Cost-center budget table ─────────────────────────────────────────────
  let CC = [];              // cached cost-center rows
  let ccExpanded = false;   // "see more" state
  const CC_TOP_N = 5;       // Raj: show top 5, then "see more"
  async function loadCostCenters() {
    try { CC = await getJSON("/api/ai-cost/cost-centers"); renderCostCenters(); }
    catch (e) { console.error("[ai-cost] cost-centers", e); }
  }
  function renderCostCenters() {
    const tb = document.querySelector("#ccTable tbody");
    if (!CC.length) { tb.innerHTML = '<tr><td colspan="8" class="loading">No cost-center budgets found.</td></tr>'; return; }
    const shown = ccExpanded ? CC : CC.slice(0, CC_TOP_N);
    let html = shown.map((r) => {
      const cls = r.status === "OVER_BUDGET" ? "over" : r.status === "AT_RISK" ? "risk" : "ontrack";
      const lbl = (r.status || "").replace("_", " ");
      const payload = { kind: "budget", cost_center: r.cost_center, department: r.department,
        utilization_pct: r.utilization_pct, ttm_cost: r.ttm_cost, budget: r.budget, status: lbl };
      return `<tr><td>${r.cost_center}</td><td>${r.department || ""}</td><td>${r.policy || ""}</td>
        <td class="num">${fmtUSD(r.ttm_cost)}</td><td class="num">${fmtUSD(r.budget)}</td>
        <td class="num">${fmtPct(r.utilization_pct, 0)}</td><td><span class="badge ${cls}">${lbl}</span></td>
        <td><button class="nudge-btn" onclick='AICost.nudge(${JSON.stringify(payload).replace(/'/g, "&#39;")})'>Nudge Dept Head</button></td></tr>`;
    }).join("");
    if (CC.length > CC_TOP_N) {
      const label = ccExpanded ? "Show top " + CC_TOP_N : "See all " + CC.length + " cost centers";
      html += `<tr class="see-more-row"><td colspan="8"><button class="see-more-btn" onclick="AICost.toggleCostCenters()">${label}</button></td></tr>`;
    }
    tb.innerHTML = html;
  }
  function toggleCostCenters() { ccExpanded = !ccExpanded; renderCostCenters(); }

  // ─── Engagements → client table, recoverable table, provider concentration, scatter ─
  async function loadEngagements() {
    try {
      ENG = await getJSON("/api/ai-cost/engagements");
      renderClientChart(); renderRecoverTable(); renderConcentration(); renderScatter();
    } catch (e) { console.error("[ai-cost] engagements", e); }
  }
  function filtered() {
    const rg = el("fRegion").value, lo = el("fLocation").value,
          pr = el("fPractice").value, ind = el("fIndustry").value;
    return ENG.filter((r) =>
      (rg === "All" || r.region === rg) &&
      (lo === "All" || r.location === lo) &&
      (pr === "All" || r.practice === pr) &&
      (ind === "All" || r.industry === ind));
  }
  // Client AI Spend vs Invoice — CHART (top clients by AI spend, with AI-as-%-of-invoice line).
  function renderClientChart() {
    // roll up to client grain
    const byClient = {};
    filtered().forEach((r) => {
      const c = byClient[r.client] || (byClient[r.client] = { ai: 0, rev: 0 });
      c.ai += r.ai_cost || 0; c.rev += r.revenue || 0;
    });
    const clients = Object.entries(byClient).sort((a, b) => b[1].ai - a[1].ai).slice(0, 12);
    const labels = clients.map((c) => c[0]);
    const ai = clients.map((c) => c[1].ai);
    const pct = clients.map((c) => (c[1].rev > 0 ? +(c[1].ai / c[1].rev * 100).toFixed(2) : 0));
    // Bars over the DRAGGABLE threshold render in the orange accent (over-consuming AI vs.
    // billings); under-threshold bars stay teal. Orange-vs-teal avoids the red/green
    // "Christmas" clash. clientThreshold is module-level so a drag persists across re-renders.
    // Bars: BLUE by default, RED when over the threshold. The % line stays amber (ACCENT) —
    // blue/red bars vs amber line reads cleanly (amber-on-amber was mush).
    const recolor = () => pct.map((p) => (p > clientThreshold ? DB.red : DB.blue));
    const thresholdLine = new Array(labels.length).fill(clientThreshold);
    if (charts.client) charts.client.destroy();
    charts.client = new Chart(el("clientChart"), {
      data: {
        labels,
        datasets: [
          { type: "bar", label: "Attributed AI Spend", data: ai, backgroundColor: recolor(), yAxisID: "y", order: 3 },
          { type: "line", label: "AI % of Invoice", data: pct, borderColor: ACCENT, backgroundColor: ACCENT, borderWidth: 1.5, yAxisID: "y1", tension: 0.3, order: 1, pointRadius: 3 },
          // Draggable threshold — neutral dark dashed so it's distinct from the orange %
          // line. Label drawn ABOVE it (thresholdLabelPlugin). Drag via dragThresholdPlugin.
          { type: "line", label: "Threshold", data: thresholdLine, borderColor: DB.ink, borderWidth: 2.5, borderDash: [6, 4], yAxisID: "y1", pointRadius: 0, order: 2 },
        ],
      },
      options: {
        maintainAspectRatio: false, interaction: { mode: "index", intersect: false },
        // Chart.js 3.9 default events omit mousedown/mouseup — dragThresholdPlugin needs them
        // to detect the grab. Include the defaults + those so both hover and drag work.
        events: ["mousemove", "mouseout", "click", "mousedown", "mouseup", "touchstart", "touchmove", "touchend"],
        plugins: {
          legend: { labels: { usePointStyle: true, boxWidth: 22, font: { size: 12 }, generateLabels: lineAwareLegend } },
          // House style: NO color boxes — colored label TEXT. Threshold dropped from the body
          // rows and re-added as plain text via afterBody (consistent with the forecast chart).
          tooltip: {
            displayColors: false,
            bodyColor: "#ffffff",   // afterBody "Threshold: N%" line renders neutral white
            filter: (item) => item.dataset.label !== "Threshold",
            callbacks: {
              label: (ctx) => ctx.dataset.label + ": " + (ctx.dataset.yAxisID === "y1" ? ctx.parsed.y + "%" : fmtUSD(ctx.parsed.y)),
              // Data rows keep their color; Threshold (via afterBody) stays white/neutral.
              labelTextColor: (ctx) => (ctx.dataset.label === "AI % of Invoice"
                ? ACCENT
                : ((ctx.chart.data.datasets[1].data[ctx.dataIndex] > clientThreshold) ? DB.red : DB.blue)),
              afterBody: () => "Threshold: " + clientThreshold.toFixed(0) + "%",
            },
          },
          thresholdLabel: { lines: [{ value: clientThreshold, text: clientThreshold.toFixed(0) + "% of invoice (drag to adjust)", axis: "y1", color: DB.ink }] },
          dragThreshold: {
            datasetIndex: 2, axis: "y1", min: 2, max: 25,
            // Mutate label + bar colors ONLY — no chart.update() here; the drag plugin does a
            // single update() after this returns (avoids the dropped re-entrant update).
            onChange: (v) => {
              clientThreshold = v;
              const ch = charts.client;
              const pctData = ch.data.datasets[1].data;   // the % line's own data
              ch.data.datasets[0].backgroundColor = pctData.map((p) => (p > v ? DB.red : DB.blue));
              ch.options.plugins.thresholdLabel.lines[0].value = v;
              ch.options.plugins.thresholdLabel.lines[0].text = v.toFixed(0) + "% of invoice (drag to adjust)";
            },
          },
        },
        scales: {
          y: { position: "left", ticks: { callback: (v) => fmtUSD(v), font: { size: 12 } }, grid: { color: "#f0f3f4" } },
          y1: { position: "right", min: 0, ticks: { callback: (v) => v + "%", font: { size: 12 } }, grid: { drawOnChartArea: false } },
          x: { ticks: { font: { size: 13 }, maxRotation: 40, minRotation: 40 } },
        },
      },
      plugins: [thresholdLabelPlugin, dragThresholdPlugin],
    });
  }
  function renderRecoverTable() {
    const tb = document.querySelector("#recoverTable tbody");
    const rows = filtered().filter((r) => r.recoverability === "RECOVERABLE" || r.recoverability === "REPRICE_AT_RENEWAL")
      .sort((a, b) => b.ai_cost - a.ai_cost).slice(0, 12);
    tb.innerHTML = rows.length ? rows.map((r) => {
      const cls = r.recoverability === "RECOVERABLE" ? "recoverable" : r.recoverability === "REPRICE_AT_RENEWAL" ? "reprice" : "review";
      const lbl = r.recoverability.replace(/_/g, " ").toLowerCase();
      const payload = { kind: "engagement", client: r.client, project: r.project, ai_cost: r.ai_cost,
        contract_type: r.contract_type, ai_pct_invoice: r.ai_pct_invoice, action: r.action };
      return `<tr><td>${r.client}</td><td>${r.project}</td><td class="num">${fmtUSD(r.ai_cost)}</td>
        <td>${r.contract_type}</td><td><span class="badge ${cls}">${lbl}</span></td>
        <td><button class="nudge-btn" onclick='AICost.nudge(${JSON.stringify(payload).replace(/'/g, "&#39;")})'>${r.action || "Review"}</button></td></tr>`;
    }).join("") : '<tr><td colspan="6" class="loading">No recoverable spend.</td></tr>';
  }
  function renderConcentration() {
    const tb = document.querySelector("#concTable tbody");
    const rows = filtered().filter((r) => r.concentration_flag).sort((a, b) => b.ai_cost - a.ai_cost).slice(0, 10);
    tb.innerHTML = rows.length ? rows.map((r) => {
      const payload = { kind: "provider", client: r.client, project: r.project, dominant_provider: r.dominant_provider,
        pct_one_provider: r.pct_one_provider, ai_cost: r.ai_cost, endpoints: r.endpoints };
      return `<tr><td>${r.client}</td><td>${r.project}</td><td>${r.dominant_provider || ""}</td>
       <td class="num">${fmtPct(r.pct_one_provider, 0)}</td><td class="num">${fmtUSD(r.ai_cost)}</td>
       <td><button class="nudge-btn" onclick='AICost.nudge(${JSON.stringify(payload).replace(/'/g, "&#39;")})'>Set fallbacks &amp; traffic splits</button></td></tr>`;
    }).join("") : '<tr><td colspan="6" class="loading">No provider-concentration risks flagged.</td></tr>';
  }
  const median = (arr) => {
    if (!arr.length) return 0;
    const s = [...arr].sort((a, b) => a - b), m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
  };
  function renderScatter() {
    const rows = filtered().filter((r) => r.ai_per_hour != null && r.margin_pct != null);
    // A handful of engagements have very high AI-$/hr (>$900) that stretch the x-axis and
    // smush the bulk (clustered <$300) against the left. Cap the axis at the 95th percentile
    // so the meaningful cluster is readable; clamp outliers to the edge and flag them (▸) in
    // the tooltip so they're not hidden, just corralled.
    const xsAll = rows.map((r) => r.ai_per_hour).sort((a, b) => a - b);
    const xCap = xsAll.length ? Math.max(300, xsAll[Math.floor(xsAll.length * 0.95)] || 0) : 1000;
    const pts = rows.map((r) => ({
      x: Math.min(r.ai_per_hour, xCap), xRaw: r.ai_per_hour, capped: r.ai_per_hour > xCap,
      y: r.margin_pct, r: Math.max(4, Math.min(24, Math.sqrt((r.tcv || 0) / 1e6) * 2)),
      label: r.client + " · " + r.project,
    }));
    // Quadrant dividers at the MEDIAN AI-$/hr (x) and MEDIAN margin (y) of what's shown —
    // splits the field into four decision zones (per Raj/Dennis). High AI + high margin
    // (top-right) = green (working). High AI + low margin (bottom-right) = red (spending
    // without value — the most concerning). Left half (low AI spend) = yellow (opportunity).
    const xMid = median(pts.map((p) => p.xRaw));
    const yMid = median(pts.map((p) => p.y));
    if (charts.scatter) charts.scatter.destroy();
    charts.scatter = new Chart(el("valueScatter"), {
      type: "bubble",
      data: { datasets: [{ label: "Engagements", data: pts, backgroundColor: "rgba(27,47,54,.55)", borderColor: DB.ink }] },
      options: {
        maintainAspectRatio: false,
        layout: { padding: { top: 6 } },
        plugins: {
          legend: { display: false },
          tooltip: { displayColors: false, callbacks: { label: (c) => c.raw.label + " — $" + c.raw.xRaw.toFixed(2) + "/hr" + (c.raw.capped ? " (off-scale ▸)" : "") + ", " + c.raw.y.toFixed(1) + "% margin" } },
          valueQuadrants: { xMid: Math.min(xMid, xCap), yMid },
        },
        scales: {
          x: { min: 0, max: xCap, title: { display: true, text: "AI spend per delivery hour ($)", font: { size: 13 } }, ticks: { callback: (v) => "$" + v, font: { size: 12 } }, grid: { color: "#f0f3f4" } },
          y: { title: { display: true, text: "Engagement margin (%)", font: { size: 13 } }, ticks: { callback: (v) => v + "%", font: { size: 12 } }, grid: { color: "#f0f3f4" } },
        },
      },
      plugins: [valueQuadrantsPlugin],
    });
  }

  // ─── Forecast: usage-growth-driven projection vs TWO constraint ceilings ──
  const IT_BUDGET_QTR = 1.6e9, BILL_REV_QTR = 6e9;   // illustrative (see method note)
  function updateForecast() {
    const growthPct = +el("grS").value, itPct = +el("itS").value, brPct = +el("brS").value;
    el("grV").textContent = growthPct + "%";
    el("itV").textContent = itPct.toFixed(1) + "%";
    el("brV").textContent = brPct.toFixed(2) + "%";
    // Two independent monthly ceilings; the LOWER binds.
    const itCeil = (IT_BUDGET_QTR / 3) * itPct / 100;
    const brCeil = (BILL_REV_QTR / 3) * brPct / 100;
    const bindingCeil = Math.min(itCeil, brCeil);
    const bindingLabel = itCeil <= brCeil ? "IT budget" : "billable revenue";
    el("tCeil").textContent = fmtUSD(bindingCeil) + "/mo (" + bindingLabel + ")";

    if (!TREND.length) return;
    const HORIZON = 12;   // months projected — long enough that the growth slider visibly bends the line
    // TREND is now per-dimension rows (month × region × …) → aggregate to a firmwide
    // monthly series for the forecast (the forecast is the strategic firmwide view).
    const _byMonth = {};
    TREND.forEach((r) => { const m = (r.month || "").slice(0, 7); _byMonth[m] = (_byMonth[m] || 0) + (r.ai_cost || 0); });
    const labels = Object.keys(_byMonth).sort();
    const hist = labels.map((m) => _byMonth[m]);
    // Forecast grows the last actual by the slider's annual rate (monthly-compounded).
    const g = Math.pow(1 + growthPct / 100, 1 / 12);
    const last = hist[hist.length - 1] || 0;
    const fLabels = [], fVals = []; let cur = last;
    for (let i = 1; i <= HORIZON; i++) { cur *= g; fLabels.push("+" + i + "mo"); fVals.push(cur); }
    const allLabels = labels.concat(fLabels);
    const n = allLabels.length;
    const actualsSeries = hist.concat(new Array(HORIZON).fill(null));
    const forecastSeries = new Array(hist.length - 1).fill(null).concat([last]).concat(fVals);
    const itSeries = new Array(n).fill(itCeil);
    const brSeries = new Array(n).fill(brCeil);
    const breaches = cur > bindingCeil;
    // When it breaches, find the first month it crosses the binding ceiling.
    let breachMo = null;
    for (let i = 0; i < fVals.length; i++) { if (fVals[i] > bindingCeil) { breachMo = i + 1; break; } }
    el("tHead").textContent = breaches
      ? "Breaches ceiling in " + (breachMo || HORIZON) + "mo"
      : fmtUSD(bindingCeil - cur) + " headroom";
    el("tHead").style.color = breaches ? DB.red : DB.green;
    // Which ceiling binds (the tighter/lower one). Both lines are DASHED; the binding one
    // is emphasized by color (orange if breached, green if safe), the non-binding one grey.
    const itBinds = itCeil <= brCeil;
    // Scale the y-axis to fit BOTH ceiling lines + actuals/forecast, so the "triangle of
    // constraints" reads like the wireframe: you see both the IT-budget ceiling and the
    // billable-rev ceiling, and where the forecast crosses the lower (binding) one. The
    // higher ceiling can be far above actuals ($16M vs ~$3.6M) — that's the real story
    // (AI is tiny vs. the IT budget), so we let the actual bars sit lower on the axis.
    const yMax = Math.max(Math.max(...hist), cur, itCeil, brCeil) * 1.1;
    // Secondary right axis = the SAME data expressed as % of the monthly IT budget (the CFO's
    // primary guardrail). It's a linear re-scale of the left ($) axis, so we pin its max to the
    // % that yMax represents → the two axes stay in lockstep and the ceiling lines read as both
    // a $ figure and a % of budget (Raj's "add a right-hand % axis that maps" ask).
    const itBudgetMonthly = IT_BUDGET_QTR / 3;
    const yMaxPct = yMax / itBudgetMonthly * 100;

    if (charts.forecast) charts.forecast.destroy();
    charts.forecast = new Chart(el("forecastChart"), {
      data: {
        labels: allLabels,
        datasets: [
          { type: "bar", label: "Actual", data: actualsSeries, backgroundColor: DB.teal, order: 4 },
          // Forecast = cool BLUE so it never collides with the ceiling's warm states
          // (grey / green=safe / red=breached). Distinct in every scenario.
          { type: "line", label: "Forecast", data: forecastSeries, borderColor: DB.blue, borderDash: [5, 4], borderWidth: 2, tension: 0.3, pointRadius: 0, order: 3 },
          // BOTH ceilings dashed (consistency). The binding one is emphasized by color
          // (orange if breached, green if safe); the non-binding one is grey. Each is
          // labeled directly on the plot via thresholdLabelPlugin below.
          { type: "line", label: "IT-budget ceiling", data: itSeries, borderColor: itBinds ? (breaches ? DB.red : DB.green) : "#9aa7b0", borderWidth: 1.5, borderDash: [6, 4], pointRadius: 0, order: itBinds ? 1 : 2 },
          { type: "line", label: "Billable-rev ceiling", data: brSeries, borderColor: !itBinds ? (breaches ? DB.red : DB.green) : "#9aa7b0", borderWidth: 1.5, borderDash: [6, 4], pointRadius: 0, order: !itBinds ? 1 : 2 },
        ],
      },
      options: {
        maintainAspectRatio: false, interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { labels: { usePointStyle: true, boxWidth: 22, font: { size: 13 }, generateLabels: lineAwareLegend } },
          // Consistent house style: NO color boxes — the label TEXT is colored to match its
          // line/series. Avoids the box-with-dashed-outline / stray-circle confusion.
          tooltip: {
            displayColors: false,
            callbacks: {
              label: (ctx) => ctx.dataset.label + ": " + fmtUSD(ctx.parsed.y),
              labelTextColor: (ctx) => {
                const L = ctx.dataset.label;
                if (L === "Actual") return DB.teal;
                if (L === "Forecast") return DB.blue;
                if (L === "IT-budget ceiling") return itBinds ? (breaches ? DB.red : DB.green) : "#9aa7b0";
                if (L === "Billable-rev ceiling") return !itBinds ? (breaches ? DB.red : DB.green) : "#9aa7b0";
                return "#ffffff";
              },
            },
          },
          thresholdLabel: { lines: [
            { value: itCeil, text: "IT-budget ceiling", axis: "y", color: itBinds ? (breaches ? DB.red : DB.green) : "#8fa3aa" },
            { value: brCeil, text: "Billable-rev ceiling", axis: "y", color: !itBinds ? (breaches ? DB.red : DB.green) : "#8fa3aa" },
          ] },
        },
        scales: {
          y: { position: "left", max: yMax, ticks: { callback: (v) => fmtUSD(v), font: { size: 12 } }, grid: { color: "#f0f3f4" }, title: { display: true, text: "AI spend ($/mo)", font: { size: 12 } } },
          yPct: { display: true, position: "right", max: yMaxPct, min: 0, ticks: { callback: (v) => v.toFixed(1) + "%", font: { size: 12 } }, grid: { drawOnChartArea: false }, title: { display: true, text: "% of IT budget", font: { size: 12 } } },
          x: {
            grid: { display: false },
            ticks: {
              font: { size: 12 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 9,
              callback: function (val) {
                const raw = this.getLabelForValue(val);
                if (!raw || raw.startsWith("+")) return raw;   // "+3mo" forecast labels pass through
                const [y, m] = raw.split("-");
                if (!y || !m) return raw;
                const mon = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][+m - 1] || m;
                return mon + " ’" + y.slice(2);
              },
            },
          },
        },
      },
      plugins: [thresholdLabelPlugin],
    });
  }

  // ─── Nudge → open the SAME email modal the Finance/Admin tabs use ─────────
  // Delegates to openAICostEmail() (defined in ai_cost.html's scripts block, copied
  // from the admin.html email-modal implementation) so the UX is identical.
  function nudge(row) {
    if (typeof window.openAICostEmail === "function") {
      window.openAICostEmail(row);
    } else {
      console.error("[ai-cost] email modal not loaded");
    }
  }

  // ─── Genie chips → open the SAME chat modal the other tabs use ────────────
  // askGenie() lives in modal.js (streaming chat + markdown rendering).
  function askGenie(chip) {
    const q = chip.textContent.trim();
    if (typeof window.askGenie === "function") {
      window.askGenie(q);
    } else {
      console.error("[ai-cost] Genie chat modal (modal.js) not loaded");
    }
  }

  // ─── Filters ──────────────────────────────────────────────────────────────
  const FILTER_IDS = ["fRegion", "fLocation", "fPractice", "fIndustry"];
  const FILTER_KEY = { fRegion: "region", fLocation: "location", fPractice: "practice", fIndustry: "industry" };
  // Parent chain: each filter's options are constrained by the selections ABOVE it, so you
  // can't pick a Location outside the chosen Region, etc. (region → location → practice → industry).
  const FILTER_PARENTS = { fRegion: [], fLocation: ["fRegion"], fPractice: ["fRegion", "fLocation"], fIndustry: ["fRegion", "fLocation", "fPractice"] };

  // (Re)build each dropdown's option list from the rows still consistent with its PARENT
  // selections, preserving the current value if still valid (else reset to All). Rebuilds the
  // custom-dropdown widget so the narrowed list shows. Called on load AND on every change.
  function cascadeFilters() {
    FILTER_IDS.forEach((id) => {
      const sel = el(id); if (!sel) return;
      const key = FILTER_KEY[id];
      // rows matching all PARENT filters (not this one or its children)
      const parentSel = {};
      FILTER_PARENTS[id].forEach((pid) => { parentSel[FILTER_KEY[pid]] = el(pid).value; });
      const opts = [...new Set(ENG
        .filter((r) => Object.entries(parentSel).every(([k, v]) => v === "All" || r[k] === v))
        .map((r) => r[key]).filter(Boolean))].sort();
      const prev = sel.value;
      const keep = prev !== "All" && opts.includes(prev);
      sel.innerHTML = "";
      const allOpt = document.createElement("option"); allOpt.value = "All"; allOpt.textContent = "All"; sel.appendChild(allOpt);
      opts.forEach((v) => { const o = document.createElement("option"); o.value = v; o.textContent = v; sel.appendChild(o); });
      sel.value = keep ? prev : "All";
      // rebuild the custom-dropdown widget for this select
      const w = sel.parentNode.querySelector(".custom-dropdown");
      if (w) w.remove();
      sel.style.display = "";
    });
    if (typeof window.initializeCustomDropdowns === "function") window.initializeCustomDropdowns();
  }
  function populateFilters() { cascadeFilters(); }
  // Re-render the WHOLE page from the active filter — all client-side, instant, no re-fetch.
  // Cascade first so dependent dropdowns narrow (and any now-invalid child selection resets).
  function applyFilters() {
    cascadeFilters();
    renderKPIs(); renderTrend(); renderOffice();
    renderClientChart(); renderRecoverTable(); renderConcentration(); renderScatter();
  }
  function clearFilters() {
    FILTER_IDS.forEach((id) => {
      const sel = el(id); if (!sel) return;
      sel.value = "All";
      // reflect in the custom-dropdown widget if present
      const cd = window.customDropdowns && window.customDropdowns[id];
      const txt = sel.parentNode.querySelector(".custom-dropdown-text");
      if (txt) txt.textContent = "All";
      if (cd && typeof cd.updateOptions === "function") cd.updateOptions();
    });
    applyFilters();
  }

  // ─── Value Lens toggle: Client delivery (real) ↔ Productization (illustrative) ──
  function setValueMode(mode, btn) {
    document.querySelectorAll("#valueToggle button").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    el("valueDelivery").style.display = mode === "delivery" ? "" : "none";
    el("valueProductization").style.display = mode === "productization" ? "" : "none";
    if (mode === "productization" && !charts.prod) renderProductization();
  }
  // ILLUSTRATIVE view — AI build INVESTMENT vs. the REVENUE each productized offering
  // earns → the ROI read: "revenue per $ of AI build spend". Not derived from the data
  // model (no offering dimension) — hardcoded per the launch-call decision that this lens
  // is intentionally hand-wavy. Figures reconcile to the wireframe (6 offerings, ~$148M
  // new-offering revenue, ~$1.8M attributed AI build spend). Association, not causation.
  function renderProductization() {
    // [offering, revenue $M, AI build spend $K] — build spend intentionally NOT proportional
    // to revenue, so ROI (revenue per $ of build spend) VARIES across offerings. That
    // dispersion is the CFO insight: the biggest-revenue offering isn't the most capital-
    // efficient AI investment. Total build spend ≈ $1.8M, reconciling to the wireframe.
    const rows = [
      ["GenAI Accelerators", 52, 780],
      ["Data Platform", 38, 260],
      ["Industry Solutions", 28, 440],
      ["Managed AI Ops", 16, 150],
      ["Risk & Compliance Suite", 9, 110],
      ["Advisory Tools", 5, 70],
    ].sort((a, b) => b[1] - a[1]);   // sort by revenue (bars descend); ROI line will zig-zag
    const lines = rows.map((r) => r[0]);
    const revenue = rows.map((r) => r[1]);          // $M
    const buildK = rows.map((r) => r[2]);           // $K
    // ROI multiple = revenue $ ÷ build spend $ (unitless "$ revenue per $1 AI build spend")
    const roi = rows.map((r) => (r[1] * 1e6) / (r[2] * 1e3));
    // color each ROI point green/amber/red by efficiency vs the firmwide blended ratio,
    // so "efficient vs. over-invested" reads at a glance (not just the zig-zag).
    const blended = (revenue.reduce((s, v) => s + v, 0) * 1e6) / (buildK.reduce((s, v) => s + v, 0) * 1e3);
    const roiColors = roi.map((x) => (x >= blended * 1.15 ? DB.green : x <= blended * 0.85 ? DB.red : DB.amber));
    charts.prod = new Chart(el("prodChart"), {
      data: {
        labels: lines,
        datasets: [
          // Bars = offering revenue → BLUE (frees green for the ROI-point coding below).
          { type: "bar", label: "Offering revenue ($M)", data: revenue, backgroundColor: DB.blue, yAxisID: "y", order: 2 },
          // ROI line → neutral gray/black so the RED/AMBER/GREEN point coding carries the
          // meaning without a grating colored line. Points colored by efficiency vs firm avg.
          { type: "line", label: "Return on AI build spend (points colored by efficiency)", data: roi, borderColor: DB.ink, backgroundColor: roiColors, pointBackgroundColor: roiColors, pointBorderColor: roiColors, borderWidth: 1.5, yAxisID: "y1", tension: 0.25, pointRadius: 6, order: 1 },
        ],
      },
      options: {
        maintainAspectRatio: false, interaction: { mode: "index", intersect: false },
        plugins: {
          // Custom legend: real datasets + 3 point-style entries explaining the circle colors.
          legend: { labels: { usePointStyle: true, boxWidth: 12, font: { size: 12.5 }, generateLabels: (ch) => [
            { text: "Offering revenue ($M)", fillStyle: DB.blue, strokeStyle: DB.blue, pointStyle: "rect" },
            { text: "Above firm avg return", fillStyle: DB.green, strokeStyle: DB.green, pointStyle: "circle" },
            { text: "Near firm avg", fillStyle: DB.amber, strokeStyle: DB.amber, pointStyle: "circle" },
            { text: "Below firm avg (over-invested)", fillStyle: DB.red, strokeStyle: DB.red, pointStyle: "circle" },
          ] } },
          tooltip: { callbacks: {
            afterBody: (items) => {
              const i = items[0].dataIndex;
              return "\nAI build spend: $" + buildK[i] + "K\nReturn: $" + roi[i].toFixed(0) + " revenue per $1 AI build spend"
                + (roi[i] >= blended ? "  ✓ above firm avg" : "  ▼ below firm avg");
            },
          } },
        },
        scales: {
          y: { position: "left", title: { display: true, text: "Offering revenue ($M)", font: { size: 12 } }, ticks: { callback: (v) => "$" + v + "M", font: { size: 12 } }, grid: { color: "#f0f3f4" } },
          y1: { position: "right", min: 0, title: { display: true, text: "Revenue per $1 AI build spend", font: { size: 12 } }, ticks: { callback: (v) => "$" + v, font: { size: 12 } }, grid: { drawOnChartArea: false } },
          x: { ticks: { font: { size: 11 } } },
        },
      },
    });
  }

  window.AICost = { setOfficeMode, setValueMode, updateForecast, nudge, askGenie, applyFilters, clearFilters, toggleCostCenters };

  document.addEventListener("DOMContentLoaded", async () => {
    await Promise.all([loadKPIs(), loadTrend(), loadOffice(), loadCostCenters(), loadEngagements()]);
    populateFilters();
    updateForecast();
  });
})();
