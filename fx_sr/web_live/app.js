const state = {
  summary: {},
  pairs: {},
  signals: [],
  positions: [],
  alerts: [],
  executions: [],
  log: [],
};

let socket = null;
let reconnectTimer = null;
let countdownTimer = null;
let executionTogglePending = false;
let selectedExecutionKey = null;
let selectedExecutionChart = null;
let selectedExecutionSeries = null;
let selectedExecutionLines = [];
let transactionBeepState = {
  nextHourBucket: null,
  milestones: {
    600: false,
    300: false,
    120: false,
  },
};
let transactionAudioContext = null;
let isTransactionBeeping = false;
let previousBacktestStatus = null;
let rerunBacktestBtn = null;

const els = {
  connectionPill: document.getElementById("connection-pill"),
  scanStatus: document.getElementById("scan-status"),
  scanProgress: document.getElementById("scan-progress"),
  signalCount: document.getElementById("signal-count"),
  pendingCount: document.getElementById("pending-count"),
  positionCount: document.getElementById("position-count"),
  executionMode: document.getElementById("execution-mode"),
  nextTransactionTimer: document.getElementById("next-transaction-timer"),
  nextTransactionAt: document.getElementById("next-transaction-at"),
  tradeToggleBtn: document.getElementById("trade-toggle-btn"),
  sizingSummary: document.getElementById("sizing-summary"),
  strategyLabel: document.getElementById("strategy-label"),
  watchlistBody: document.getElementById("watchlist-body"),
  signalsList: document.getElementById("signals-list"),
  positionsList: document.getElementById("positions-list"),
  alertsList: document.getElementById("alerts-list"),
  executionsList: document.getElementById("executions-list"),
  logList: document.getElementById("log-list"),
};
const PRICE_DISPLAY_DECIMALS = 5;
const FILL_ENDPOINT_PATHS = [
  "/api/fill",
  "/api/fill/",
  "/api/fill-cache",
  "/fill",
  "/fill/",
  "/fill-cache",
  "/fill_cache",
];
const BACKTEST_RERUN_ENDPOINT_PATHS = [
  "/api/backtest-rerun",
  "/backtest-rerun",
  "/api/backtest-rerun/",
  "/backtest-rerun/",
];

function buildEndpointCandidates(rawPaths) {
  const candidates = Array.from(new Set(rawPaths.map((path) => `/${String(path).replace(/^\/+/, "")}`)));
  const currentPath = window.location.pathname || "/";
  const directory = currentPath.endsWith("/") ? currentPath : currentPath.replace(/[^/]*$/, "");
  const relativeDir = directory || "/";
  const normalisedRelative = relativeDir.endsWith("/") ? relativeDir : `${relativeDir}/`;

  const allCandidates = [...candidates];
  for (const directoryPrefix of [normalisedRelative, "/"]) {
    const prefix = directoryPrefix === "/" ? "" : directoryPrefix;
    for (const rawPath of candidates) {
      const trimmedPath = String(rawPath).replace(/^\/+/, "");
      if (!trimmedPath) {
        continue;
      }
      allCandidates.push(`${prefix}${trimmedPath}`);
    }
  }

  return [...new Set(allCandidates)];
}

async function postToFirstAvailableEndpoint(endpoints, options = {}) {
  let response = null;
  let payload = {};

  const requestOptions = {
    method: "POST",
    ...options,
  };

  for (const endpoint of endpoints) {
    const attemptResponse = await fetch(endpoint, requestOptions);
    const attemptPayload = await attemptResponse.json().catch(() => ({}));
    response = attemptResponse;
    payload = attemptPayload;
    if (attemptResponse.ok || attemptResponse.status !== 404) {
      return { response, payload };
    }
  }

  return { response, payload };
}

async function invokeDashboardAction({
  button,
  confirmMessage,
  endpointPaths,
  loadingText,
  resetText = null,
  requestOptions = {},
  successMessage,
  successLogLevel = "success",
  errorTitle,
  statusErrorHandlers = {},
  onSuccess,
  onFinally,
}) {
  if (!button) {
    return { initiated: false };
  }
  if (!confirm(confirmMessage)) {
    return { initiated: false };
  }

  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = loadingText;

  try {
    const endpoints = buildEndpointCandidates(endpointPaths);
    const { response, payload } = await postToFirstAvailableEndpoint(endpoints, requestOptions);
    if (!response.ok) {
      const baseMessage = payload?.message || payload?.error || `HTTP ${response.status}`;
      const handler = statusErrorHandlers[response.status] || statusErrorHandlers[String(response.status)];
      const message = handler ? handler(baseMessage, response, payload) : baseMessage;
      throw new Error(message);
    }

    const finalMessage = payload?.message || successMessage;
    if (finalMessage) {
      if (typeof onSuccess === "function") {
        await onSuccess({ response, payload, message: finalMessage });
      } else {
        pushLog({
          ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
          level: successLogLevel,
          message: finalMessage,
        });
      }
    }

    return { response, payload, initiated: true };
  } catch (error) {
    const message = error?.message || "Request failed.";
    pushLog({
      ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      level: "error",
      message,
    });
    if (errorTitle) {
      window.alert(`${errorTitle}: ${message}`);
    }
    return { initiated: false, message };
  } finally {
    button.disabled = false;
    if (typeof resetText === "string") {
      button.textContent = resetText;
    } else if (resetText === null) {
      button.textContent = originalText;
    }
    if (typeof onFinally === "function") {
      await onFinally();
    }
  }
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatNumber(value, digits = PRICE_DISPLAY_DECIMALS) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "–";
  }
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatSigned(value, digits = 1, suffix = "") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "–";
  }
  const number = Number(value);
  const prefix = number > 0 ? "+" : "";
  return `${prefix}${formatNumber(number, digits)}${suffix}`;
}

function formatRelativeCountdown(isoString) {
  if (!isoString) {
    return "Waiting";
  }
  const remaining = new Date(isoString).getTime() - Date.now();
  if (remaining <= 0) {
    return "Due now";
  }
  const totalSeconds = Math.floor(remaining / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function getNextTopOfHour(now = new Date()) {
  const nextHour = new Date(now.getTime());
  nextHour.setHours(nextHour.getHours() + 1);
  nextHour.setMinutes(0, 0, 0);
  return nextHour;
}

function ensureTransactionAudioContext() {
  if (transactionAudioContext) {
    return transactionAudioContext;
  }
  const AudioContext = window.AudioContext || window.webkitAudioContext;
  if (!AudioContext) {
    return null;
  }
  transactionAudioContext = new AudioContext();
  return transactionAudioContext;
}

async function beepOnce() {
  const ctx = ensureTransactionAudioContext();
  if (!ctx) {
    return;
  }
  try {
    if (ctx.state === "suspended") {
      await ctx.resume();
    }
  } catch {
    return;
  }

  const start = ctx.currentTime;
  const osc = ctx.createOscillator();
  const gain = ctx.createGain();
  osc.type = "sine";
  osc.frequency.value = 880;
  gain.gain.setValueAtTime(0.0001, start);
  gain.gain.exponentialRampToValueAtTime(0.2, start + 0.02);
  gain.gain.exponentialRampToValueAtTime(0.0001, start + 0.18);
  osc.connect(gain);
  gain.connect(ctx.destination);
  osc.start(start);
  osc.stop(start + 0.16);
}

async function beepCount(count) {
  for (let i = 0; i < count; i += 1) {
    await beepOnce();
    if (i + 1 < count) {
      await new Promise((resolve) => setTimeout(resolve, 180));
    }
  }
}

function updateTransactionBeep(remainingSeconds) {
  if (isTransactionBeeping) {
    return;
  }

  const triggerBeep = (threshold, count) => {
    if (!transactionBeepState.milestones[threshold] && remainingSeconds <= threshold) {
      transactionBeepState.milestones[threshold] = true;
      isTransactionBeeping = true;
      beepCount(count)
        .then(() => {
          isTransactionBeeping = false;
        })
        .catch(() => {
          isTransactionBeeping = false;
        });
    }
  };

  triggerBeep(600, 1);
  triggerBeep(300, 2);
  triggerBeep(120, 3);
}

function renderTransactionCountdown() {
  if (!els.nextTransactionTimer || !els.nextTransactionAt) {
    return;
  }
  const now = new Date();
  const nextHour = getNextTopOfHour(now);
  const nextHourBucket = `${nextHour.getFullYear()}-${nextHour.getMonth() + 1}-${nextHour.getDate()}-${nextHour.getHours()}`;
  if (transactionBeepState.nextHourBucket !== nextHourBucket) {
    transactionBeepState = {
      nextHourBucket,
      milestones: {
        600: false,
        300: false,
        120: false,
      },
    };
  }

  const remainingSeconds = Math.max(0, Math.floor((nextHour - now) / 1000));
  const minutes = Math.floor(remainingSeconds / 60);
  const seconds = remainingSeconds % 60;
  const countdown = `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  const nextLabel = nextHour.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

  els.nextTransactionTimer.textContent = countdown;
  els.nextTransactionAt.textContent = `at ${nextLabel}`;
  updateTransactionBeep(remainingSeconds);
}

function formatTimestamp(isoString) {
  if (!isoString) {
    return "–";
  }
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) {
    return "–";
  }
  return date.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatDateOnly(isoString) {
  if (!isoString) {
    return "–";
  }
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) {
    return "–";
  }
  return date.toLocaleDateString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
  });
}

function executionKey(execution) {
  return [
    execution.pair || "",
    execution.direction || "",
    execution.order_id || "",
    execution.time || "",
  ].join("|");
}

function replayDateForExecution(execution) {
  if (!execution?.time) {
    return "";
  }
  const date = new Date(execution.time);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toISOString().slice(0, 10);
}

function destroySelectedExecutionChart() {
  selectedExecutionLines = [];
  selectedExecutionSeries = null;
  if (selectedExecutionChart) {
    selectedExecutionChart.remove();
    selectedExecutionChart = null;
  }
}

function formatExecutionPrice(value, execution) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "–";
  }
  const digits = execution?.pair?.includes("JPY") ? 3 : PRICE_DISPLAY_DECIMALS;
  return formatNumber(value, digits);
}

function createExecutionPriceLine(price, color, title) {
  if (!selectedExecutionSeries || price === null || price === undefined || Number.isNaN(Number(price))) {
    return;
  }
  const line = selectedExecutionSeries.createPriceLine({
    price: Number(price),
    color,
    lineWidth: 2,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    axisLabelVisible: true,
    title,
  });
  selectedExecutionLines.push(line);
}

async function renderSelectedExecutionChart() {
  destroySelectedExecutionChart();

  if (!selectedExecutionKey) {
    return;
  }
  const execution = [...state.executions].reverse().find((item) => executionKey(item) === selectedExecutionKey);
  const chartEl = document.getElementById("selected-execution-chart");
  const statusEl = document.getElementById("selected-execution-chart-status");
  if (!execution || !chartEl || !statusEl || typeof LightweightCharts === "undefined") {
    return;
  }

  const replayDate = replayDateForExecution(execution);
  if (!replayDate) {
    statusEl.textContent = "No valid replay date for this transaction.";
    return;
  }

  statusEl.textContent = `Loading ${execution.pair} ${replayDate}…`;
  const startIso = `${replayDate}T00:00:00Z`;
  const endIso = `${replayDate}T23:59:59Z`;

  try {
    const params = new URLSearchParams({
      pair: execution.pair,
      tf: "1h",
      start: startIso,
      end: endIso,
    });
    const res = await fetch(`/api/replay/bars?${params.toString()}`);
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || "Failed to load replay bars.");
    }

    const bars = (data.bars || []).map((bar) => ({
      time: Math.floor(new Date(bar.time).getTime() / 1000),
      open: Number(bar.open),
      high: Number(bar.high),
      low: Number(bar.low),
      close: Number(bar.close),
    })).filter((bar) => Number.isFinite(bar.time));

    if (!bars.length) {
      statusEl.textContent = "No replay bars found for that date.";
      return;
    }

    selectedExecutionChart = LightweightCharts.createChart(chartEl, {
      layout: {
        background: { type: "solid", color: "#fffaf2" },
        textColor: "#5b4b3a",
      },
      grid: {
        vertLines: { color: "rgba(91, 75, 58, 0.08)" },
        horzLines: { color: "rgba(91, 75, 58, 0.08)" },
      },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: "rgba(91, 75, 58, 0.18)" },
      timeScale: {
        borderColor: "rgba(91, 75, 58, 0.18)",
        timeVisible: true,
        secondsVisible: false,
      },
      width: chartEl.clientWidth || 520,
      height: 220,
    });

    selectedExecutionSeries = selectedExecutionChart.addCandlestickSeries({
      upColor: "#1f7a49",
      downColor: "#b23b29",
      borderUpColor: "#1f7a49",
      borderDownColor: "#b23b29",
      wickUpColor: "#1f7a49",
      wickDownColor: "#b23b29",
    });
    selectedExecutionSeries.setData(bars);
    selectedExecutionChart.timeScale().fitContent();

    createExecutionPriceLine(execution.submitted_entry_price, "#456b8c", "Entry");
    createExecutionPriceLine(execution.submitted_sl_price, "#b23b29", "SL");
    createExecutionPriceLine(execution.submitted_tp_price, "#1f7a49", "TP");

    statusEl.textContent = `${execution.pair} replay for ${replayDate}`;
  } catch (error) {
    statusEl.textContent = error.message || "Failed to load replay bars.";
  }
}

function badgeClass(value) {
  const token = String(value || "muted").toLowerCase().replaceAll(/[^a-z0-9]+/g, "-");
  return `pill pill-${token}`;
}

function renderBadge(value, label = value) {
  return `<span class="${badgeClass(value)}">${escapeHtml(label || "–")}</span>`;
}

function levelTone(level) {
  const token = String(level || "info").toLowerCase();
  if (token === "success") return "tone-success";
  if (token === "warning") return "tone-warning";
  if (token === "error") return "tone-error";
  if (token === "muted") return "tone-muted";
  return "tone-info";
}

function formatBacktestButtonText(backtest) {
  const status = String(backtest.status || "idle");
  if (status === "starting") {
    const total = Number(backtest.items_requested || 0);
    const processed = Number(backtest.items_processed || 0);
    if (total > 0) {
      const pct = total > 0 ? Math.round((processed / total) * 100) : 0;
      return `Re-running ${processed}/${total} (${pct}%)`;
    }
    return "Starting…";
  }
  if (status === "running") {
    const total = Number(backtest.items_requested || 0);
    const processed = Number(backtest.items_processed || 0);
    const pct = total > 0 ? Math.round((processed / total) * 100) : 0;
    return `Re-running ${processed}/${total} (${pct}%)`;
  }
  return "Re-run Backtest";
}

function updateRerunBacktestButton() {
  if (!rerunBacktestBtn) {
    return;
  }
  const backtest = (state.summary || {}).backtest || {};
  const status = String(backtest.status || "idle");

  if (previousBacktestStatus !== status) {
    if ((previousBacktestStatus === "starting" || previousBacktestStatus === "running") && status === "complete") {
      pushLog({
        ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        level: "success",
        message: "Backtest rerun completed.",
      });
    } else if (
      (previousBacktestStatus === "starting" || previousBacktestStatus === "running")
      && (status === "error" || status === "canceled")
    ) {
      pushLog({
        ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        level: "warning",
        message: `Backtest rerun ${status}.`,
      });
    }
  }

  rerunBacktestBtn.textContent = formatBacktestButtonText(backtest);
  rerunBacktestBtn.disabled = status === "starting" || status === "running";

  previousBacktestStatus = status;
}

function sortPairs() {
  const positionPairs = new Set(state.positions.map((position) => position.pair));
  const priority = {
    LONG: 0,
    SHORT: 0,
    OPEN: 1,
    PARTIAL: 2,
    NEAR: 3,
    WATCH: 4,
    INSIDE: 5,
    WAIT: 6,
    "NO DATA": 7,
  };

  function nearestZoneDist(row) {
    const s = row.support_dist_pct ?? Infinity;
    const r = row.resistance_dist_pct ?? Infinity;
    return Math.min(s, r);
  }

  return Object.values(state.pairs).sort((left, right) => {
    // 1. Signals first
    const leftSignal = left.signal ? 1 : 0;
    const rightSignal = right.signal ? 1 : 0;
    if (leftSignal !== rightSignal) {
      return rightSignal - leftSignal;
    }

    // 2. Open positions
    const leftPosition = positionPairs.has(left.pair) ? 1 : 0;
    const rightPosition = positionPairs.has(right.pair) ? 1 : 0;
    if (leftPosition !== rightPosition) {
      return rightPosition - leftPosition;
    }

    // 3. Closest to support/resistance
    const leftDist = nearestZoneDist(left);
    const rightDist = nearestZoneDist(right);
    if (leftDist !== rightDist) {
      return leftDist - rightDist;
    }

    return left.pair.localeCompare(right.pair);
  });
}

function applyState(nextState) {
  state.summary = nextState.summary || {};
  state.pairs = nextState.pairs || {};
  state.signals = nextState.signals || [];
  state.positions = nextState.positions || [];
  state.alerts = nextState.alerts || [];
  state.executions = nextState.executions || [];
  state.log = nextState.log || [];
  renderAll();
  updateRerunBacktestButton();
}

function upsertPair(row, summary) {
  const sizePlanBySignal = new Map(
    state.signals.map((signal) => [`${signal.pair}:${signal.direction}`, signal.size_plan || null]),
  );
  state.pairs[row.pair] = row;
  if (summary) {
    state.summary = summary;
  }
  state.signals = Object.values(state.pairs)
    .filter((pair) => pair.signal)
    .map((pair) => ({
      ...pair.signal,
      size_plan: sizePlanBySignal.get(`${pair.signal.pair}:${pair.signal.direction}`) ?? pair.signal.size_plan ?? null,
    }));
  renderSummary();
  renderWatchlist();
  renderSignals();
}

function pushLog(entry) {
  state.log = [...state.log, entry].slice(-80);
  renderLog();
}

function renderSummary() {
  const summary = state.summary || {};
  const backfill = summary.backfill || {};
  const isBackfilling = summary.status === "backfilling" && backfill.phase && backfill.phase !== "done";

  // Scan state label
  if (isBackfilling) {
    els.scanStatus.textContent = "BACKFILLING";
  } else if (summary.status === "live") {
    els.scanStatus.textContent = "LIVE";
  } else {
    els.scanStatus.textContent = String(summary.status || "starting").toUpperCase();
  }

  els.signalCount.textContent = String(summary.signal_count || 0);
  els.pendingCount.textContent = `${summary.pending_count || 0} pending blockers`;
  els.positionCount.textContent = String(summary.position_count || 0);

  // Execution mode with trading-paused indicator during backfill
  if (isBackfilling) {
    els.executionMode.textContent = "Trading paused (backfilling)";
  } else if (summary.execution_available && summary.execution_paused) {
    els.executionMode.textContent = "Paper execution paused";
  } else {
    els.executionMode.textContent = summary.execution_enabled ? "Paper execution active" : "Scan only";
  }

  if (els.tradeToggleBtn) {
    if (!summary.execution_available) {
      els.tradeToggleBtn.hidden = true;
    } else {
      const paused = Boolean(summary.execution_paused);
      els.tradeToggleBtn.hidden = false;
      els.tradeToggleBtn.disabled = executionTogglePending;
      els.tradeToggleBtn.textContent = executionTogglePending
        ? (paused ? "Resuming..." : "Pausing...")
        : (paused ? "Resume Entries" : "Pause Entries");
      els.tradeToggleBtn.classList.toggle("is-paused", paused);
    }
  }

  // Balance display
  if (summary.balance != null && summary.account_currency) {
    const bal = Number(summary.balance).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const risk = summary.risk_pct != null ? ` · ${Number(summary.risk_pct).toFixed(1)}% risk` : "";
    els.sizingSummary.textContent = `${summary.account_currency} ${bal}${risk}`;
  } else {
    els.sizingSummary.textContent = "Resolving";
  }

  els.strategyLabel.textContent = `${summary.strategy_label || "Strategy"} · ${summary.mode || "scanner"}`;

  // Connection pill
  if (isBackfilling) {
    els.connectionPill.className = "pill pill-connecting";
    els.connectionPill.textContent = "Backfilling";
  } else if (summary.status === "live") {
    els.connectionPill.className = "pill pill-live";
    els.connectionPill.textContent = "Live";
  } else if (summary.status === "error") {
    els.connectionPill.className = "pill pill-disconnected";
    els.connectionPill.textContent = "Error";
  } else {
    els.connectionPill.className = "pill pill-muted";
    els.connectionPill.textContent = summary.status || "Starting";
  }

  renderTransactionCountdown();
  renderScanProgress();
}

function renderScanProgress() {
  const summary = state.summary || {};
  const backfill = summary.backfill || {};
  const fill = summary.fill || {};
  const backtest = summary.backtest || {};
  renderTransactionCountdown();

  if (fill.status && fill.status !== "idle") {
    const total = Number(fill.items_requested || 0);
    const processed = Number(fill.items_processed || 0);
    const attempts = Number(fill.attempts || 0);
    const errors = Number(fill.errors || 0);
    const pct = total > 0 ? Math.round((processed / total) * 100) : 0;
    const current = fill.current_item ? ` ${String.fromCharCode(0x2022)} ${fill.current_item}` : "";
    const statusLabel = fill.status === "running" ? "Fill" : `Fill ${fill.status}`;
    const attemptsText = attempts > 0 ? ` ${String.fromCharCode(0x2022)} attempts ${attempts}` : "";
    const errorsText = errors > 0 ? ` ${String.fromCharCode(0x2022)} errors ${errors}` : "";
    els.scanProgress.textContent = `${statusLabel}: ${processed} of ${total} (${pct}%)${attemptsText}${errorsText}${current}`;
    return;
  }
  if (backtest.status && backtest.status !== "idle" && backtest.status !== "complete") {
    const total = Number(backtest.items_requested || 0);
    const processed = Number(backtest.items_processed || 0);
    const pct = total > 0 ? Math.round((processed / total) * 100) : 0;
    const current = backtest.current_item ? ` ${String.fromCharCode(0x2022)} ${backtest.current_item}` : "";
    const statusLabel = backtest.status === "starting" ? "Backtest" : (backtest.status === "running" ? "Backtest" : `Backtest ${backtest.status}`);
    els.scanProgress.textContent = `${statusLabel}: ${processed} of ${total} (${pct}%)${current}`;
    return;
  }

  if (summary.status === "backfilling" && backfill.phase && backfill.phase !== "done") {
    const phase = backfill.phase === "zones" ? "Loading zones" : backfill.phase === "hourly" ? "Loading hourly" : "Scanning";
    const pct = backfill.total > 0 ? Math.round((backfill.completed / backfill.total) * 100) : 0;
    const current = backfill.current_pair ? ` · ${backfill.current_pair}` : "";
    els.scanProgress.textContent = `${phase}: ${backfill.completed}/${backfill.total} (${pct}%)${current}`;
    return;
  }

  const pairsText = `${summary.pairs_completed || 0} / ${summary.pairs_total || 0} pairs`;
  if (summary.status === "scanning") {
    els.scanProgress.textContent = pairsText;
    return;
  }
  els.scanProgress.textContent = pairsText;
}

function renderWatchlist() {
  const summary = state.summary || {};
  const backfill = summary.backfill || {};
  const pairStatus = backfill.pair_status || {};
  const isBackfilling = summary.status === "backfilling" && backfill.phase && backfill.phase !== "done";

  const rows = sortPairs();

  // During backfill, show per-pair status even if no scan rows yet
  if (!rows.length && isBackfilling) {
    const allPairs = Object.keys(pairStatus).sort();
    if (allPairs.length) {
      els.watchlistBody.innerHTML = allPairs.map((p) => {
        const s = pairStatus[p] || "pending";
        return `
          <tr>
            <td><span class="pair-main">${escapeHtml(p)}</span></td>
            <td>${renderBadge(s === "ready" ? "live" : s === "pending" ? "wait" : "connecting", s)}</td>
            <td colspan="5" class="price" style="opacity:0.5">${escapeHtml(s)}</td>
          </tr>
        `;
      }).join("");
      return;
    }
    els.watchlistBody.innerHTML = `<tr><td colspan="7" class="empty">Backfilling data...</td></tr>`;
    return;
  }

  if (!rows.length) {
    els.watchlistBody.innerHTML = `<tr><td colspan="7" class="empty">Waiting for first scan.</td></tr>`;
    return;
  }

  const NEAR_THRESHOLD = 0.30;
  els.watchlistBody.innerHTML = rows.map((row) => {
    const signal = row.signal;
    const setupText = signal
      ? `${signal.zone_type} · ${signal.zone_strength}`
      : row.note || "No setup";
    const direction = signal?.direction || row.state;
    const sNear = row.support_dist_pct != null && row.support_dist_pct <= NEAR_THRESHOLD;
    const rNear = row.resistance_dist_pct != null && row.resistance_dist_pct <= NEAR_THRESHOLD;
    return `
      <tr>
        <td><a href="/chart?pair=${encodeURIComponent(row.pair)}" target="_blank" class="pair-main pair-link" title="Live chart ${escapeHtml(row.pair)}">${escapeHtml(row.pair)}</a></td>
        <td>${renderBadge(row.state)}</td>
        <td class="price">${formatNumber(row.price, PRICE_DISPLAY_DECIMALS)}</td>
        <td class="price${sNear ? " zone-near" : ""}">${escapeHtml(row.support_text || "–")}</td>
        <td class="price${rNear ? " zone-near" : ""}">${escapeHtml(row.resistance_text || "–")}</td>
        <td>${escapeHtml(setupText)}</td>
        <td>${signal ? renderBadge(direction) : '<span class="pair-sub">–</span>'}</td>
      </tr>
    `;
  }).join("");
}

function renderSignals() {
  if (!state.signals.length) {
    els.signalsList.innerHTML = `<div class="empty-card">No active signals.</div>`;
    return;
  }

  els.signalsList.innerHTML = state.signals.map((signal) => {
    const plan = signal.size_plan || {};
    return `
      <article class="signal-card">
        <div class="signal-head">
          <div>
            <strong>${escapeHtml(signal.pair)}</strong>
            <span class="pair-sub">${escapeHtml(signal.zone_type || "setup")} · ${escapeHtml(signal.zone_strength || "–")}</span>
          </div>
          ${renderBadge(signal.direction)}
        </div>
        <div class="signal-meta">
          <div><span class="value-label">Entry</span><span class="value">${formatNumber(signal.entry_price, PRICE_DISPLAY_DECIMALS)}</span></div>
          <div><span class="value-label">Stop</span><span class="value">${formatNumber(signal.sl_price, PRICE_DISPLAY_DECIMALS)}</span></div>
          <div><span class="value-label">Target</span><span class="value">${formatNumber(signal.tp_price, PRICE_DISPLAY_DECIMALS)}</span></div>
          <div><span class="value-label">Units</span><span class="value">${plan.units ? Number(plan.units).toLocaleString() : "–"}</span></div>
          <div><span class="value-label">Risk</span><span class="value">${plan.risk_amount ? `${formatNumber(plan.risk_amount, 2)} ${escapeHtml(plan.account_currency || "")}` : "–"}</span></div>
          <div><span class="value-label">Notional</span><span class="value">${plan.notional_account ? `${formatNumber(plan.notional_account, 0)} ${escapeHtml(plan.account_currency || "")}` : "–"}</span></div>
        </div>
      </article>
    `;
  }).join("");
}

function renderPositions() {
  if (!state.positions.length) {
    els.positionsList.innerHTML = `<div class="empty-card">No tracked positions.</div>`;
    return;
  }

  els.positionsList.innerHTML = state.positions.map((position) => {
    const pnlClass = Number(position.pnl_pips || 0) >= 0 ? "up" : "down";
    return `
      <article class="position-card">
        <div class="position-head">
          <div>
            <strong>${escapeHtml(position.pair)}</strong>
            <span class="pair-sub">${Number(position.size || 0).toLocaleString()} units</span>
          </div>
          ${renderBadge(position.status === "OK" ? position.direction : position.status, position.status === "OK" ? position.direction : position.status)}
        </div>
        <div class="position-meta">
        <div><span class="value-label">Entry</span><span class="value">${formatNumber(position.entry_price, PRICE_DISPLAY_DECIMALS)}</span></div>
        <div><span class="value-label">Current</span><span class="value">${formatNumber(position.current_price, PRICE_DISPLAY_DECIMALS)}</span></div>
          <div><span class="value-label">Direction</span><span class="value">${escapeHtml(position.direction)}</span></div>
          <div><span class="value-label">P/L</span><span class="value ${pnlClass}">${formatSigned(position.pnl_pips, 1, " pips")}</span></div>
        </div>
      </article>
    `;
  }).join("");
}

function renderAlerts() {
  if (!state.alerts.length) {
    els.alertsList.innerHTML = `<div class="empty-card">No exit alerts.</div>`;
    return;
  }

  els.alertsList.innerHTML = state.alerts.map((alert) => `
    <article class="mini-card">
      <div class="mini-head">
        <div>
          <strong>${escapeHtml(alert.pair)}</strong>
          <span class="pair-sub">${escapeHtml(alert.direction)}</span>
        </div>
        ${renderBadge("exit", alert.exit_reason)}
      </div>
      <div class="mini-meta">
        <div><span class="value-label">Current</span><span class="value">${formatNumber(alert.current_price, PRICE_DISPLAY_DECIMALS)}</span></div>
        <div><span class="value-label">P/L</span><span class="value ${Number(alert.pnl_pips || 0) >= 0 ? "up" : "down"}">${formatSigned(alert.pnl_pips, 1, " pips")}</span></div>
      </div>
    </article>
  `).join("");
}

function renderExecutions() {
  if (!state.executions.length) {
    destroySelectedExecutionChart();
    els.executionsList.innerHTML = `<div class="empty-card">No execution activity.</div>`;
    return;
  }

  els.executionsList.innerHTML = [...state.executions].reverse().map((execution) => {
    const key = executionKey(execution);
    const isSelected = key === selectedExecutionKey;
    return `
    <article class="mini-card mini-card-clickable ${isSelected ? "mini-card-selected" : ""}" data-execution-key="${escapeHtml(key)}">
      <div class="mini-head">
        <div>
          <strong>${escapeHtml(execution.pair)}</strong>
          <span class="pair-sub">${escapeHtml(execution.direction)} · ${Number(execution.units || 0).toLocaleString()} units</span>
        </div>
        ${renderBadge(execution.status)}
      </div>
      <div class="mini-meta">
        <div><span class="value-label">When / Order</span><span class="value">${escapeHtml(formatTimestamp(execution.time))} · #${escapeHtml(execution.order_id || "–")}</span></div>
        <div><span class="value-label">Note</span><span class="value">${escapeHtml(execution.note || "–")}</span></div>
      </div>
      ${isSelected ? `
      <div class="mini-detail">
        <div><span class="value-label">Ticker</span><span class="value">${escapeHtml(execution.pair || "–")}</span></div>
        <div><span class="value-label">Date</span><span class="value">${escapeHtml(formatDateOnly(execution.time))}</span></div>
        <div><span class="value-label">Entry</span><span class="value">${escapeHtml(formatExecutionPrice(execution.submitted_entry_price, execution))}</span></div>
        <div><span class="value-label">SL / TP</span><span class="value">${escapeHtml(formatExecutionPrice(execution.submitted_sl_price, execution))} / ${escapeHtml(formatExecutionPrice(execution.submitted_tp_price, execution))}</span></div>
        <div class="mini-detail-wide">
          <div id="selected-execution-chart-status" class="chart-status">Loading replay…</div>
          <div id="selected-execution-chart" class="execution-mini-chart"></div>
        </div>
      </div>
      ` : ""}
    </article>
  `;
  }).join("");
  void renderSelectedExecutionChart();
}

function renderLog() {
  if (!state.log.length) {
    els.logList.innerHTML = `<div class="empty-card">No events yet.</div>`;
    return;
  }

  els.logList.innerHTML = [...state.log].reverse().map((entry) => `
    <article class="log-row ${levelTone(entry.level)}">
      <p>${escapeHtml(entry.message)}</p>
      <time>${escapeHtml(entry.ts || "")}</time>
    </article>
  `).join("");
}

function renderAll() {
  renderSummary();
  renderWatchlist();
  renderSignals();
  renderPositions();
  renderAlerts();
  renderExecutions();
  renderLog();
}

function setConnection(stateName, text) {
  els.connectionPill.className = badgeClass(stateName);
  els.connectionPill.textContent = stateName;
}

function scheduleReconnect() {
  if (reconnectTimer) {
    return;
  }
  reconnectTimer = window.setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, 1500);
}

async function toggleExecutionPaused() {
  const summary = state.summary || {};
  if (!summary.execution_available || executionTogglePending) {
    return;
  }

  executionTogglePending = true;
  renderSummary();

  try {
    const res = await fetch("/api/execution-mode", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ paused: !Boolean(summary.execution_paused) }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data.error || "Unable to update execution mode.");
    }
    if (data.state) {
      applyState(data.state);
    }
  } catch (error) {
    pushLog({
      ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      level: "error",
      message: error.message || "Unable to update execution mode",
    });
  } finally {
    executionTogglePending = false;
    renderSummary();
  }
}

function connect() {
  if (socket && socket.readyState === WebSocket.OPEN) {
    return;
  }

  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const endpoint = `${protocol}://${window.location.host}/ws`;
  setConnection("connecting");
  socket = new WebSocket(endpoint);

  socket.addEventListener("open", () => {
    setConnection("live", "Live feed connected");
  });

  socket.addEventListener("message", (event) => {
    let message;
    try {
      message = JSON.parse(event.data);
    } catch (_error) {
      setConnection("error", "Malformed dashboard message");
      pushLog({
        ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        level: "error",
        message: "Received malformed dashboard message",
      });
      return;
    }
    if (message.type === "bootstrap") {
      applyState(message.state || {});
      updateRerunBacktestButton();
      return;
    }
    if (message.type === "scan_status" || message.type === "backfill_progress") {
      state.summary = message.summary || {};
      renderSummary();
      renderWatchlist();
      updateRerunBacktestButton();
      return;
    }
    if (message.type === "fill_progress" || message.type === "backtest_progress") {
      state.summary = message.summary || {};
      renderSummary();
      renderWatchlist();
      updateRerunBacktestButton();
      return;
    }
    if (message.type === "pair_update") {
      upsertPair(message.row, message.summary);
      return;
    }
    if (message.type === "snapshot") {
      applyState(message.state || {});
      return;
    }
    if (message.type === "log_entry") {
      pushLog(message.entry || {});
      return;
    }
    if (message.type === "error") {
      state.summary = message.summary || state.summary;
      renderSummary();
      pushLog({
        ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        level: "error",
        message: message.message || "Unknown live dashboard error",
      });
    }
  });

  socket.addEventListener("close", () => {
    setConnection("disconnected", "Socket closed, retrying");
    scheduleReconnect();
  });

  socket.addEventListener("error", () => {
    setConnection("disconnected", "Socket error, retrying");
    socket.close();
  });
}

countdownTimer = window.setInterval(renderScanProgress, 1000);
if (els.tradeToggleBtn) {
  els.tradeToggleBtn.addEventListener("click", toggleExecutionPaused);
}
if (els.executionsList) {
  els.executionsList.addEventListener("click", (event) => {
    const card = event.target.closest("[data-execution-key]");
    if (!card) {
      return;
    }
    const key = card.dataset.executionKey || null;
    selectedExecutionKey = selectedExecutionKey === key ? null : key;
    renderExecutions();
  });
}

const stopBtn = document.getElementById("stop-server-btn");
if (stopBtn) {
  stopBtn.addEventListener("click", async () => {
    if (!confirm("Stop the live server?")) return;
    stopBtn.disabled = true;
    stopBtn.textContent = "Stopping\u2026";
    try {
      const res = await fetch("/api/shutdown", { method: "POST" });
      if (!res.ok) {
        const message = await res.text().catch(() => "Shutdown request failed.");
        throw new Error(message || `HTTP ${res.status}`);
      }
      stopBtn.textContent = "Shutdown sent";
    } catch (error) {
      stopBtn.disabled = false;
      stopBtn.textContent = "Stop Server";
      const message = error?.message || "Unable to send shutdown request.";
      pushLog({
        ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        level: "error",
        message,
      });
      window.alert(`Unable to stop server: ${message}`);
    }
  });
}

const fillCacheBtn = document.getElementById("fill-cache-btn");
if (fillCacheBtn) {
  fillCacheBtn.addEventListener("click", () => {
    void invokeDashboardAction({
      button: fillCacheBtn,
      confirmMessage: "Run cache fill now?",
      endpointPaths: FILL_ENDPOINT_PATHS,
      loadingText: "Filling...",
      resetText: "Fill",
      errorTitle: "Unable to start cache fill",
      statusErrorHandlers: {
        404: (message, response) => `${message} (fill endpoint not found at ${response.url}). Restart dashboard and hard refresh the browser.`,
      },
      successMessage: "Cache fill started.",
      onSuccess: ({ message }) => {
        pushLog({
          ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
          level: "success",
          message,
        });
      },
      requestOptions: { method: "POST" },
    });
  });
}

rerunBacktestBtn = document.getElementById("rerun-backtest-btn");
if (rerunBacktestBtn) {
  rerunBacktestBtn.addEventListener("click", () => {
    previousBacktestStatus = "starting";
    void invokeDashboardAction({
      button: rerunBacktestBtn,
      confirmMessage: "Re-run full backtest now?",
      endpointPaths: BACKTEST_RERUN_ENDPOINT_PATHS,
      loadingText: "Starting...",
      errorTitle: "Unable to start backtest rerun",
      statusErrorHandlers: {
        409: (message) => message,
      },
      successMessage: "Backtest rerun started.",
      onSuccess: ({ message }) => {
        pushLog({
          ts: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
          level: "success",
          message,
        });
      },
      onFinally: updateRerunBacktestButton,
      requestOptions: { method: "POST" },
    });
  });
}

updateRerunBacktestButton();

connect();


