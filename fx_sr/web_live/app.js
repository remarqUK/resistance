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
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
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
  renderTransactionCountdown();

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
    els.executionsList.innerHTML = `<div class="empty-card">No execution activity.</div>`;
    return;
  }

  els.executionsList.innerHTML = state.executions.map((execution) => `
    <article class="mini-card">
      <div class="mini-head">
        <div>
          <strong>${escapeHtml(execution.pair)}</strong>
          <span class="pair-sub">${escapeHtml(execution.direction)} · ${Number(execution.units || 0).toLocaleString()} units</span>
        </div>
        ${renderBadge(execution.status)}
      </div>
      <div class="mini-meta">
        <div><span class="value-label">Order</span><span class="value">${escapeHtml(execution.order_id || "–")}</span></div>
        <div><span class="value-label">Note</span><span class="value">${escapeHtml(execution.note || "–")}</span></div>
      </div>
    </article>
  `).join("");
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
      return;
    }
    if (message.type === "scan_status" || message.type === "backfill_progress") {
      state.summary = message.summary || {};
      renderSummary();
      renderWatchlist();
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

connect();
