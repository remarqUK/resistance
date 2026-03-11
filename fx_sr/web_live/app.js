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

const els = {
  connectionPill: document.getElementById("connection-pill"),
  scanStatus: document.getElementById("scan-status"),
  scanProgress: document.getElementById("scan-progress"),
  signalCount: document.getElementById("signal-count"),
  pendingCount: document.getElementById("pending-count"),
  positionCount: document.getElementById("position-count"),
  executionMode: document.getElementById("execution-mode"),
  sizingSummary: document.getElementById("sizing-summary"),
  strategyLabel: document.getElementById("strategy-label"),
  watchlistBody: document.getElementById("watchlist-body"),
  signalsList: document.getElementById("signals-list"),
  positionsList: document.getElementById("positions-list"),
  alertsList: document.getElementById("alerts-list"),
  executionsList: document.getElementById("executions-list"),
  logList: document.getElementById("log-list"),
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatNumber(value, digits = 2) {
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
    WATCH: 2,
    INSIDE: 3,
    WAIT: 5,
    "NO DATA": 6,
  };

  return Object.values(state.pairs).sort((left, right) => {
    const leftSignal = left.signal ? 1 : 0;
    const rightSignal = right.signal ? 1 : 0;
    if (leftSignal !== rightSignal) {
      return rightSignal - leftSignal;
    }

    const leftPosition = positionPairs.has(left.pair) ? 1 : 0;
    const rightPosition = positionPairs.has(right.pair) ? 1 : 0;
    if (leftPosition !== rightPosition) {
      return rightPosition - leftPosition;
    }

    const leftPriority = priority[left.state] ?? 4;
    const rightPriority = priority[right.state] ?? 4;
    if (leftPriority !== rightPriority) {
      return leftPriority - rightPriority;
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
  els.scanStatus.textContent = String(summary.status || "starting").toUpperCase();
  els.signalCount.textContent = String(summary.signal_count || 0);
  els.pendingCount.textContent = `${summary.pending_count || 0} pending blockers`;
  els.positionCount.textContent = String(summary.position_count || 0);
  els.executionMode.textContent = summary.execution_enabled ? "Paper execution enabled" : "Scan only";
  els.sizingSummary.textContent = summary.sizing_summary || "Resolving";
  els.strategyLabel.textContent = `${summary.strategy_label || "Strategy"} · ${summary.mode || "scanner"}`;
  const stateLabel = summary.status || "connecting";
  els.connectionPill.className = badgeClass(stateLabel === "error" ? "disconnected" : stateLabel === "starting" ? "connecting" : "live");

  renderScanProgress();
}

function renderScanProgress() {
  const summary = state.summary || {};
  const pairsText = `${summary.pairs_completed || 0} / ${summary.pairs_total || 0} pairs`;
  if (summary.status === "scanning") {
    els.scanProgress.textContent = pairsText;
    return;
  }
  const next = formatRelativeCountdown(summary.next_scan_at);
  els.scanProgress.textContent = next === "Waiting" ? pairsText : `${pairsText} · next ${next}`;
}

function renderWatchlist() {
  const rows = sortPairs();
  if (!rows.length) {
    els.watchlistBody.innerHTML = `<tr><td colspan="7" class="empty">Waiting for first scan.</td></tr>`;
    return;
  }

  els.watchlistBody.innerHTML = rows.map((row) => {
    const signal = row.signal;
    const setupText = signal
      ? `${signal.zone_type} · ${signal.zone_strength}`
      : row.note || "No setup";
    const direction = signal?.direction || row.state;
    return `
      <tr>
        <td><a href="/replay?pair=${encodeURIComponent(row.pair)}" target="_blank" class="pair-main pair-link" title="Replay ${escapeHtml(row.pair)}">${escapeHtml(row.pair)}</a></td>
        <td>${renderBadge(row.state)}</td>
        <td class="price">${formatNumber(row.price, row.decimals ?? 5)}</td>
        <td class="price">${escapeHtml(row.support_text || "–")}</td>
        <td class="price">${escapeHtml(row.resistance_text || "–")}</td>
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
          <div><span class="value-label">Entry</span><span class="value">${formatNumber(signal.entry_price, signal.decimals ?? 5)}</span></div>
          <div><span class="value-label">Stop</span><span class="value">${formatNumber(signal.sl_price, signal.decimals ?? 5)}</span></div>
          <div><span class="value-label">Target</span><span class="value">${formatNumber(signal.tp_price, signal.decimals ?? 5)}</span></div>
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
          <div><span class="value-label">Entry</span><span class="value">${formatNumber(position.entry_price, position.decimals ?? 5)}</span></div>
          <div><span class="value-label">Current</span><span class="value">${formatNumber(position.current_price, position.decimals ?? 5)}</span></div>
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
        <div><span class="value-label">Current</span><span class="value">${formatNumber(alert.current_price, alert.decimals ?? 5)}</span></div>
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
    const message = JSON.parse(event.data);
    if (message.type === "bootstrap") {
      applyState(message.state || {});
      return;
    }
    if (message.type === "scan_status") {
      state.summary = message.summary || {};
      renderSummary();
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
connect();
