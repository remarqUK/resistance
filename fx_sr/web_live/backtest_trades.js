const pairFilter = document.getElementById("pair-filter");
const loadBtn = document.getElementById("load-btn");
const summaryEl = document.getElementById("summary");
const bodyEl = document.getElementById("trades-body");

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => {
    const map = {
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      "\"": "&quot;",
      "'": "&#39;",
    };
    return map[char];
  });
}

function replayDateForTrade(trade) {
  if (trade.entry_time) return String(trade.entry_time).slice(0, 10);
  if (trade.exit_time) return String(trade.exit_time).slice(0, 10);
  return "";
}

function openReplay(pair, date) {
  if (!pair || !date) return;
  const params = new URLSearchParams({
    pair: String(pair).toUpperCase(),
    date,
  });
  window.location.href = `/replay?${params.toString()}`;
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

function formatSigned(value, digits = 2, suffix = "") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "–";
  }
  const number = Number(value);
  const prefix = number > 0 ? "+" : "";
  return `${prefix}${formatNumber(number, digits)}${suffix}`;
}

function formatTime(isoTime) {
  if (!isoTime) {
    return "—";
  }
  const parsed = new Date(isoTime);
  if (Number.isNaN(parsed.getTime())) {
    return "—";
  }
  return parsed.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function buildRows(trades) {
  if (!trades.length) {
    bodyEl.innerHTML = `<tr><td colspan="8" class="empty">No completed backtest trades in the cache.</td></tr>`;
    return;
  }

  bodyEl.innerHTML = trades.map((trade) => {
    const pnlClass = (trade.pnl_pips || 0) >= 0 ? "up" : "down";
    const directionClass = (trade.direction || "").toLowerCase();
    const digits = trade.decimals || 5;
    const tradeDate = replayDateForTrade(trade);
    const exitPrice = trade.exit_price ? formatNumber(trade.exit_price, digits) : "—";
    const safePair = escapeHtml(trade.pair || "");
    const safeDate = escapeHtml(tradeDate);
    return `
      <tr class="trade-history-row" data-pair="${safePair}" data-date="${safeDate}">
        <td><span class="pair-main">${trade.pair || "–"}</span></td>
        <td>${formatTime(trade.entry_time)}</td>
        <td>${trade.exit_time ? formatTime(trade.exit_time) : "—"}</td>
        <td><span class="pill pill-${directionClass}" style="min-width:auto;padding:4px 8px;font-size:0.65rem">${trade.direction || "—"}</span></td>
        <td>${formatNumber(trade.entry_price, digits)} → ${exitPrice}</td>
        <td class="${pnlClass}">${formatSigned(trade.pnl_pips, 1, "p")}</td>
        <td class="${pnlClass}">${formatSigned(trade.pnl_r, 2, "R")}</td>
        <td>${trade.exit_reason || "—"}</td>
      </tr>
    `;
  }).join("");

  bodyEl.querySelectorAll(".trade-history-row").forEach((row) => {
    const pair = row.dataset.pair || "";
    const date = row.dataset.date || "";
    row.addEventListener("click", () => openReplay(pair, date));
  });
}

function populatePairs(pairs) {
  const current = pairFilter.value;
  pairFilter.innerHTML = `<option value="">All pairs</option>`;
  pairs.forEach((pair) => {
    const option = document.createElement("option");
    option.value = pair;
    option.textContent = pair;
    pairFilter.appendChild(option);
  });

  if (pairs.includes(current)) {
    pairFilter.value = current;
  }
}

function renderSummary(data) {
  const total = data.count || 0;
  const activePair = data.pair_filter || "All pairs";
  summaryEl.textContent = `${total} trade${total === 1 ? "" : "s"} from ${activePair} in cache`;
}

function showMessage(message) {
  bodyEl.innerHTML = `<tr><td colspan="8" class="empty">${message}</td></tr>`;
  summaryEl.textContent = message;
}

async function loadTrades() {
  loadBtn.disabled = true;
  loadBtn.textContent = "Loading...";
  summaryEl.textContent = "Loading trades...";

  try {
    const params = new URLSearchParams();
    const selectedPair = pairFilter.value;
    if (selectedPair) {
      params.set("pair", selectedPair);
    }

    const res = await fetch(`/api/backtest/trades?${params.toString()}`);
    const data = await res.json();
    if (!res.ok) {
      showMessage(data.error || "Unable to load backtest trades.");
      return;
    }

    const rows = data.trades || [];
    populatePairs(data.pairs || []);
    renderSummary(data);
    buildRows(rows);
  } catch (err) {
    showMessage(`Network error: ${err.message}`);
  } finally {
    loadBtn.disabled = false;
    loadBtn.textContent = "Load";
  }
}

pairFilter.addEventListener("change", loadTrades);
loadBtn.addEventListener("click", loadTrades);

loadTrades();
