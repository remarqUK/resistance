const backtestFilter = document.getElementById("backtest-filter");
const pairFilter = document.getElementById("pair-filter");
const loadBtn = document.getElementById("load-btn");
const summaryEl = document.getElementById("summary");
const bodyEl = document.getElementById("trades-body");
const sortHeaders = Array.from(document.querySelectorAll("th[data-sort-key]"));
const BACKTEST_CURRENCY = "GBP";
let selectedBacktest = null;
let loadedTrades = [];
const sortState = {
  key: "entry_time",
  direction: "desc",
};

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

function openReplay(pair, date, entryTime = "", backtestKey = "", preset = "") {
  if (!pair || !date) return;
  const params = new URLSearchParams({
    pair: String(pair).toUpperCase(),
    date,
  });
  if (backtestKey) {
    params.set("backtest", backtestKey);
  }
  if (preset) {
    params.set("preset", preset);
  }
  if (entryTime) {
    params.set("entry", entryTime);
  }
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

function formatCurrency(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }
  return `${BACKTEST_CURRENCY} ${formatNumber(value, 0)}`;
}

function formatApproxMoneyDelta(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "";
  }
  return `(${formatSigned(value, 0, "")})`;
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

function compareValues(left, right, direction) {
  if (left === right) return 0;
  if (left === null || left === undefined || left === "") return 1;
  if (right === null || right === undefined || right === "") return -1;
  if (left < right) return direction === "asc" ? -1 : 1;
  return direction === "asc" ? 1 : -1;
}

function tradeSortValue(trade, key) {
  switch (key) {
    case "pair":
    case "direction":
    case "exit_reason":
      return String(trade[key] || "").toUpperCase();
    case "entry_time":
    case "exit_time": {
      const value = trade[key];
      if (!value) return null;
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? null : parsed.getTime();
    }
    case "entry_price":
    case "pnl_pips":
    case "pnl_r":
    case "balance_after": {
      const value = trade[key];
      if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return null;
      }
      return Number(value);
    }
    default:
      return trade[key];
  }
}

function sortedTrades(trades) {
  const indexed = trades.map((trade, index) => ({ trade, index }));
  indexed.sort((left, right) => {
    const leftValue = tradeSortValue(left.trade, sortState.key);
    const rightValue = tradeSortValue(right.trade, sortState.key);
    const primary = compareValues(leftValue, rightValue, sortState.direction);
    if (primary !== 0) return primary;

    const leftEntry = tradeSortValue(left.trade, "entry_time");
    const rightEntry = tradeSortValue(right.trade, "entry_time");
    const secondary = compareValues(leftEntry, rightEntry, "desc");
    if (secondary !== 0) return secondary;

    return left.index - right.index;
  });
  return indexed.map((item) => item.trade);
}

function updateSortHeaders() {
  sortHeaders.forEach((header) => {
    const key = header.dataset.sortKey || "";
    const label = header.dataset.sortLabel || header.textContent || "";
    const isActive = key === sortState.key;
    header.style.cursor = "pointer";
    header.style.userSelect = "none";
    header.setAttribute("role", "button");
    header.setAttribute(
      "aria-sort",
      !isActive ? "none" : (sortState.direction === "asc" ? "ascending" : "descending"),
    );
    header.textContent = isActive
      ? `${label} ${sortState.direction === "asc" ? "▲" : "▼"}`
      : label;
  });
}

function renderTrades() {
  buildRows(sortedTrades(loadedTrades));
}

function toggleSort(key) {
  if (!key) return;
  if (sortState.key === key) {
    sortState.direction = sortState.direction === "asc" ? "desc" : "asc";
  } else {
    sortState.key = key;
    sortState.direction = key === "entry_time" ? "desc" : "asc";
  }
  updateSortHeaders();
  renderTrades();
}

function buildRows(trades) {
  if (!trades.length) {
    bodyEl.innerHTML = `<tr><td colspan="9" class="empty">No completed backtest trades in the cache.</td></tr>`;
    return;
  }

  bodyEl.innerHTML = trades.map((trade) => {
    const pnlClass = (trade.pnl_pips || 0) >= 0 ? "up" : "down";
    const directionClass = (trade.direction || "").toLowerCase();
    const digits = trade.decimals || 5;
    const tradeDate = replayDateForTrade(trade);
    const exitPrice = trade.exit_price ? formatNumber(trade.exit_price, digits) : "—";
    const balanceDisplay = formatCurrency(trade.balance_after);
    const profitDisplay = formatApproxMoneyDelta(trade.pnl_amount);
    const safePair = escapeHtml(trade.pair || "");
    const safeDate = escapeHtml(tradeDate);
    const safeEntry = escapeHtml(trade.entry_time || "");
    return `
      <tr class="trade-history-row" data-pair="${safePair}" data-date="${safeDate}" data-entry="${safeEntry}">
        <td><span class="pair-main">${trade.pair || "–"}</span></td>
        <td>${formatTime(trade.entry_time)}</td>
        <td>${trade.exit_time ? formatTime(trade.exit_time) : "—"}</td>
        <td><span class="pill pill-${directionClass}" style="min-width:auto;padding:4px 8px;font-size:0.65rem">${trade.direction || "—"}</span></td>
        <td>${formatNumber(trade.entry_price, digits)} → ${exitPrice}</td>
        <td class="${pnlClass}">${formatSigned(trade.pnl_pips, 1, "p")}</td>
        <td class="${pnlClass}">${formatSigned(trade.pnl_r, 2, "R")}</td>
        <td>
          <div>${balanceDisplay}</div>
          ${profitDisplay ? `<div style="font-size:0.74rem;color:var(--muted);opacity:0.72">${profitDisplay}</div>` : ""}
        </td>
        <td>${trade.exit_reason || "—"}</td>
      </tr>
    `;
  }).join("");

  bodyEl.querySelectorAll(".trade-history-row").forEach((row) => {
    const pair = row.dataset.pair || "";
    const date = row.dataset.date || "";
    const entry = row.dataset.entry || "";
    row.addEventListener("click", () => openReplay(
      pair,
      date,
      entry,
      selectedBacktest?.key || "",
      selectedBacktest?.profile_name || "",
    ));
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
  } else if (current) {
    pairFilter.value = "";
  }
}

function formatBacktestOption(backtest) {
  if (!backtest) return "Unknown backtest";
  const parts = [backtest.label || backtest.profile_name || "cached run"];
  if (backtest.hourly_days && backtest.zone_history_days) {
    parts.push(`${backtest.hourly_days}d / ${backtest.zone_history_days}d`);
  }
  if (backtest.starting_balance !== null && backtest.starting_balance !== undefined) {
    parts.push(`${formatCurrency(backtest.starting_balance)} @ ${formatNumber(backtest.risk_pct, 2)}%`);
  }
  return parts.join(" · ");
}

function populateBacktests(backtests, selectedKey = "") {
  const current = selectedKey || backtestFilter.value;
  backtestFilter.innerHTML = "";

  if (!backtests.length) {
    backtestFilter.innerHTML = `<option value="">No cached runs</option>`;
    backtestFilter.disabled = true;
    selectedBacktest = null;
    return;
  }

  backtestFilter.disabled = false;
  backtests.forEach((backtest) => {
    const option = document.createElement("option");
    option.value = backtest.key || "";
    option.textContent = formatBacktestOption(backtest);
    backtestFilter.appendChild(option);
  });

  const availableKeys = backtests.map((backtest) => backtest.key);
  if (availableKeys.includes(current)) {
    backtestFilter.value = current;
  } else {
    backtestFilter.value = availableKeys[0] || "";
  }

  selectedBacktest = backtests.find((backtest) => backtest.key === backtestFilter.value) || backtests[0] || null;
}

function renderSummary(data) {
  const total = data.count || 0;
  const activePair = data.pair_filter || "All pairs";
  let summary = `${total} trade${total === 1 ? "" : "s"} from ${activePair} in cache`;
  if (data.selected_backtest) {
    summary += ` · backtest: ${data.selected_backtest.label || data.selected_backtest.profile_name || data.selected_backtest.key}`;
  }
  const compounding = data.compounding;
  if (compounding && compounding.starting_balance) {
    const assumption = compounding.assumed ? "assumed " : "";
    summary += ` · ${assumption}balance starts at ${formatCurrency(compounding.starting_balance)} @ ${formatNumber(compounding.risk_pct, 2)}% risk`;
    if (compounding.profile_name) {
      summary += ` (${compounding.profile_name})`;
    }
    if (compounding.mixed_params) {
      summary += " · mixed cached parameter sets";
    }
  }
  summaryEl.textContent = summary;
}

function showMessage(message) {
  bodyEl.innerHTML = `<tr><td colspan="9" class="empty">${message}</td></tr>`;
  summaryEl.textContent = message;
}

async function loadTrades() {
  loadBtn.disabled = true;
  loadBtn.textContent = "Loading...";
  summaryEl.textContent = "Loading trades...";

  try {
    const params = new URLSearchParams();
    const selectedBacktestKey = backtestFilter.value;
    if (selectedBacktestKey) {
      params.set("backtest", selectedBacktestKey);
    }
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

    loadedTrades = data.trades || [];
    populateBacktests(data.backtests || [], data.selected_backtest?.key || "");
    populatePairs(data.pairs || []);
    selectedBacktest = data.selected_backtest || selectedBacktest;
    renderSummary(data);
    renderTrades();
  } catch (err) {
    loadedTrades = [];
    showMessage(`Network error: ${err.message}`);
  } finally {
    loadBtn.disabled = false;
    loadBtn.textContent = "Load";
  }
}

sortHeaders.forEach((header) => {
  header.addEventListener("click", () => toggleSort(header.dataset.sortKey || ""));
});
updateSortHeaders();
backtestFilter.addEventListener("change", loadTrades);
pairFilter.addEventListener("change", loadTrades);
loadBtn.addEventListener("click", loadTrades);

loadTrades();
