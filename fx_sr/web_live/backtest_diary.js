/* Backtest Diary — page-specific logic.
 * Shared utilities are loaded from diary_shared.js (must be included first). */

const backtestFilter = document.getElementById("backtest-filter");
const loadBtn = document.getElementById("load-btn");
const monthRangeEl = document.getElementById("month-range");
const summaryEl = document.getElementById("summary");
const selectedDateEl = document.getElementById("selected-date");
const calendarEl = document.getElementById("diary-calendar");
const bodyEl = document.getElementById("diary-body");
const BACKTEST_CURRENCY = "GBP";
let selectedBacktest = null;

let dateMap = new Map();
let selectedDate = "";

function formatCurrency(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "\u2014";
  }
  return `${BACKTEST_CURRENCY} ${formatNumber(value, 2)}`;
}

function formatBacktestDate(isoTime) {
  if (!isoTime) {
    return "";
  }
  const parsed = new Date(isoTime);
  if (Number.isNaN(parsed.getTime())) {
    return String(isoTime).slice(0, 10);
  }
  return parsed.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "UTC",
  });
}

function replayDateForTrade(trade) {
  if (trade.entry_time) return String(trade.entry_time).slice(0, 10);
  if (trade.exit_time) return String(trade.exit_time).slice(0, 10);
  return "";
}

function formatBacktestOption(backtest) {
  if (!backtest) return "Unknown backtest";
  const parts = [];
  const dateLabel = formatBacktestDate(backtest.updated_at);
  const nameLabel = backtest.label || backtest.profile_name || "cached run";
  if (dateLabel) {
    parts.push(dateLabel);
  }
  parts.push(nameLabel);
  if (backtest.description && backtest.description !== nameLabel) {
    parts.push(backtest.description);
  }
  if (backtest.hourly_days && backtest.zone_history_days) {
    parts.push(`${backtest.hourly_days}d / ${backtest.zone_history_days}d`);
  }
  if (backtest.starting_balance !== null && backtest.starting_balance !== undefined) {
    parts.push(`${formatCurrency(backtest.starting_balance)} @ ${formatNumber(backtest.risk_pct, 2)}%`);
  }
  return parts.join(" \u00b7 ");
}

function populateBacktests(backtests, selectedKey = "") {
  const current = selectedKey || backtestFilter.value;
  backtestFilter.innerHTML = "";

  if (!backtests.length) {
    backtestFilter.innerHTML = "<option value=\"\">No cached runs</option>";
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

function tradeEntryDate(trade) {
  const entryDate = trade?.entry_time ? String(trade.entry_time).slice(0, 10) : "";
  return isIsoDate(entryDate) ? entryDate : "";
}

function openReplay(pair, date, preset, entryTime = "", backtestKey = "") {
  if (!pair || !date) return;
  const params = new URLSearchParams({
    pair: String(pair).toUpperCase(),
    date,
  });
  if (preset) params.set('preset', preset);
  if (backtestKey) params.set('backtest', backtestKey);
  if (entryTime) params.set('entry', entryTime);
  window.location.href = `/replay?${params.toString()}`;
}

function showMessage(message) {
  bodyEl.innerHTML = `<div style="color:var(--muted);font-size:0.84rem;padding:8px 0">${message}</div>`;
  summaryEl.textContent = message;
}

function buildRows(trades, dateFilter = "") {
  if (!trades.length) {
    bodyEl.innerHTML = `<div style="color:var(--muted);font-size:0.84rem;padding:8px 0">No trades for this date.</div>`;
    return;
  }
  const _hhmm = (iso) => {
    if (!iso) return "";
    const d = new Date(iso);
    return isNaN(d) ? "" : d.toLocaleTimeString([], {hour:"2-digit",minute:"2-digit",hour12:false,timeZone:"UTC"});
  };
  bodyEl.innerHTML = trades.map((trade, i) => {
    const cls = (trade.pnl_r || 0) >= 0 ? "up" : "down";
    const dirCls = (trade.direction || "").toLowerCase();
    const dirLabel = (trade.direction || "").charAt(0);
    const entryHm = _hhmm(trade.entry_time);
    const exitHm = _hhmm(trade.exit_time);
    const timeRange = exitHm ? `${entryHm}\u2013${exitHm}` : entryHm;
    const reason = trade.exit_reason || "\u2014";
    return `
      <div class="trade-history-row" data-idx="${i}" style="display:flex;align-items:center;gap:6px;padding:5px 0;border-bottom:1px solid var(--line);cursor:pointer;font-size:0.82rem">
        <span class="pill pill-${dirCls}" style="font-size:0.62rem;padding:1px 5px;min-width:auto">${dirLabel}</span>
        <span style="font-weight:600;min-width:56px">${trade.pair || "\u2013"}</span>
        <span style="flex:1;color:var(--muted);font-size:0.76rem">${timeRange}</span>
        <span class="${cls}" style="font-weight:600;min-width:50px;text-align:right">${formatSigned(trade.pnl_r, 2, "R")}</span>
        <span style="color:var(--muted);font-size:0.72rem;min-width:30px;text-align:right">${reason}</span>
      </div>
    `;
  }).join("");

  bodyEl.querySelectorAll(".trade-history-row").forEach((row) => {
    const idx = Number(row.dataset.idx);
    const trade = trades[idx];
    if (!trade) return;
    const tradeDate = dateFilter || replayDateForTrade(trade);
    row.addEventListener("click", () => openReplay(
      trade.pair || "",
      tradeDate,
      "",
      trade.entry_time || "",
      selectedBacktest?.key || "",
    ));
  });
}

function buildCalendarState(trades) {
  const map = new Map();
  for (const trade of trades) {
    const entryDate = tradeEntryDate(trade);
    if (!entryDate) continue;
    if (!map.has(entryDate)) {
      map.set(entryDate, {
        date: entryDate,
        trades: [],
        count: 0,
        wins: 0,
        losses: 0,
        total_pnl_pips: 0,
        total_pnl_r: 0,
      });
    }
    const row = map.get(entryDate);
    row.trades.push(trade);
    row.count += 1;
    const pnlPips = Number(trade.pnl_pips) || 0;
    const pnlR = Number(trade.pnl_r) || 0;
    row.total_pnl_pips += pnlPips;
    row.total_pnl_r += pnlR;
    if (pnlPips > 0) row.wins += 1;
    if (pnlPips < 0) row.losses += 1;
  }

  for (const row of map.values()) {
    row.trades.sort((a, b) => {
      const aTime = String(a.entry_time || "");
      const bTime = String(b.entry_time || "");
      return bTime.localeCompare(aTime);
    });
    row.total_pnl_pips = Number(row.total_pnl_pips.toFixed(1));
    row.total_pnl_r = Number(row.total_pnl_r.toFixed(2));
  }

  return map;
}

function selectReplayPair(trades) {
  if (!trades.length) return "";
  if (trades[0].pair) return trades[0].pair;
  for (const trade of trades) {
    if (trade.pair) return trade.pair;
  }
  return "";
}

function openReplayForDate(date) {
  const dayData = dateMap.get(date);
  if (!dayData || !dayData.trades.length) return;
  const firstTrade = dayData.trades[0];
  const pair = selectReplayPair(dayData.trades);
  if (!pair) return;
  openReplay(pair, date, "", firstTrade?.entry_time || "", selectedBacktest?.key || "");
}

function renderCalendar() {
  if (!dateMap.size) {
    calendarEl.innerHTML = "<div class=\"empty-card empty\">No backtest trades available yet.</div>";
    monthRangeEl.textContent = "No cached trades";
    selectedDateEl.textContent = "Load trades to render calendar.";
    return;
  }

  const range = getMonthRange(dateMap);
  if (!range) return;

  let cursor = monthStartFromDate(range.end);
  const startMonth = monthKeyFromDate(range.start);

  const monthRows = [];
  while (monthKeyFromDate(cursor) >= startMonth) {
    monthRows.push(renderMonth(cursor));
    cursor = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth() - 1, 1));
  }

  calendarEl.innerHTML = monthRows.join("");
  monthRangeEl.textContent = `${formatMonthLabel(range.end)} \u2014 ${formatMonthLabel(range.start)} (${dateMap.size} entry days)`;
  wireDayClicks();
}

function selectDate(date) {
  if (!isIsoDate(date)) return;
  selectedDate = date;
  renderCalendar();
  loadDateTrades(date);
}

function renderDateSummary(date, data) {
  const selected = data || {
    count: 0,
    wins: 0,
    losses: 0,
    total_pnl_pips: 0,
    total_pnl_r: 0,
  };

  const cls = (selected.total_pnl_pips || 0) > 0 ? "up" : (selected.total_pnl_pips || 0) < 0 ? "down" : "";
  selectedDateEl.textContent = `${formatDateLabel(date)} \u00b7 ${selected.count} trade${selected.count === 1 ? "" : "s"}`;
  summaryEl.innerHTML = `
    <strong>${date}</strong> \u2014 ${selected.count} trade${selected.count === 1 ? "" : "s"} (W/L ${selected.wins}/${selected.losses})
    \u00b7 P/L: <span class="${cls}">${formatSigned(selected.total_pnl_pips, 1, " pips")}</span>
    \u00b7 R: ${formatSigned(selected.total_pnl_r, 2, "R")}
  `;
}

function loadDateTrades(date) {
  const dayData = dateMap.get(date);
  if (!dayData) {
    summaryEl.textContent = `${date} \u2014 no trades in cache for this day`;
    bodyEl.innerHTML = `<div style="color:var(--muted);font-size:0.84rem;padding:8px 0">No trades for this date.</div>`;
    return;
  }

  renderDateSummary(date, dayData);
  buildRows(dayData.trades || [], date);
}

function showLoading(message) {
  calendarEl.innerHTML = `<div class=\"empty-card empty\">${message}</div>`;
  monthRangeEl.textContent = "";
  selectedDateEl.textContent = "Loading diary...";
  bodyEl.innerHTML = `<div style="color:var(--muted);font-size:0.84rem;padding:8px 0">Load diary to view trades.</div>`;
  summaryEl.textContent = message;
}

function loadDiaryData() {
  loadBtn.disabled = true;
  loadBtn.textContent = "Loading...";
  showLoading("Loading trades cache\u2026");

  const params = new URLSearchParams();
  const selectedBacktestKey = backtestFilter.value;
  if (selectedBacktestKey) {
    params.set("backtest", selectedBacktestKey);
  }
  const endpoint = params.toString() ? `/api/backtest/trades?${params}` : "/api/backtest/trades";

  return fetch(endpoint)
    .then((res) => res.json().then((data) => ({ ok: res.ok, data })))
    .then(({ ok, data }) => {
      if (!ok) {
        throw new Error(data.error || "Unable to load backtest trades.");
      }
      populateBacktests(data.backtests || [], data.selected_backtest?.key || "");
      selectedBacktest = data.selected_backtest || null;
      const trades = data.trades || [];
      dateMap = buildCalendarState(trades);
      renderCalendar();

      const selectedPairs = Array.from(dateMap.keys()).sort();
      const initialDate = defaultDateFromQuery(selectedPairs);
      if (initialDate) {
        selectedDate = initialDate;
        loadDateTrades(selectedDate);
      } else if (selectedPairs.length > 0) {
        selectedDate = selectedPairs[selectedPairs.length - 1];
        loadDateTrades(selectedDate);
      } else {
        selectedDate = "";
        selectedDateEl.textContent = "No trades available for this cache.";
        bodyEl.innerHTML = `<tr><td colspan=\"9\" class=\"empty\">No trades available in cache.</td></tr>`;
        summaryEl.textContent = "No cached trades.";
      }
    })
    .catch((err) => {
      showMessage(`Unable to load diary data: ${err.message}`);
    })
    .finally(() => {
      loadBtn.disabled = false;
      loadBtn.textContent = "Load Diary";
    });
}

function defaultDateFromQuery(sortedDates) {
  const params = new URLSearchParams(window.location.search);
  const candidate = params.get("date");
  if (!isIsoDate(candidate) || !sortedDates.includes(candidate)) {
    return "";
  }
  return candidate;
}

backtestFilter.addEventListener("change", loadDiaryData);
loadBtn.addEventListener("click", loadDiaryData);
loadDiaryData();
