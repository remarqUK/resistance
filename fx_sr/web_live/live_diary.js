/* Live Diary — page-specific logic.
 * Shared utilities are loaded from diary_shared.js (must be included first). */

const loadBtn = document.getElementById("load-btn");
const monthRangeEl = document.getElementById("month-range");
const summaryEl = document.getElementById("summary");
const selectedDateEl = document.getElementById("selected-date");
const calendarEl = document.getElementById("diary-calendar");
const bodyEl = document.getElementById("diary-body");

let dateMap = new Map();
let selectedDate = "";

function tradeDate(trade) {
  // Use signal_time (entry bar) as the trade date
  const t = trade.signal_time || trade.opened_at || trade.detected_at;
  if (!t) return "";
  const d = String(t).slice(0, 10);
  return isIsoDate(d) ? d : "";
}

function buildCalendarState(trades) {
  const map = new Map();
  for (const t of trades) {
    const d = tradeDate(t);
    if (!d) continue;
    if (!map.has(d)) {
      map.set(d, { date: d, trades: [], count: 0, wins: 0, losses: 0, total_pnl_r: 0 });
    }
    const row = map.get(d);
    row.trades.push(t);
    row.count += 1;
    const r = Number(t.pnl_r) || 0;
    row.total_pnl_r += r;
    if (r > 0) row.wins += 1;
    if (r < 0) row.losses += 1;
  }
  for (const row of map.values()) {
    row.trades.sort((a, b) => String(b.signal_time || "").localeCompare(String(a.signal_time || "")));
    row.total_pnl_r = Number(row.total_pnl_r.toFixed(2));
  }
  return map;
}

function buildRows(trades) {
  if (!trades.length) {
    bodyEl.innerHTML = `<div style="color:var(--muted);font-size:0.84rem;padding:8px 0">No trades for this date.</div>`;
    return;
  }
  const _hhmm = (iso) => {
    if (!iso) return "";
    const d = new Date(iso);
    return isNaN(d) ? "" : d.toLocaleTimeString([], {hour:"2-digit",minute:"2-digit",hour12:false});
  };
  bodyEl.innerHTML = trades.map((t, i) => {
    const pnlR = Number(t.pnl_r) || 0;
    const cls = pnlR >= 0 ? "up" : "down";
    const dirCls = (t.direction || "").toLowerCase();
    const dirLabel = (t.direction || "").charAt(0);
    const entryHm = _hhmm(t.opened_at || t.signal_time);
    const exitHm = _hhmm(t.closed_at);
    const timeRange = exitHm ? `${entryHm}\u2013${exitHm}` : entryHm;
    const reason = t.close_reason || (t.status === "OPEN" ? "Open" : "");
    return `
      <div class="trade-history-row" data-idx="${i}" style="display:flex;align-items:center;gap:6px;padding:5px 0;border-bottom:1px solid var(--line);cursor:pointer;font-size:0.82rem">
        <span class="pill pill-${dirCls}" style="font-size:0.62rem;padding:1px 5px;min-width:auto">${dirLabel}</span>
        <span style="font-weight:600;min-width:56px">${t.pair || "\u2013"}</span>
        <span style="flex:1;color:var(--muted);font-size:0.76rem">${timeRange}</span>
        <span class="${cls}" style="font-weight:600;min-width:50px;text-align:right">${formatSigned(t.pnl_r, 2, "R")}</span>
        <span style="color:var(--muted);font-size:0.72rem;min-width:30px;text-align:right">${reason}</span>
      </div>
    `;
  }).join("");

  bodyEl.querySelectorAll(".trade-history-row").forEach((row) => {
    const idx = Number(row.dataset.idx);
    row.addEventListener("click", () => {
      openTradeChart(trades[idx]);
    });
  });
}

function openTradeChart(trade) {
  if (!trade || !trade.pair) return;
  const params = new URLSearchParams({ pair: String(trade.pair).toUpperCase() });
  if (trade.signal_id != null) {
    params.set("signal_id", trade.signal_id);
  } else {
    if (trade.opened_price != null) params.set("entry_price", trade.opened_price);
    if (trade.opened_at || trade.signal_time) params.set("entry_time", trade.opened_at || trade.signal_time);
    if (trade.closed_price != null) params.set("exit_price", trade.closed_price);
    if (trade.closed_at) params.set("exit_time", trade.closed_at);
    if (trade.submitted_sl_price != null) params.set("sl", trade.submitted_sl_price);
    if (trade.submitted_tp_price != null) params.set("tp", trade.submitted_tp_price);
    if (trade.direction) params.set("direction", trade.direction);
  }
  window.location.href = `/live-trade?${params.toString()}`;
}

function selectDate(date) {
  if (!isIsoDate(date)) return;
  selectedDate = date;
  renderCalendar();
  loadDateTrades(date);
}

function loadDateTrades(date) {
  const dayData = dateMap.get(date);
  if (!dayData) {
    summaryEl.textContent = `${date} \u2014 no trades`;
    bodyEl.innerHTML = `<div style="color:var(--muted);font-size:0.84rem;padding:8px 0">No trades for this date.</div>`;
    return;
  }
  const r = dayData.total_pnl_r;
  const cls = r > 0 ? "up" : r < 0 ? "down" : "";
  selectedDateEl.textContent = `${formatDateLabel(date)} \u00b7 ${dayData.count} trade${dayData.count === 1 ? "" : "s"}`;
  summaryEl.innerHTML = `
    <strong>${date}</strong> \u2014 ${dayData.count} trade${dayData.count === 1 ? "" : "s"} (W/L ${dayData.wins}/${dayData.losses})
    \u00b7 R: <span class="${cls}">${formatSigned(r, 2, "R")}</span>
  `;
  buildRows(dayData.trades);
}

function renderCalendar() {
  if (!dateMap.size) {
    calendarEl.innerHTML = `<div class="empty-card empty">No live trades recorded yet.</div>`;
    monthRangeEl.textContent = "";
    selectedDateEl.textContent = "No trades to display.";
    return;
  }
  const range = getMonthRange(dateMap);
  if (!range) return;

  let cursor = monthStartFromDate(range.end);
  const startMonth = monthKeyFromDate(range.start);
  const months = [];
  while (monthKeyFromDate(cursor) >= startMonth) {
    months.push(renderMonth(cursor));
    cursor = new Date(cursor.getFullYear(), cursor.getMonth() - 1, 1);
  }
  calendarEl.innerHTML = months.join("");
  monthRangeEl.textContent = `${formatMonthLabel(range.end)} \u2014 ${formatMonthLabel(range.start)} (${dateMap.size} trading days)`;
  wireDayClicks();
}

// Load data
function loadDiaryData() {
  loadBtn.disabled = true;
  loadBtn.textContent = "Loading...";
  calendarEl.innerHTML = `<div class="empty-card empty">Loading live trades...</div>`;

  fetch("/api/live-diary")
    .then((res) => res.json())
    .then((data) => {
      const trades = data.trades || [];
      dateMap = buildCalendarState(trades);
      renderCalendar();

      const dates = Array.from(dateMap.keys()).sort();
      if (dates.length) {
        selectedDate = dates[dates.length - 1];
        loadDateTrades(selectedDate);
      } else {
        selectedDateEl.textContent = "No live trades yet.";
      }
    })
    .catch((err) => {
      calendarEl.innerHTML = `<div class="empty-card empty">Error: ${err.message}</div>`;
    })
    .finally(() => {
      loadBtn.disabled = false;
      loadBtn.textContent = "Load Diary";
    });
}

loadBtn.addEventListener("click", loadDiaryData);
loadDiaryData();
