const loadBtn = document.getElementById("load-btn");
const monthRangeEl = document.getElementById("month-range");
const summaryEl = document.getElementById("summary");
const selectedDateEl = document.getElementById("selected-date");
const calendarEl = document.getElementById("diary-calendar");
const bodyEl = document.getElementById("diary-body");

const WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

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

function formatDateLabel(dateKey) {
  const date = parseDateKey(dateKey);
  return date.toLocaleDateString(undefined, {
    weekday: "short",
    year: "numeric",
    month: "short",
    day: "2-digit",
  });
}

function formatMonthLabel(date) {
  return date.toLocaleDateString(undefined, {
    month: "long",
    year: "numeric",
  });
}

function parseDateKey(value) {
  const [year, month, day] = String(value).split("-").map((part) => Number(part));
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return new Date(NaN);
  }
  return new Date(year, month - 1, day);
}

function formatDateKey(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function isIsoDate(value) {
  if (!value || typeof value !== "string") return false;
  const match = /^\d{4}-\d{2}-\d{2}$/.test(value);
  if (!match) return false;
  const parsed = new Date(`${value}T00:00:00`);
  return !Number.isNaN(parsed.getTime()) && formatDateKey(parsed) === value;
}

function formatSigned(value, digits = 2, suffix = "") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "–";
  }
  const number = Number(value);
  const prefix = number > 0 ? "+" : "";
  return `${prefix}${Number(number).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}${suffix}`;
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

function replayDateForTrade(trade) {
  if (trade.entry_time) return String(trade.entry_time).slice(0, 10);
  if (trade.exit_time) return String(trade.exit_time).slice(0, 10);
  return "";
}

function openReplay(pair, date, preset) {
  if (!pair || !date) return;
  const params = new URLSearchParams({
    pair: String(pair).toUpperCase(),
    date,
  });
  if (preset) params.set('preset', preset);
  window.location.href = `/replay?${params.toString()}`;
}

function showMessage(message) {
  bodyEl.innerHTML = `<tr><td colspan=\"8\" class=\"empty\">${message}</td></tr>`;
  summaryEl.textContent = message;
}

function buildRows(trades, dateFilter = "") {
  if (!trades.length) {
    bodyEl.innerHTML = `<tr><td colspan="8" class="empty">No trades for this date.</td></tr>`;
    return;
  }

  bodyEl.innerHTML = trades.map((trade) => {
    const pnlClass = (trade.pnl_pips || 0) >= 0 ? "up" : "down";
    const directionClass = (trade.direction || "").toLowerCase();
    const digits = trade.decimals || 5;
    const tradeDate = dateFilter || replayDateForTrade(trade);
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

function buildCalendarState(trades) {
  const dateMap = new Map();
  for (const trade of trades) {
    const affectedDates = new Set();
    const entryDate = trade.entry_time ? String(trade.entry_time).slice(0, 10) : "";
    const exitDate = trade.exit_time ? String(trade.exit_time).slice(0, 10) : "";

    if (entryDate) affectedDates.add(entryDate);
    if (exitDate) affectedDates.add(exitDate);
    if (affectedDates.size === 0) continue;

    for (const date of affectedDates) {
      if (!isIsoDate(date)) continue;
      if (!dateMap.has(date)) {
        dateMap.set(date, {
          date,
          trades: [],
          count: 0,
          wins: 0,
          losses: 0,
          total_pnl_pips: 0,
          total_pnl_r: 0,
        });
      }
      const row = dateMap.get(date);
      row.trades.push(trade);
      row.count += 1;
      const pnlPips = Number(trade.pnl_pips) || 0;
      const pnlR = Number(trade.pnl_r) || 0;
      row.total_pnl_pips += pnlPips;
      row.total_pnl_r += pnlR;
      if (pnlPips > 0) row.wins += 1;
      if (pnlPips < 0) row.losses += 1;
    }
  }

  for (const row of dateMap.values()) {
    row.trades.sort((a, b) => {
      const aTime = String(a.entry_time || "");
      const bTime = String(b.entry_time || "");
      return bTime.localeCompare(aTime);
    });
    row.total_pnl_pips = Number(row.total_pnl_pips.toFixed(1));
    row.total_pnl_r = Number(row.total_pnl_r.toFixed(2));
  }

  return dateMap;
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
  const pair = selectReplayPair(dayData.trades);
  if (!pair) return;
  openReplay(pair, date);
}

function monthStartFromDate(date) {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function monthKeyFromDate(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function getMonthRange(dateMap) {
  const dates = Array.from(dateMap.keys()).sort();
  if (!dates.length) return null;
  return {
    start: parseDateKey(dates[0]),
    end: parseDateKey(dates[dates.length - 1]),
  };
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
    cursor = new Date(cursor.getFullYear(), cursor.getMonth() - 1, 1);
  }

  calendarEl.innerHTML = monthRows.join("");
  monthRangeEl.textContent = `${formatMonthLabel(range.end)} — ${formatMonthLabel(range.start)} (${dateMap.size} active days)`;
  wireDayClicks();
}

function renderMonth(monthDate) {
  const monthKey = monthKeyFromDate(monthDate);
  const monthStart = monthStartFromDate(monthDate);
  const monthEnd = new Date(monthDate.getFullYear(), monthDate.getMonth() + 1, 0);
  const firstWeekday = (monthStart.getDay() + 6) % 7; // Monday = 0
  const lastWeekday = (monthEnd.getDay() + 6) % 7; // Monday = 0
  const gridStart = new Date(monthStart);
  gridStart.setDate(1 - firstWeekday);
  const gridEnd = new Date(monthEnd);
  gridEnd.setDate(monthEnd.getDate() + (6 - lastWeekday));

  const header = `<div class="diary-weekdays">${WEEKDAY_LABELS.map((day) => `<div>${day}</div>`).join("")}</div>`;
  const weeks = [];
  for (let cursor = new Date(gridStart); cursor <= gridEnd; cursor.setDate(cursor.getDate() + 7)) {
    const week = [];
    for (let i = 0; i < 7; i++) {
      const day = new Date(cursor);
      day.setDate(cursor.getDate() + i);
      week.push(renderDay(day, monthDate.getMonth()));
    }
    weeks.push(`<div class="diary-week">${week.join("")}</div>`);
  }

  return `
    <section class="diary-month" data-month="${monthKey}">
      <h3>${formatMonthLabel(monthDate)}</h3>
      ${header}
      ${weeks.join("")}
    </section>
  `;
}

function renderDay(dayDate, activeMonth) {
  const isCurrentMonth = dayDate.getMonth() === activeMonth;
  const dateKey = formatDateKey(dayDate);
  const dayState = isCurrentMonth ? dateMap.get(dateKey) : null;
  const isSelected = dateKey === selectedDate;
  const hasTrades = dayState && dayState.count > 0;
  const dayClasses = ["diary-day"];
  if (!isCurrentMonth) dayClasses.push("other-month");
  if (!isCurrentMonth) {
    dayClasses.push("outside");
  }
  if (hasTrades) dayClasses.push("has-trades");
  if (isSelected) dayClasses.push("selected");
  if (hasTrades) {
    dayClasses.push(dayState.total_pnl_pips >= 0 ? "up" : "down");
  }

  if (!isCurrentMonth) {
    return `<div class="${dayClasses.join(" ")}"><span class="diary-day-number">${dayDate.getDate()}</span></div>`;
  }

  if (!hasTrades) {
    return `<div class="${dayClasses.join(" ")} no-trades"><span class="diary-day-number">${dayDate.getDate()}</span><span class="diary-day-count">No trades</span></div>`;
  }

  const pnlDisplay = hasTrades ? formatSigned(dayState.total_pnl_pips, 1, "p") : "—";
  const countDisplay = hasTrades ? `${dayState.count} trade${dayState.count === 1 ? "" : "s"}` : "No trades";

  return `
    <button class="${dayClasses.join(" ")}" data-date="${dateKey}" type="button">
      <span class="diary-day-number">${dayDate.getDate()}</span>
      <span class="diary-day-count">${countDisplay}</span>
      <span class="diary-day-pl">${pnlDisplay}</span>
    </button>
  `;
}

function wireDayClicks() {
  calendarEl.querySelectorAll(".diary-day[data-date]").forEach((dayBtn) => {
    const date = dayBtn.dataset.date;
    dayBtn.addEventListener("click", () => selectDate(date));
  });
}

function selectDate(date) {
  if (!isIsoDate(date)) return;
  selectedDate = date;
  renderCalendar();
  openReplayForDate(date);
}

function renderDateSummary(date, data) {
  const selected = data || {
    count: 0,
    wins: 0,
    losses: 0,
    total_pnl_pips: 0,
    total_pnl_r: 0,
  };

  const cls = (selected.total_pnl_pips || 0) >= 0 ? "up" : "down";
  selectedDateEl.textContent = `${formatDateLabel(date)} · ${selected.count} trade${selected.count === 1 ? "" : "s"}`;
  summaryEl.innerHTML = `
    <strong>${date}</strong> — ${selected.count} trade${selected.count === 1 ? "" : "s"} (W/L ${selected.wins}/${selected.losses})
    · P/L: <span class="${cls}">${formatSigned(selected.total_pnl_pips, 1, " pips")}</span>
    · R: ${formatSigned(selected.total_pnl_r, 2, "R")}
  `;
}

function loadDateTrades(date) {
  const dayData = dateMap.get(date);
  if (!dayData) {
    summaryEl.textContent = `${date} — no trades in cache for this day`;
    bodyEl.innerHTML = `<tr><td colspan="8" class="empty">No trades for this date.</td></tr>`;
    return;
  }

  renderDateSummary(date, dayData);
  buildRows(dayData.trades || [], date);
}

function showLoading(message) {
  calendarEl.innerHTML = `<div class=\"empty-card empty\">${message}</div>`;
  monthRangeEl.textContent = "";
  selectedDateEl.textContent = "Loading diary...";
  bodyEl.innerHTML = `<tr><td colspan="8" class="empty">Load diary to view trades.</td></tr>`;
  summaryEl.textContent = message;
}

function loadDiaryData() {
  loadBtn.disabled = true;
  loadBtn.textContent = "Loading...";
  showLoading("Loading trades cache…");

  return fetch("/api/backtest/trades")
    .then((res) => res.json().then((data) => ({ ok: res.ok, data })))
    .then(({ ok, data }) => {
      if (!ok) {
        throw new Error(data.error || "Unable to load backtest trades.");
      }
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
        bodyEl.innerHTML = `<tr><td colspan=\"8\" class=\"empty\">No trades available in cache.</td></tr>`;
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

let dateMap = new Map();
let selectedDate = "";

loadBtn.addEventListener("click", loadDiaryData);
loadDiaryData();
