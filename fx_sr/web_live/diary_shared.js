/**
 * Shared utilities for backtest_diary.js and live_diary.js.
 *
 * Each page must define these globals before calling shared functions:
 *   - dateMap   (Map)   — built by that page's buildCalendarState()
 *   - selectedDate (string) — the currently-selected YYYY-MM-DD key
 *   - calendarEl, monthRangeEl, selectedDateEl (DOM elements)
 *   - selectDate(date)  — page-specific handler called on day click
 */

/* ── Constants ───────────────────────────────────────────────────────── */

const WEEKDAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri"];
const PRICE_DISPLAY_DECIMALS = 5;

/* ── Formatting helpers ──────────────────────────────────────────────── */

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;",
  }[char]));
}

function formatDateLabel(dateKey) {
  return parseDateKey(dateKey).toLocaleDateString(undefined, {
    weekday: "short",
    year: "numeric",
    month: "short",
    day: "2-digit",
    timeZone: "UTC",
  });
}

function formatMonthLabel(date) {
  return date.toLocaleDateString(undefined, {
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  });
}

function parseDateKey(value) {
  const [y, m, d] = String(value).split("-").map(Number);
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) {
    return new Date(NaN);
  }
  return new Date(Date.UTC(y, m - 1, d));
}

function formatDateKey(date) {
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-${String(date.getUTCDate()).padStart(2, "0")}`;
}

function isIsoDate(value) {
  if (!value || typeof value !== "string") return false;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(parsed.getTime()) && formatDateKey(parsed) === value;
}

function formatSigned(value, digits = 2, suffix = "") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "\u2013";
  const n = Number(value);
  return `${n > 0 ? "+" : ""}${n.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}${suffix}`;
}

function formatNumber(value, digits = PRICE_DISPLAY_DECIMALS) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "\u2013";
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatTime(isoTime) {
  if (!isoTime) return "\u2014";
  const parsed = new Date(isoTime);
  if (Number.isNaN(parsed.getTime())) return "\u2014";
  return parsed.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC",
  });
}

/* ── Calendar rendering ──────────────────────────────────────────────── */

function monthStartFromDate(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
}

function monthKeyFromDate(date) {
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
}

function getMonthRange(map) {
  const dates = Array.from(map.keys()).sort();
  if (!dates.length) return null;
  return {
    start: parseDateKey(dates[0]),
    end: parseDateKey(dates[dates.length - 1]),
  };
}

function renderMonth(monthDate) {
  const monthKey = monthKeyFromDate(monthDate);
  const monthStart = monthStartFromDate(monthDate);
  const monthEnd = new Date(Date.UTC(monthDate.getUTCFullYear(), monthDate.getUTCMonth() + 1, 0));
  const firstWeekday = monthStart.getUTCDay();  // Sun=0 start
  const lastWeekday = monthEnd.getUTCDay();
  const gridStart = new Date(monthStart);
  gridStart.setUTCDate(1 - firstWeekday);
  const gridEnd = new Date(monthEnd);
  // Pad to Friday (day 5); if lastWeekday is already Sat(6) pad wraps to next Fri
  const padDays = lastWeekday <= 5 ? (5 - lastWeekday) : 6;
  gridEnd.setUTCDate(monthEnd.getUTCDate() + padDays);

  const header = `<div class="diary-weekdays">${WEEKDAY_LABELS.map((d) => `<div>${d}</div>`).join("")}</div>`;
  const weeks = [];
  for (let c = new Date(gridStart); c <= gridEnd; c.setUTCDate(c.getUTCDate() + 7)) {
    const week = [];
    for (let i = 0; i < 7; i++) {
      const day = new Date(c);
      day.setUTCDate(c.getUTCDate() + i);
      if (day.getUTCDay() === 6) continue;
      week.push(renderDay(day, monthDate.getUTCMonth()));
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
  const isCurrentMonth = dayDate.getUTCMonth() === activeMonth;
  const dateKey = formatDateKey(dayDate);
  const dayState = isCurrentMonth ? dateMap.get(dateKey) : null;
  const isSelected = dateKey === selectedDate;
  const hasTrades = dayState && dayState.count > 0;
  const cls = ["diary-day"];
  if (!isCurrentMonth) cls.push("other-month", "outside");
  if (hasTrades) cls.push("has-trades");
  if (isSelected) cls.push("selected");
  if (hasTrades) {
    if (dayState.total_pnl_gbp > 0) cls.push("up");
    else if (dayState.total_pnl_gbp < 0) cls.push("down");
  }

  if (!isCurrentMonth) {
    return `<div class="${cls.join(" ")}"><span class="diary-day-number">${dayDate.getUTCDate()}</span></div>`;
  }
  if (!hasTrades) {
    return `<div class="${cls.join(" ")} no-trades"><span class="diary-day-number">${dayDate.getUTCDate()}</span><span class="diary-day-count">No trades</span></div>`;
  }

  return `
    <button class="${cls.join(" ")}" data-date="${dateKey}" type="button">
      <span class="diary-day-number">${dayDate.getUTCDate()}</span>
      <span class="diary-day-count">${dayState.count} trade${dayState.count === 1 ? "" : "s"}</span>
      <span class="diary-day-pl">${formatSigned(dayState.total_pnl_r, 2, "R")}</span>
      <span class="diary-day-pl">\u00a3${formatSigned(dayState.total_pnl_gbp, 2, "")}</span>
    </button>
  `;
}

function wireDayClicks() {
  calendarEl.querySelectorAll(".diary-day[data-date]").forEach((btn) => {
    btn.addEventListener("click", () => selectDate(btn.dataset.date));
  });
}
