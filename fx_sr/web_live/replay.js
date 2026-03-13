/* Strategy Replay — progressive chart playback with TradingView Lightweight Charts */

const PAIRS = [
  'AUDCAD','AUDJPY','AUDNZD','AUDUSD',
  'CADJPY','CHFJPY',
  'EURAUD','EURCAD','EURCHF','EURGBP','EURJPY','EURUSD',
  'GBPAUD','GBPCAD','GBPCHF','GBPJPY','GBPUSD',
  'NZDJPY','NZDUSD',
  'USDCAD','USDCHF','USDJPY',
];
const BACKTEST_CURRENCY = 'GBP';

const replay = {
  frames: [],
  contextBars: [],
  zones: [],
  summary: null,
  pair: '',
  currentIndex: -1,
  isPlaying: false,
  speed: 1,
  playTimer: null,
  chart: null,
  candleSeries: null,
  markers: [],
  zoneLines: [],
  allTrades: [],
  navigationTrades: [],
  tradeNavCache: {},
  activeTradeIndex: -1,
};

// DOM refs
const pairSelect   = document.getElementById('pair-select');
const dateInput    = document.getElementById('date-input');
const presetSelect = document.getElementById('preset-select');
const loadBtn      = document.getElementById('load-btn');
const errorBanner  = document.getElementById('error-banner');
const chartEl      = document.getElementById('chart-container');
const playbackRow  = document.getElementById('playback-row');
const infoGrid     = document.getElementById('info-grid');
const tfSelect     = document.getElementById('tf-select');
const refreshBtn   = document.getElementById('refresh-btn');
const prevTradeBtn = document.getElementById('prev-trade-btn');
const nextTradeBtn = document.getElementById('next-trade-btn');
const tradeNavLabel = document.getElementById('trade-nav-label');

// ── Init ──

PAIRS.forEach(p => {
  const opt = document.createElement('option');
  opt.value = p; opt.textContent = p;
  pairSelect.appendChild(opt);
});

const urlParams = new URLSearchParams(window.location.search);

// Pre-select pair/date from query string (e.g. /replay?pair=EURUSD&date=2026-03-10)
const urlPair = urlParams.get('pair');
if (urlPair && PAIRS.includes(urlPair.toUpperCase())) {
  pairSelect.value = urlPair.toUpperCase();
}
const urlPreset = urlParams.get('preset');
const urlTimeframe = urlParams.get('tf');
let requestedBacktestKey = urlParams.get('backtest') || '';
if (urlTimeframe && Array.from(tfSelect.options).some((option) => option.value === urlTimeframe)) {
  tfSelect.value = urlTimeframe;
}

const urlDate = isIsoDate(urlParams.get('date')) ? urlParams.get('date') : '';
let requestedReplayDate = urlDate;
let requestedTradeEntry = urlParams.get('entry') || '';
if (urlDate) {
  dateInput.value = urlDate;
}

pairSelect.addEventListener('change', () => { fetchDateRange().then(() => loadReplay()); });
loadBtn.addEventListener('click', loadReplay);
tfSelect.addEventListener('change', loadReplay);
refreshBtn.addEventListener('click', refreshData);
prevTradeBtn.addEventListener('click', () => navigateTrade(-1));
nextTradeBtn.addEventListener('click', () => navigateTrade(1));

// Fetch available presets from server, then init
fetchPresets().then(() => {
  fetchDateRange().then(() => {
    if (!dateInput.value) {
      const today = new Date().toISOString().slice(0, 10);
      dateInput.value = today;
    }
    loadReplay();
  });
});

async function fetchPresets() {
  try {
    const res = await fetch('/api/replay/presets');
    if (!res.ok) return;
    const data = await res.json();
    const presets = data.presets || [];
    if (!presets.length) return;

    presetSelect.innerHTML = '';
    presets.forEach(p => {
      const opt = document.createElement('option');
      opt.value = p.name;
      opt.textContent = p.name;
      opt.title = p.description || '';
      presetSelect.appendChild(opt);
    });

    // Select from URL param, or default to high_volume, or first
    if (urlPreset && presets.some(p => p.name === urlPreset)) {
      presetSelect.value = urlPreset;
    } else if (presets.some(p => p.name === 'high_volume')) {
      presetSelect.value = 'high_volume';
    }
  } catch (_) { /* keep hardcoded fallback */ }
}

// ── Date range ──

async function fetchDateRange() {
  const pair = pairSelect.value;
  if (!pair) return;
  try {
    const res = await fetch(`/api/replay/dates?pair=${pair}`);
    if (!res.ok) return;
    const data = await res.json();
    const requestedDate = requestedReplayDate || dateInput.value;
    dateInput.min = data.first_date;
    dateInput.max = data.last_date;
    const requestedInRange = (
      requestedDate
      && requestedDate >= data.first_date
      && requestedDate <= data.last_date
    );
    const currentOutOfRange = (
      !dateInput.value
      || dateInput.value < data.first_date
      || dateInput.value > data.last_date
    );

    if (requestedInRange) {
      dateInput.value = requestedDate;
    } else if (currentOutOfRange) {
      dateInput.value = data.last_date;
    }
    requestedReplayDate = '';
  } catch (_) { /* ignore */ }
}

// ── Load replay data ──

async function loadReplay() {
  const pair = pairSelect.value;
  const dateVal = dateInput.value;
  const preset = presetSelect.value;
  const tf = tfSelect.value;
  const backtestKey = requestedBacktestKey;

  if (!pair || !dateVal) {
    showError('Select a pair and date');
    return;
  }

  loadBtn.disabled = true;
  loadBtn.textContent = 'Loading...';
  hideError();
  stopPlay();

  try {
    const [trades, res] = await Promise.all([
      fetchPairTradeNavigation(pair, backtestKey),
      fetch(`/api/replay?pair=${pair}&date=${dateVal}&preset=${preset}&tf=${tf}&backtest=${encodeURIComponent(backtestKey)}`),
    ]);
    const data = await res.json();

    if (!res.ok) {
      replay.navigationTrades = trades || [];
      syncTradeNavigation(dateVal, requestedTradeEntry);
      showError(data.error || 'Failed to load replay data');
      return;
    }

    replay.pair = pair;
    replay.frames = data.frames;
    replay.contextBars = data.context_bars || [];
    replay.zones = data.zones;
    replay.summary = data.summary;
    replay.allTrades = (trades && trades.length > 0) ? trades : (data.all_completed_trades || []);
    replay.currentIndex = -1;
    replay.markers = [];
    replay.navigationTrades = trades || [];
    syncTradeNavigation(dateVal, requestedTradeEntry);
    const activeTrade = replay.navigationTrades[replay.activeTradeIndex];
    requestedTradeEntry = activeTrade?.entry_time || '';
    updateReplayUrl(pair, dateVal, preset, tf, requestedTradeEntry, backtestKey);

    initChart();
    drawZoneLines();

    playbackRow.style.display = 'flex';
    infoGrid.style.display = '';

    // Show incomplete banner for partial days
    const incompleteBanner = document.getElementById('incomplete-banner');
    incompleteBanner.style.display = data.summary?.incomplete ? 'block' : 'none';

    // Warn if minute data was requested but unavailable
    const tfBanner = document.getElementById('tf-fallback-banner');
    if (data.summary?.timeframe_requested === '1m' && data.summary?.timeframe === '1h') {
      tfBanner.style.display = 'block';
    } else {
      tfBanner.style.display = 'none';
    }

    renderSummary();
    if (replay.frames.length > 0) {
      if (requestedTradeEntry) {
        if (!focusTradeOnChart(activeTrade, requestedTradeEntry)) {
          stepTo(replay.frames.length - 1);
        }
      } else {
        stepTo(replay.frames.length - 1);
      }
    } else {
      // No target-day bars — still render context bars and trades
      const candles = replay.contextBars
        .map((b) => {
          const time = parseTime(b.time);
          if (time == null) return null;
          return { time, open: b.open, high: b.high, low: b.low, close: b.close };
        })
        .filter(Boolean);
      replay.candleSeries.setData(candles);
      replay.zoneSeries.setData(candles.map(c => ({ time: c.time, value: c.close })));
      // Render trades panel with empty frame
      renderInfo(-1);
    }
  } catch (err) {
    showError('Network error: ' + err.message);
  } finally {
    loadBtn.disabled = false;
    loadBtn.textContent = 'Load';
  }
}

function isIsoDate(value) {
  if (!value || typeof value !== 'string') return false;
  const match = /^\d{4}-\d{2}-\d{2}$/.test(value);
  if (!match) return false;
  const parsed = new Date(value);
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

function normalizeTimestampInput(value) {
  if (value == null) return '';
  const normalized = String(value).trim();
  if (!normalized) return '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) return `${normalized}T00:00:00Z`;
  if (normalized.includes(' ') && !normalized.includes('T')) {
    return normalized.replace(' ', 'T');
  }
  return normalized;
}

function parseTimestamp(value) {
  const normalized = normalizeTimestampInput(value);
  if (!normalized) return null;
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function parseUnixTime(value) {
  const parsed = parseTimestamp(value);
  return parsed ? Math.floor(parsed.getTime() / 1000) : null;
}

function formatTimestamp(value, options) {
  const parsed = parseTimestamp(value);
  if (!parsed) return String(value || '');
  return parsed.toLocaleString([], options);
}

function escapeAttr(value) {
  return String(value ?? '').replace(/'/g, '&#39;');
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '—';
  }
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatCurrency(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '—';
  }
  return `${BACKTEST_CURRENCY} ${formatNumber(value, digits)}`;
}

function formatSignedCurrency(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '—';
  }
  const number = Number(value);
  const prefix = number > 0 ? '+' : number < 0 ? '-' : '';
  return `${prefix}${BACKTEST_CURRENCY} ${formatNumber(Math.abs(number), digits)}`;
}

function buildAccountSummaryLabel(account) {
  if (!account) return '';

  const parts = [];
  if (account.starting_balance !== null && account.starting_balance !== undefined) {
    parts.push(`starts at ${formatCurrency(account.starting_balance)}`);
  }
  if (account.risk_pct !== null && account.risk_pct !== undefined) {
    parts.push(`${formatNumber(account.risk_pct, 2)}% risk`);
  }

  let summary = parts.join(' @ ');
  if (account.profile_name) {
    summary += `${summary ? ' ' : ''}(${account.profile_name})`;
  }
  if (!summary) return '';

  if (account.assumed) {
    summary = `assumed ${summary}`;
  }
  if (account.mixed_params) {
    summary += ' · mixed cached parameter sets';
  }
  return summary;
}

function getActiveReplayTrade() {
  const trades = replay.navigationTrades || [];
  if (replay.activeTradeIndex < 0 || replay.activeTradeIndex >= trades.length) {
    return null;
  }
  return trades[replay.activeTradeIndex];
}

function getTradeFocusTimestamp(trade, fallbackEntry = '') {
  const entryDate = String(trade?.entry_time || fallbackEntry || '').slice(0, 10);
  const exitDate = String(trade?.exit_time || '').slice(0, 10);
  if (entryDate && exitDate && exitDate !== entryDate) {
    return parseTimestamp(`${exitDate}T23:59:59Z`);
  }
  const exitTimestamp = parseTimestamp(trade?.exit_time);
  if (exitTimestamp) return exitTimestamp;
  return parseTimestamp(trade?.entry_time || fallbackEntry);
}

function focusTradeOnChart(trade, fallbackEntry = '') {
  const focusTime = getTradeFocusTimestamp(trade, fallbackEntry);
  if (!focusTime) return false;
  _scrubToTime(focusTime);
  return true;
}

function updateReplayUrl(pair, date, preset, tf, entry = '', backtest = '') {
  const params = new URLSearchParams(window.location.search);
  params.set('pair', pair);
  params.set('date', date);
  params.set('preset', preset);
  params.set('tf', tf);
  if (backtest) {
    params.set('backtest', backtest);
  } else {
    params.delete('backtest');
  }
  if (entry) {
    params.set('entry', entry);
  } else {
    params.delete('entry');
  }
  requestedBacktestKey = backtest;
  window.history.replaceState({}, '', `/replay?${params.toString()}`);
}

async function fetchPairTradeNavigation(pair, backtestKey = '') {
  if (!pair) {
    replay.navigationTrades = [];
    return [];
  }
  const cacheKey = `${pair}|${backtestKey}`;
  if (replay.tradeNavCache[cacheKey]) {
    replay.navigationTrades = replay.tradeNavCache[cacheKey];
    return replay.navigationTrades;
  }

  try {
    const params = new URLSearchParams({ pair });
    if (backtestKey) {
      params.set('backtest', backtestKey);
    }
    const res = await fetch(`/api/backtest/trades?${params.toString()}`);
    const data = await res.json();
    if (!res.ok) {
      replay.navigationTrades = [];
      return [];
    }

    const trades = [...(data.trades || [])].sort((a, b) =>
      String(a.entry_time || '').localeCompare(String(b.entry_time || ''))
    );
    replay.tradeNavCache[cacheKey] = trades;
    replay.navigationTrades = trades;
    return trades;
  } catch (_) {
    replay.navigationTrades = [];
    return [];
  }
}

function findTradeIndexByEntryTime(entryTime) {
  if (!entryTime) return -1;
  return replay.navigationTrades.findIndex((trade) => trade.entry_time === entryTime);
}

function findTradeIndexForSelection(selectedDate, entryTime = '') {
  const trades = replay.navigationTrades || [];
  if (!trades.length) return -1;

  const exactIndex = findTradeIndexByEntryTime(entryTime);
  if (exactIndex >= 0) return exactIndex;

  const entryDateIndex = trades.findIndex((trade) =>
    String(trade.entry_time || '').slice(0, 10) === selectedDate
  );
  if (entryDateIndex >= 0) return entryDateIndex;

  const activeDateIndex = trades.findIndex((trade) => tradeTouchesSelectedDate(trade, selectedDate));
  if (activeDateIndex >= 0) return activeDateIndex;

  const selectedTs = parseTimestamp(selectedDate)?.getTime();
  if (selectedTs == null) return 0;

  const nextIndex = trades.findIndex((trade) => {
    const entryTs = parseTimestamp(trade.entry_time)?.getTime();
    return entryTs != null && entryTs >= selectedTs;
  });
  if (nextIndex >= 0) return nextIndex;
  return trades.length - 1;
}

function formatTradeNavLabel(trade, index, total) {
  if (!trade) return 'No trade selected';
  const entryText = formatTimestamp(trade.entry_time, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  return `Trade ${index + 1} / ${total} · ${entryText} · ${trade.direction}`;
}

function syncTradeNavigation(selectedDate, entryTime = '') {
  replay.activeTradeIndex = findTradeIndexForSelection(selectedDate, entryTime);
  updateTradeNavigation();
}

function updateTradeNavigation() {
  const trades = replay.navigationTrades || [];
  if (!trades.length) {
    tradeNavLabel.textContent = 'No cached trades for pair';
    prevTradeBtn.disabled = true;
    nextTradeBtn.disabled = true;
    renderSummary();
    return;
  }

  if (replay.activeTradeIndex < 0 || replay.activeTradeIndex >= trades.length) {
    tradeNavLabel.textContent = `${trades.length} cached trade${trades.length === 1 ? '' : 's'}`;
    prevTradeBtn.disabled = true;
    nextTradeBtn.disabled = true;
    renderSummary();
    return;
  }

  const trade = trades[replay.activeTradeIndex];
  tradeNavLabel.textContent = formatTradeNavLabel(trade, replay.activeTradeIndex, trades.length);
  prevTradeBtn.disabled = replay.activeTradeIndex <= 0;
  nextTradeBtn.disabled = replay.activeTradeIndex >= trades.length - 1;
  renderSummary();
}

async function selectTradeByIndex(index) {
  const trades = replay.navigationTrades || [];
  if (index < 0 || index >= trades.length) return;

  const trade = trades[index];
  replay.activeTradeIndex = index;
  requestedTradeEntry = trade.entry_time || '';
  updateTradeNavigation();

  const tradeDate = String(trade.entry_time || '').slice(0, 10);
  if (!tradeDate) return;

  if (dateInput.value !== tradeDate || replay.summary?.date !== tradeDate) {
    dateInput.value = tradeDate;
    await loadReplay();
    return;
  }

  updateReplayUrl(
    pairSelect.value,
    tradeDate,
    presetSelect.value,
    tfSelect.value,
    requestedTradeEntry,
    requestedBacktestKey,
  );
  focusTradeOnChart(trade, requestedTradeEntry);
}

async function navigateTrade(offset) {
  const trades = replay.navigationTrades || [];
  if (!trades.length) return;

  let currentIndex = replay.activeTradeIndex;
  if (currentIndex < 0) {
    currentIndex = findTradeIndexForSelection(dateInput.value, requestedTradeEntry);
  }

  if (currentIndex < 0) {
    currentIndex = offset > 0 ? -1 : trades.length;
  }

  const nextIndex = Math.max(0, Math.min(trades.length - 1, currentIndex + offset));
  if (nextIndex === currentIndex) return;
  await selectTradeByIndex(nextIndex);
}

async function refreshData() {
  const pair = pairSelect.value;
  if (!pair) { showError('Select a pair first'); return; }

  refreshBtn.disabled = true;
  refreshBtn.textContent = 'Fetching...';
  hideError();

  try {
    const res = await fetch(`/api/replay/refresh?pair=${pair}`, { method: 'POST' });
    const data = await res.json();
    if (!res.ok) { showError(data.error || 'Refresh failed'); return; }

    // Update date range and auto-reload
    await fetchDateRange();
    loadReplay();
  } catch (err) {
    showError('Network error: ' + err.message);
  } finally {
    refreshBtn.disabled = false;
    refreshBtn.textContent = 'Update Data';
  }
}

// ── Chart ──

function initChart() {
  if (replay.chart) { replay.chart.remove(); }
  replay.zoneLines = [];
  const dec = replay.summary?.decimals || 5;
  const priceFormat = {
    type: 'price',
    precision: dec,
    minMove: Math.pow(10, -dec),
  };

  replay.chart = LightweightCharts.createChart(chartEl, {
    width: chartEl.clientWidth,
    height: chartEl.clientHeight,
    layout: {
      background: { color: '#fffbf5' },
      textColor: '#1f1a17',
      fontFamily: '"Aptos", "Segoe UI Variable Text", sans-serif',
    },
    grid: {
      vertLines: { color: 'rgba(53,43,34,0.06)' },
      horzLines: { color: 'rgba(53,43,34,0.06)' },
    },
    localization: {
      priceFormatter: (value) => Number(value).toFixed(dec),
    },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: {
      borderColor: 'rgba(53,43,34,0.12)',
    },
    timeScale: {
      borderColor: 'rgba(53,43,34,0.12)',
      timeVisible: true,
      secondsVisible: false,
      rightOffset: 12,
    },
  });

  // Invisible line series added first — zone price lines attach here (renders behind candles)
  replay.zoneSeries = replay.chart.addLineSeries({
    color: 'transparent',
    lineWidth: 0,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
    priceFormat,
  });

  replay.candleSeries = replay.chart.addCandlestickSeries({
    upColor: '#1f7a49',
    downColor: '#b23b29',
    borderUpColor: '#1f7a49',
    borderDownColor: '#b23b29',
    wickUpColor: '#1f7a49',
    wickDownColor: '#b23b29',
    priceFormat,
  });

  // Resize handler
  const ro = new ResizeObserver(() => {
    replay.chart.applyOptions({ width: chartEl.clientWidth });
  });
  ro.observe(chartEl);
}

function drawZoneLines() {
  // Remove old lines
  replay.zoneLines.forEach(l => replay.zoneSeries.removePriceLine(l));
  replay.zoneLines = [];

  replay.zones.forEach(z => {
    const isSupport = z.zone_type === 'support';
    const color = isSupport ? '#2f637d' : '#b95728';
    const labelPrefix = isSupport ? 'S' : 'R';

    const label = `${labelPrefix} (${z.touches}t)`;
    const upper = replay.zoneSeries.createPriceLine({
      price: z.upper,
      color: color,
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dotted,
      axisLabelVisible: false,
      title: '',
    });
    const lower = replay.zoneSeries.createPriceLine({
      price: z.lower,
      color: color,
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dotted,
      axisLabelVisible: false,
      title: '',
    });
    const mid = replay.zoneSeries.createPriceLine({
      price: z.midpoint,
      color: color,
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dashed,
      axisLabelVisible: true,
      title: label,
    });

    replay.zoneLines.push(upper, lower, mid);
  });
}

function parseTime(timeStr) {
  return parseUnixTime(timeStr);
}

function tradeActiveDates(trade) {
  if (Array.isArray(trade?.active_dates) && trade.active_dates.length) {
    return trade.active_dates.map((value) => String(value));
  }

  const dates = [];
  if (trade?.entry_time) dates.push(String(trade.entry_time).slice(0, 10));
  const exitDate = trade?.exit_time ? String(trade.exit_time).slice(0, 10) : '';
  if (exitDate && !dates.includes(exitDate)) dates.push(exitDate);
  return dates;
}

function tradeTouchesSelectedDate(trade, selectedDate) {
  if (!selectedDate) return false;
  return tradeActiveDates(trade).includes(selectedDate);
}

function renderToFrame(targetIndex) {
  if (targetIndex < 0 || targetIndex >= replay.frames.length) return;

  // Start with context bars (prior days, always fully visible)
  const candles = replay.contextBars
    .map((b) => {
      const time = parseTime(b.time);
      if (time == null) return null;
      return { time, open: b.open, high: b.high, low: b.low, close: b.close };
    })
    .filter(Boolean);
  const markers = [];

  // Add entry/exit markers onto context bars for trades involving the selected day
  const selectedDate = replay.summary?.date || '';
  const contextTimes = new Set(candles.map(c => c.time));
  const relevantTrades = (replay.allTrades || []).filter(t => tradeTouchesSelectedDate(t, selectedDate));
  for (const t of relevantTrades) {
    const entryTs = parseTime(t.entry_time);
    if (entryTs != null && contextTimes.has(entryTs)) {
      markers.push({
        time: entryTs,
        position: t.direction === 'LONG' ? 'belowBar' : 'aboveBar',
        color: t.direction === 'LONG' ? '#1f7a49' : '#b23b29',
        shape: t.direction === 'LONG' ? 'arrowUp' : 'arrowDown',
        text: t.direction,
      });
    }
    if (t.exit_time) {
      const exitTs = parseTime(t.exit_time);
      if (exitTs != null && contextTimes.has(exitTs)) {
        const win = (t.pnl_pips || 0) > 0;
        markers.push({
          time: exitTs,
          position: 'inBar',
          color: win ? '#1f7a49' : '#b23b29',
          shape: 'circle',
          text: `${t.exit_reason} ${win ? '+' : ''}${t.pnl_pips}p`,
        });
      }
    }
  }

  // Append target-day bars up to current index
  for (let i = 0; i <= targetIndex; i++) {
    const f = replay.frames[i];
    const t = parseTime(f.time);
    if (t == null) continue;
    candles.push({ time: t, open: f.open, high: f.high, low: f.low, close: f.close });

    // Entry signal marker
    if (f.signal) {
      markers.push({
        time: t,
        position: f.signal.direction === 'LONG' ? 'belowBar' : 'aboveBar',
        color: f.signal.direction === 'LONG' ? '#1f7a49' : '#b23b29',
        shape: f.signal.direction === 'LONG' ? 'arrowUp' : 'arrowDown',
        text: f.signal.direction,
      });
    }

    // Exit marker
    if (f.exit) {
      const win = f.exit.pnl_pips > 0;
      markers.push({
        time: t,
        position: 'inBar',
        color: win ? '#1f7a49' : '#b23b29',
        shape: 'circle',
        text: `${f.exit.reason} ${f.exit.pnl_pips > 0 ? '+' : ''}${f.exit.pnl_pips}p`,
      });
    }
  }

  replay.candleSeries.setData(candles);
  // Feed zone series same time range so its price lines render (behind candles)
  replay.zoneSeries.setData(candles.map(c => ({ time: c.time, value: c.close })));
  // Markers must be sorted by time for Lightweight Charts
  markers.sort((a, b) => a.time - b.time);
  replay.candleSeries.setMarkers(markers);

  // Open trade SL/TP lines
  removeTradeLevels();
  const currentFrame = replay.frames[targetIndex];
  if (currentFrame.open_trade) {
    const t = currentFrame.open_trade;
    const slColor = '#b23b29';
    const tpColor = '#1f7a49';

    replay._slLine = replay.candleSeries.createPriceLine({
      price: t.sl_price, color: slColor, lineWidth: 2,
      lineStyle: LightweightCharts.LineStyle.Solid,
      axisLabelVisible: true, title: 'SL',
    });
    replay._tpLine = replay.candleSeries.createPriceLine({
      price: t.tp_price, color: tpColor, lineWidth: 2,
      lineStyle: LightweightCharts.LineStyle.Solid,
      axisLabelVisible: true, title: 'TP',
    });
    replay._entryLine = replay.candleSeries.createPriceLine({
      price: t.entry_price, color: '#b88917', lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dashed,
      axisLabelVisible: true, title: 'Entry',
    });
  }
}

function removeTradeLevels() {
  if (replay._slLine) { replay.candleSeries.removePriceLine(replay._slLine); replay._slLine = null; }
  if (replay._tpLine) { replay.candleSeries.removePriceLine(replay._tpLine); replay._tpLine = null; }
  if (replay._entryLine) { replay.candleSeries.removePriceLine(replay._entryLine); replay._entryLine = null; }
}

// ── Playback ──

function jumpToTradeEntry(entryTime) {
  const dt = parseTimestamp(entryTime);
  if (!dt) return;
  const tradeDate = dt.toISOString().slice(0, 10);
  const currentDate = dateInput.value;
  const exactIndex = findTradeIndexByEntryTime(entryTime);
  const selectedTrade = exactIndex >= 0 ? replay.navigationTrades[exactIndex] : null;
  if (exactIndex >= 0) {
    replay.activeTradeIndex = exactIndex;
  }
  requestedTradeEntry = entryTime;
  updateTradeNavigation();

  if (tradeDate !== currentDate) {
    // Different day — update date picker and reload
    dateInput.value = tradeDate;
    loadReplay();
  } else {
    updateReplayUrl(
      pairSelect.value,
      tradeDate,
      presetSelect.value,
      tfSelect.value,
      requestedTradeEntry,
      requestedBacktestKey,
    );
    focusTradeOnChart(selectedTrade, requestedTradeEntry);
  }
}

function _scrubToTime(dt) {
  if (!(dt instanceof Date) || Number.isNaN(dt.getTime())) return;
  const target = Math.floor(dt.getTime() / 1000);
  let best = 0;
  for (let i = 0; i < replay.frames.length; i++) {
    const ft = parseUnixTime(replay.frames[i].time);
    if (ft != null && ft <= target) best = i;
  }
  stopPlay();
  stepTo(best);
}

function stepTo(index) {
  if (index < 0) index = 0;
  if (index >= replay.frames.length) index = replay.frames.length - 1;
  if (index === replay.currentIndex) return;

  replay.currentIndex = index;

  renderToFrame(index);
  renderInfo(index);

}

function stopPlay() {
  replay.isPlaying = false;
  if (replay.playTimer) { clearInterval(replay.playTimer); replay.playTimer = null; }
}

// ── Info panels ──

function _infoRow(label, value) {
  return `<div class="info-row"><span class="info-label">${label}</span><span>${value}</span></div>`;
}

function renderInfo(index) {
  const f = replay.frames[index];
  const dec = replay.summary?.decimals || 5;

  if (!f) {
    // No frames for target day — clear bar/trade panels but still show trades list
    document.getElementById('bar-details').innerHTML = '<div class="info-row" style="color:var(--muted)">No bars for selected date</div>';
    document.getElementById('trade-details').innerHTML = '<div class="info-row" style="color:var(--muted)">No data</div>';
    _renderTradeList(dec);
    return;
  }

  // Bar info
  const timeStr = formatTimestamp(f.time, {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', hour12:false});
  let barHtml = '';
  barHtml += _infoRow('Time', timeStr);
  barHtml += _infoRow('Open', f.open.toFixed(dec));
  barHtml += _infoRow('High', f.high.toFixed(dec));
  barHtml += _infoRow('Low', f.low.toFixed(dec));
  barHtml += _infoRow('Close', f.close.toFixed(dec));
  if (f.nearest_support) {
    barHtml += _infoRow('Support', `${f.nearest_support.lower.toFixed(dec)} \u2013 ${f.nearest_support.upper.toFixed(dec)} (${f.nearest_support.touches}t)`);
  }
  if (f.nearest_resistance) {
    barHtml += _infoRow('Resistance', `${f.nearest_resistance.lower.toFixed(dec)} \u2013 ${f.nearest_resistance.upper.toFixed(dec)} (${f.nearest_resistance.touches}t)`);
  }
  document.getElementById('bar-details').innerHTML = barHtml;

  // Trade state
  let tradeHtml = '';
  if (f.exit) {
    const cls = f.exit.pnl_pips > 0 ? 'up' : 'down';
    tradeHtml += _infoRow('Exit', `${f.exit.reason} @ ${f.exit.price.toFixed(dec)}`);
    tradeHtml += `<div class="info-row"><span class="info-label">P&L</span><span class="${cls}">${f.exit.pnl_pips > 0 ? '+' : ''}${f.exit.pnl_pips}p (${f.exit.pnl_r > 0 ? '+' : ''}${f.exit.pnl_r}R)</span></div>`;
  }
  if (f.open_trade) {
    const openSince = formatTimestamp(f.open_trade.entry_time, {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', hour12:false});
    const label = f.signal ? 'Entry' : 'Open';
    tradeHtml += `<div class="info-row"><span class="pill pill-${f.open_trade.direction.toLowerCase()}" style="font-size:0.68rem;padding:2px 6px;min-width:auto">${f.open_trade.direction}</span><span>${label} @ ${f.open_trade.entry_price.toFixed(dec)}${!f.signal ? ' since ' + openSince : ''}</span></div>`;
    tradeHtml += _infoRow('SL', f.open_trade.sl_price.toFixed(dec));
    tradeHtml += _infoRow('TP', f.open_trade.tp_price.toFixed(dec));
    tradeHtml += _infoRow('Bars held', f.open_trade.bars_held);
  } else if (f.signal) {
    tradeHtml += `<div class="info-row"><span class="pill pill-${f.signal.direction.toLowerCase()}" style="font-size:0.68rem;padding:2px 6px;min-width:auto">${f.signal.direction}</span><span>Entry @ ${f.signal.entry_price.toFixed(dec)}</span></div>`;
    tradeHtml += _infoRow('SL', f.signal.sl_price.toFixed(dec));
    tradeHtml += _infoRow('TP', f.signal.tp_price.toFixed(dec));
  }
  if (!f.signal && !f.exit && !f.open_trade) {
    tradeHtml = '<div class="info-row" style="color:var(--muted)">Flat \u2014 no position</div>';
  }
  document.getElementById('trade-details').innerHTML = tradeHtml;

  _renderTradeList(dec);
}

function _renderTradeList(dec) {
  const trades = replay.allTrades || [];
  const selectedDate = replay.summary?.date || '';
  const isSelectedDay = t => tradeTouchesSelectedDate(t, selectedDate);
  const todayTrades = trades.filter(isSelectedDay);
  const otherTrades = trades.filter(t => !isSelectedDay(t));

  const renderTrade = (t) => {
    const cls = (t.pnl_pips || 0) > 0 ? 'up' : 'down';
    const entryDt = formatTimestamp(t.entry_time, {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', hour12:false});
    const exitDt = t.exit_time ? formatTimestamp(t.exit_time, {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', hour12:false}) : '';
    return `<div class="trade-row" style="flex-wrap:wrap;gap:2px;cursor:pointer" onclick="jumpToTradeEntry('${escapeAttr(t.entry_time)}')">
      <span><span class="pill pill-${t.direction.toLowerCase()}" style="font-size:0.68rem;padding:2px 6px;min-width:auto">${t.direction}</span> @ ${t.entry_price.toFixed(dec)}${t.exit_price ? ' \u2192 ' + t.exit_price.toFixed(dec) : ''}</span>
      <span class="${cls}">${t.exit_reason} ${(t.pnl_pips || 0) > 0 ? '+' : ''}${t.pnl_pips}p (${(t.pnl_r || 0) > 0 ? '+' : ''}${t.pnl_r}R)</span>
      <span style="width:100%;font-size:0.76rem;color:var(--muted)">\u{1F4C4} Cached backtest \u00b7 ${entryDt}${exitDt ? ' \u2192 ' + exitDt : ''}</span>
    </div>`;
  };

  let listHtml = '';
  if (trades.length === 0) {
    listHtml = '<p class="detail" style="color:var(--muted)">No completed trades</p>';
  } else {
    if (todayTrades.length > 0) {
      listHtml += `<div class="info-section-label">Selected day (${selectedDate})</div>`;
      listHtml += [...todayTrades].reverse().map(renderTrade).join('');
    } else {
      listHtml += `<div class="info-section-label" style="color:var(--muted)">No trades on ${selectedDate}</div>`;
    }
    if (otherTrades.length > 0) {
      listHtml += `<hr style="border:none;border-top:2px solid #1f1a17;margin:10px 0">`;
      listHtml += `<div class="info-section-label">Other days (${otherTrades.length})</div>`;
      listHtml += `<div style="max-height:200px;overflow-y:auto">`;
      listHtml += [...otherTrades].reverse().map(renderTrade).join('');
      listHtml += `</div>`;
    }
  }
  document.getElementById('trade-list').innerHTML = listHtml;
}

function renderSummary() {
  const s = replay.summary;
  if (!s) return;
  const badge = s.incomplete
    ? '<span class="pill pill-warning" style="font-size:0.68rem;padding:4px 8px;min-width:auto">Incomplete</span>'
    : '';
  const dayR = s.total_pnl_r || 0;
  const daySign = dayR > 0 ? '+' : '';

  const allR = s.all_pnl_r || 0;
  const allSign = allR > 0 ? '+' : '';
  const account = s.account || null;
  const selectedTrade = getActiveReplayTrade();
  const accountDayPnl = Number(account?.day_pnl_amount ?? 0);
  const accountDayPnlClass = accountDayPnl > 0 ? 'up' : accountDayPnl < 0 ? 'down' : '';
  const accountSummaryLabel = buildAccountSummaryLabel(account);

  let html = '';
  html += _infoRow('Pair', s.pair);
  html += _infoRow('Date', `${s.date} ${badge}`);
  const selectedBars = s.selected_day_bars ?? s.total_bars;
  const replayBars = s.replay_bars ?? s.total_bars;
  const barLabel = replayBars > selectedBars
    ? `${selectedBars} selected · ${replayBars} shown`
    : `${selectedBars}`;
  html += _infoRow('Bars', `${barLabel}${s.incomplete ? ' (in progress)' : ''}`);
  if (s.continues_after_selected_day) {
    html += _infoRow('Replay', 'Extended until the selected-day trade closed');
  }
  html += `<div class="info-section-label">Trades touching selected day</div>`;
  html += _infoRow('Trades', `${s.total_trades} (${s.wins}W / ${s.losses}L)`);
  html += `<div class="info-row"><span class="info-label">Outcome</span><span class="${s.total_pnl_pips > 0 ? 'up' : s.total_pnl_pips < 0 ? 'down' : ''}">${s.total_pnl_pips > 0 ? '+' : ''}${s.total_pnl_pips}p <span style="color:var(--muted)">(${daySign}${dayR}R)</span></span></div>`;
  if (selectedTrade) {
    html += _infoRow('Selected trade risk', formatCurrency(selectedTrade.risk_amount));
    html += _infoRow('Selected trade P&L', formatSignedCurrency(selectedTrade.pnl_amount));
    html += _infoRow('Balance after selected close', formatCurrency(selectedTrade.balance_after));
  }
  if (account) {
    html += `<div class="info-section-label">Account</div>`;
    const realizedTrades = Number(account.realized_trades ?? 0);
    const realizedLabel = realizedTrades > 0
      ? `Realized day P&L (${realizedTrades} close${realizedTrades === 1 ? '' : 's'})`
      : 'Realized day P&L';
    html += `<div class="info-row"><span class="info-label">${realizedLabel}</span><span class="${accountDayPnlClass}">${formatSignedCurrency(account.day_pnl_amount)}</span></div>`;
    html += _infoRow('Balance at day end', formatCurrency(account.balance));
    if (accountSummaryLabel) {
      html += _infoRow('Sizing', accountSummaryLabel);
    }
    if (selectedTrade && account.balance !== null && selectedTrade.balance_after !== null && Number(account.balance) !== Number(selectedTrade.balance_after)) {
      html += _infoRow('Note', 'Day-end balance includes other pairs that closed later that day');
    }
    if (s.continues_after_selected_day) {
      html += _infoRow('Note', `Balance stays unchanged until that trade closes after ${s.date}`);
    }
  }
  html += `<div class="info-section-label">30-day</div>`;
  html += _infoRow('Trades', s.all_trades);
  html += `<div class="info-row"><span class="info-label">P&L</span><span class="${s.all_pnl_pips > 0 ? 'up' : s.all_pnl_pips < 0 ? 'down' : ''}">${s.all_pnl_pips > 0 ? '+' : ''}${s.all_pnl_pips}p <span style="color:var(--muted)">(${allSign}${allR}R)</span></span></div>`;
  document.getElementById('summary-details').innerHTML = html;
}

// ── Errors ──

function showError(msg) {
  errorBanner.textContent = msg;
  errorBanner.style.display = 'block';
}

function hideError() {
  errorBanner.style.display = 'none';
}
