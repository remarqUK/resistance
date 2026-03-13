/* Strategy Replay — progressive chart playback with TradingView Lightweight Charts */

const PAIRS = [
  'AUDCAD','AUDJPY','AUDNZD','AUDUSD',
  'CADJPY','CHFJPY',
  'EURAUD','EURCAD','EURCHF','EURGBP','EURJPY','EURUSD',
  'GBPAUD','GBPCAD','GBPCHF','GBPJPY','GBPUSD',
  'NZDJPY','NZDUSD',
  'USDCAD','USDCHF','USDJPY',
];

const replay = {
  frames: [],
  contextBars: [],
  zones: [],
  summary: null,
  currentIndex: -1,
  isPlaying: false,
  speed: 1,
  playTimer: null,
  chart: null,
  candleSeries: null,
  markers: [],
  zoneLines: [],
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
if (urlTimeframe && Array.from(tfSelect.options).some((option) => option.value === urlTimeframe)) {
  tfSelect.value = urlTimeframe;
}

const urlDate = isIsoDate(urlParams.get('date')) ? urlParams.get('date') : '';
let requestedReplayDate = urlDate;
if (urlDate) {
  dateInput.value = urlDate;
}

pairSelect.addEventListener('change', () => { fetchDateRange().then(() => loadReplay()); });
loadBtn.addEventListener('click', loadReplay);
tfSelect.addEventListener('change', loadReplay);
refreshBtn.addEventListener('click', refreshData);

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

  if (!pair || !dateVal) {
    showError('Select a pair and date');
    return;
  }

  loadBtn.disabled = true;
  loadBtn.textContent = 'Loading...';
  hideError();
  stopPlay();

  try {
    const res = await fetch(`/api/replay?pair=${pair}&date=${dateVal}&preset=${preset}&tf=${tf}`);
    const data = await res.json();

    if (!res.ok) {
      showError(data.error || 'Failed to load replay data');
      return;
    }

    replay.frames = data.frames;
    replay.contextBars = data.context_bars || [];
    replay.zones = data.zones;
    replay.summary = data.summary;
    replay.allTrades = data.all_completed_trades || [];
    replay.currentIndex = -1;
    replay.markers = [];
    updateReplayUrl(pair, dateVal, preset, tf);

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
      stepTo(replay.frames.length - 1);
    } else {
      // No target-day bars — still render context bars and trades
      const candles = replay.contextBars.map(b => ({
        time: parseTime(b.time), open: b.open, high: b.high, low: b.low, close: b.close,
      }));
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

function updateReplayUrl(pair, date, preset, tf) {
  const params = new URLSearchParams(window.location.search);
  params.set('pair', pair);
  params.set('date', date);
  params.set('preset', preset);
  params.set('tf', tf);
  window.history.replaceState({}, '', `/replay?${params.toString()}`);
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
  // Convert ISO string to UNIX timestamp (seconds)
  return Math.floor(new Date(timeStr).getTime() / 1000);
}

function renderToFrame(targetIndex) {
  if (targetIndex < 0 || targetIndex >= replay.frames.length) return;

  // Start with context bars (prior days, always fully visible)
  const candles = replay.contextBars.map(b => ({
    time: parseTime(b.time), open: b.open, high: b.high, low: b.low, close: b.close,
  }));
  const markers = [];

  // Add entry/exit markers onto context bars for trades involving the selected day
  const selectedDate = replay.summary?.date || '';
  const contextTimes = new Set(candles.map(c => c.time));
  const relevantTrades = (replay.allTrades || []).filter(t =>
    (t.entry_time || '').slice(0, 10) === selectedDate ||
    (t.exit_time || '').slice(0, 10) === selectedDate
  );
  for (const t of relevantTrades) {
    const entryTs = parseTime(t.entry_time);
    if (contextTimes.has(entryTs)) {
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
      if (contextTimes.has(exitTs)) {
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
  const dt = new Date(entryTime);
  const tradeDate = dt.toISOString().slice(0, 10);
  const currentDate = dateInput.value;

  if (tradeDate !== currentDate) {
    // Different day — update date picker and reload
    dateInput.value = tradeDate;
    loadReplay().then(() => {
      _scrubToTime(dt);
    });
  } else {
    _scrubToTime(dt);
  }
}

function _scrubToTime(dt) {
  const target = Math.floor(dt.getTime() / 1000);
  let best = 0;
  for (let i = 0; i < replay.frames.length; i++) {
    const ft = Math.floor(new Date(replay.frames[i].time).getTime() / 1000);
    if (ft <= target) best = i;
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
  const dt = new Date(f.time);
  const timeStr = dt.toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', hour12:false});
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
    const openSince = new Date(f.open_trade.entry_time).toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', hour12:false});
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
  const isSelectedDay = t => (t.entry_time || '').slice(0, 10) === selectedDate || (t.exit_time || '').slice(0, 10) === selectedDate;
  const todayTrades = trades.filter(isSelectedDay);
  const otherTrades = trades.filter(t => !isSelectedDay(t));

  const renderTrade = (t) => {
    const cls = (t.pnl_pips || 0) > 0 ? 'up' : 'down';
    const entryDt = new Date(t.entry_time).toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', hour12:false});
    const exitDt = t.exit_time ? new Date(t.exit_time).toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', hour12:false}) : '';
    return `<div class="trade-row" style="flex-wrap:wrap;gap:2px;cursor:pointer" onclick="jumpToTradeEntry('${t.entry_time}')">
      <span><span class="pill pill-${t.direction.toLowerCase()}" style="font-size:0.68rem;padding:2px 6px;min-width:auto">${t.direction}</span> @ ${t.entry_price.toFixed(dec)}${t.exit_price ? ' \u2192 ' + t.exit_price.toFixed(dec) : ''}</span>
      <span class="${cls}">${t.exit_reason} ${(t.pnl_pips || 0) > 0 ? '+' : ''}${t.pnl_pips}p (${(t.pnl_r || 0) > 0 ? '+' : ''}${t.pnl_r}R)</span>
      <span style="width:100%;font-size:0.76rem;color:var(--muted)">\u{1F4C4} Simulated \u00b7 ${entryDt}${exitDt ? ' \u2192 ' + exitDt : ''}</span>
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
  const risk = 100;
  const dayR = s.total_pnl_r || 0;
  const dayMoney = (dayR * risk).toFixed(0);
  const daySign = dayR > 0 ? '+' : '';
  const dayClass = dayR > 0 ? 'up' : dayR < 0 ? 'down' : '';

  const allR = s.all_pnl_r || 0;
  const allMoney = (allR * risk).toFixed(0);
  const allSign = allR > 0 ? '+' : '';
  const allClass = allR > 0 ? 'up' : allR < 0 ? 'down' : '';

  let html = '';
  html += _infoRow('Pair', s.pair);
  html += _infoRow('Date', `${s.date} ${badge}`);
  html += _infoRow('Bars', `${s.total_bars}${s.incomplete ? ' (in progress)' : ''}`);
  html += `<div class="info-section-label">Selected day</div>`;
  html += _infoRow('Trades', `${s.total_trades} (${s.wins}W / ${s.losses}L)`);
  html += `<div class="info-row"><span class="info-label">P&L</span><span class="${s.total_pnl_pips > 0 ? 'up' : s.total_pnl_pips < 0 ? 'down' : ''}">${s.total_pnl_pips > 0 ? '+' : ''}${s.total_pnl_pips}p <span style="color:var(--muted)">(${daySign}${dayR}R)</span></span></div>`;
  html += `<div class="info-row"><span class="info-label">At \u00a3${risk} risk</span><span class="${dayClass}">${daySign}\u00a3${dayMoney}</span></div>`;
  html += `<div class="info-section-label">30-day</div>`;
  html += _infoRow('Trades', s.all_trades);
  html += `<div class="info-row"><span class="info-label">P&L</span><span class="${s.all_pnl_pips > 0 ? 'up' : s.all_pnl_pips < 0 ? 'down' : ''}">${s.all_pnl_pips > 0 ? '+' : ''}${s.all_pnl_pips}p <span style="color:var(--muted)">(${allSign}${allR}R)</span></span></div>`;
  html += `<div class="info-row"><span class="info-label">At \u00a3${risk} risk</span><span class="${allClass}">${allSign}\u00a3${allMoney}</span></div>`;
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
