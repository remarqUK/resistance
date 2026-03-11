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
const scrubber     = document.getElementById('scrubber');
const barLabel     = document.getElementById('bar-label');
const playPause    = document.getElementById('play-pause');
const stepBack     = document.getElementById('step-back');
const stepFwd      = document.getElementById('step-fwd');
const speedSelect  = document.getElementById('speed-select');
const tfSelect     = document.getElementById('tf-select');
const refreshBtn   = document.getElementById('refresh-btn');

// ── Init ──

PAIRS.forEach(p => {
  const opt = document.createElement('option');
  opt.value = p; opt.textContent = p;
  pairSelect.appendChild(opt);
});

// Pre-select pair from query string (e.g. /replay?pair=EURUSD)
const urlPair = new URLSearchParams(window.location.search).get('pair');
if (urlPair && PAIRS.includes(urlPair.toUpperCase())) {
  pairSelect.value = urlPair.toUpperCase();
}

pairSelect.addEventListener('change', fetchDateRange);
loadBtn.addEventListener('click', loadReplay);
playPause.addEventListener('click', togglePlay);
stepBack.addEventListener('click', () => stepTo(replay.currentIndex - 1));
stepFwd.addEventListener('click', () => stepTo(replay.currentIndex + 1));
scrubber.addEventListener('input', () => stepTo(parseInt(scrubber.value)));
speedSelect.addEventListener('change', () => {
  replay.speed = parseFloat(speedSelect.value);
  if (replay.isPlaying) { stopPlay(); startPlay(); }
});
tfSelect.addEventListener('change', loadReplay);
refreshBtn.addEventListener('click', refreshData);

fetchDateRange().then(() => {
  // Auto-load today's data on page open
  if (!dateInput.value) {
    const today = new Date().toISOString().slice(0, 10);
    dateInput.value = today;
  }
  loadReplay();
});

// ── Date range ──

async function fetchDateRange() {
  const pair = pairSelect.value;
  if (!pair) return;
  try {
    const res = await fetch(`/api/replay/dates?pair=${pair}`);
    if (!res.ok) return;
    const data = await res.json();
    dateInput.min = data.first_date;
    dateInput.max = data.last_date;
    if (!dateInput.value || dateInput.value < data.first_date || dateInput.value > data.last_date) {
      dateInput.value = data.last_date;
    }
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
    replay.currentIndex = -1;
    replay.markers = [];

    initChart();
    drawZoneLines();

    scrubber.max = replay.frames.length - 1;
    scrubber.value = 0;
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
    stepTo(replay.frames.length - 1);
  } catch (err) {
    showError('Network error: ' + err.message);
  } finally {
    loadBtn.disabled = false;
    loadBtn.textContent = 'Load';
  }
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
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: 'rgba(53,43,34,0.12)' },
    timeScale: {
      borderColor: 'rgba(53,43,34,0.12)',
      timeVisible: true,
      secondsVisible: false,
    },
  });

  // Invisible line series added first — zone price lines attach here (renders behind candles)
  replay.zoneSeries = replay.chart.addLineSeries({
    color: 'transparent',
    lineWidth: 0,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  });

  replay.candleSeries = replay.chart.addCandlestickSeries({
    upColor: '#1f7a49',
    downColor: '#b23b29',
    borderUpColor: '#1f7a49',
    borderDownColor: '#b23b29',
    wickUpColor: '#1f7a49',
    wickDownColor: '#b23b29',
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
  if (markers.length) {
    replay.candleSeries.setMarkers(markers);
  } else {
    replay.candleSeries.setMarkers([]);
  }

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

function stepTo(index) {
  if (index < 0) index = 0;
  if (index >= replay.frames.length) index = replay.frames.length - 1;
  if (index === replay.currentIndex) return;

  replay.currentIndex = index;
  scrubber.value = index;
  barLabel.textContent = `${index + 1} / ${replay.frames.length}`;

  renderToFrame(index);
  renderInfo(index);

  if (index >= replay.frames.length - 1) stopPlay();
}

function startPlay() {
  if (replay.isPlaying) return;
  replay.isPlaying = true;
  playPause.innerHTML = '&#9646;&#9646;';
  const interval = Math.max(50, 1000 / replay.speed);
  replay.playTimer = setInterval(() => stepTo(replay.currentIndex + 1), interval);
}

function stopPlay() {
  replay.isPlaying = false;
  playPause.innerHTML = '&#9654;';
  if (replay.playTimer) { clearInterval(replay.playTimer); replay.playTimer = null; }
}

function togglePlay() {
  if (replay.isPlaying) stopPlay();
  else {
    if (replay.currentIndex >= replay.frames.length - 1) stepTo(0);
    startPlay();
  }
}

// ── Info panels ──

function renderInfo(index) {
  const f = replay.frames[index];
  if (!f) return;

  const dec = replay.summary?.decimals || 5;

  // Bar info
  const dt = new Date(f.time);
  const timeStr = dt.toLocaleString();
  document.getElementById('bar-details').innerHTML = `
    <p class="detail"><strong>Time:</strong> ${timeStr}</p>
    <p class="detail"><strong>O:</strong> ${f.open.toFixed(dec)}&ensp;
       <strong>H:</strong> ${f.high.toFixed(dec)}&ensp;
       <strong>L:</strong> ${f.low.toFixed(dec)}&ensp;
       <strong>C:</strong> ${f.close.toFixed(dec)}</p>
    ${f.nearest_support ? `<p class="detail"><strong>Support:</strong> ${f.nearest_support.lower.toFixed(dec)} &ndash; ${f.nearest_support.upper.toFixed(dec)} (${f.nearest_support.touches}t)</p>` : ''}
    ${f.nearest_resistance ? `<p class="detail"><strong>Resistance:</strong> ${f.nearest_resistance.lower.toFixed(dec)} &ndash; ${f.nearest_resistance.upper.toFixed(dec)} (${f.nearest_resistance.touches}t)</p>` : ''}
  `;

  // Trade state
  let tradeHtml = '';
  if (f.signal) {
    tradeHtml += `<p class="detail"><span class="pill pill-${f.signal.direction.toLowerCase()}">${f.signal.direction}</span> Entry @ ${f.signal.entry_price.toFixed(dec)}</p>
      <p class="detail">SL: ${f.signal.sl_price.toFixed(dec)} &ensp; TP: ${f.signal.tp_price.toFixed(dec)}</p>`;
  }
  if (f.exit) {
    const cls = f.exit.pnl_pips > 0 ? 'up' : 'down';
    tradeHtml += `<p class="detail"><strong>Exit:</strong> ${f.exit.reason} @ ${f.exit.price.toFixed(dec)}</p>
      <p class="detail ${cls}"><strong>${f.exit.pnl_pips > 0 ? '+' : ''}${f.exit.pnl_pips}p</strong> (${f.exit.pnl_r > 0 ? '+' : ''}${f.exit.pnl_r}R)</p>`;
  }
  if (f.open_trade) {
    tradeHtml += `<p class="detail"><span class="pill pill-${f.open_trade.direction.toLowerCase()}">${f.open_trade.direction}</span> open since ${new Date(f.open_trade.entry_time).toLocaleString()}</p>
      <p class="detail">Entry: ${f.open_trade.entry_price.toFixed(dec)} &ensp; SL: ${f.open_trade.sl_price.toFixed(dec)} &ensp; TP: ${f.open_trade.tp_price.toFixed(dec)}</p>
      <p class="detail">Bars held: ${f.open_trade.bars_held}</p>`;
  }
  if (!f.signal && !f.exit && !f.open_trade) {
    tradeHtml = '<p class="detail" style="color:var(--muted)">Flat &mdash; no position</p>';
  }
  document.getElementById('trade-details').innerHTML = tradeHtml;

  // Completed trades list
  const trades = f.completed_trades || [];
  if (trades.length === 0) {
    document.getElementById('trade-list').innerHTML = '<p class="detail" style="color:var(--muted)">No completed trades yet</p>';
  } else {
    document.getElementById('trade-list').innerHTML = trades.map((t, i) => {
      const cls = (t.pnl_pips || 0) > 0 ? 'up' : 'down';
      return `<div class="trade-row">
        <span>#${i + 1} ${t.direction} @ ${t.entry_price.toFixed(dec)}</span>
        <span class="${cls}">${t.exit_reason} ${(t.pnl_pips || 0) > 0 ? '+' : ''}${t.pnl_pips}p (${(t.pnl_r || 0) > 0 ? '+' : ''}${t.pnl_r}R)</span>
      </div>`;
    }).join('');
  }
}

function renderSummary() {
  const s = replay.summary;
  if (!s) return;
  const badge = s.incomplete
    ? '<span class="pill pill-warning" style="font-size:0.68rem;padding:4px 8px;min-width:auto">Incomplete</span>'
    : '';
  document.getElementById('summary-details').innerHTML = `
    <p class="detail"><strong>Pair:</strong> ${s.pair}</p>
    <p class="detail"><strong>Date:</strong> ${s.date} ${badge}</p>
    <p class="detail"><strong>Bars:</strong> ${s.total_bars}${s.incomplete ? ' (day in progress)' : ''}</p>
    <p class="detail"><strong>Trades:</strong> ${s.total_trades} (${s.wins}W / ${s.losses}L)</p>
    <p class="detail"><strong>Total P&amp;L:</strong> <span class="${s.total_pnl_pips > 0 ? 'up' : s.total_pnl_pips < 0 ? 'down' : ''}">${s.total_pnl_pips > 0 ? '+' : ''}${s.total_pnl_pips} pips</span></p>
  `;
}

// ── Errors ──

function showError(msg) {
  errorBanner.textContent = msg;
  errorBanner.style.display = 'block';
}

function hideError() {
  errorBanner.style.display = 'none';
}
