/* live_trade.js -- chart + trade overlay logic for Live Trade Review page */
(function () {
  if (window.__fxLiveTradeScriptLoaded) return;

  var DECIMALS = 5;
  var params = new URLSearchParams(window.location.search);
  var pair = params.get('pair') || '';
  var signalId = params.get('signal_id') || '';
  var direction = (params.get('direction') || '').toUpperCase();
  var entryPrice = parseFloat(params.get('entry_price')) || null;
  var entryTime = params.get('entry_time') || null;
  var exitPrice = parseFloat(params.get('exit_price')) || null;
  var exitTime = params.get('exit_time') || null;
  var sl = parseFloat(params.get('sl')) || null;
  var tp = parseFloat(params.get('tp')) || null;

  function _row(label, value) {
    return '<div class="info-row"><span class="info-label">' + label + '</span><span>' + value + '</span></div>';
  }

  function formatTs(iso) {
    if (!iso) return '\u2014';
    var d = new Date(iso);
    if (isNaN(d)) return '\u2014';
    return d.toLocaleString([], {year:'numeric',month:'short',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false});
  }

  function normalizeDecimal(value) {
    var parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function applyTradePayload(trade) {
    if (!trade) return;
    if (trade.pair) pair = String(trade.pair).toUpperCase();
    if (trade.direction) direction = String(trade.direction).toUpperCase();
    entryPrice = normalizeDecimal(trade.opened_price);
    if (entryPrice == null) {
      entryPrice = normalizeDecimal(trade.entry_price);
    }
    if (entryPrice == null) {
      entryPrice = normalizeDecimal(trade.submitted_entry_price);
    }
    entryTime = trade.opened_at || trade.signal_time || null;
    exitPrice = normalizeDecimal(trade.closed_price);
    exitTime = trade.closed_at || null;
    sl = normalizeDecimal(trade.submitted_sl_price);
    if (sl == null) {
      sl = normalizeDecimal(trade.sl_price);
    }
    tp = normalizeDecimal(trade.submitted_tp_price);
    if (tp == null) {
      tp = normalizeDecimal(trade.tp_price);
    }
  }

  function setPageTitle() {
    document.title = (pair && direction)
      ? pair + ' ' + direction + ' - Live Trade'
      : (pair ? pair + ' - Live Trade Review' : 'Live Trade Review');
  }

  function formatPrice(v) {
    if (v == null) return '\u2013';
    return Number(v).toFixed(DECIMALS);
  }

  /* ---- Info panels ---- */
  function renderInfo() {
    var isLong = direction === 'LONG';
    var dirClass = isLong ? 'pill-long' : 'pill-short';

    var tradeEl = document.getElementById('trade-details');
    var execEl = document.getElementById('execution-details');
    var outcomeEl = document.getElementById('outcome-details');

    if (tradeEl) {
      var tradeHtml = '';
      tradeHtml += '<div class="info-row"><span class="pill ' + dirClass + '" style="font-size:0.72rem;padding:3px 8px;min-width:auto">' + direction + '</span><span>' + pair + '</span></div>';
      tradeHtml += _row('Entry', formatPrice(entryPrice) + ' @ ' + formatTs(entryTime));
      if (sl != null) tradeHtml += _row('Stop Loss', formatPrice(sl));
      if (tp != null) tradeHtml += _row('Take Profit', formatPrice(tp));
      tradeEl.innerHTML = tradeHtml;
    }

    if (execEl) {
      var execHtml = '';
      if (entryTime) execHtml += _row('Opened', formatTs(entryTime));
      if (exitTime) execHtml += _row('Closed', formatTs(exitTime));
      if (entryTime && exitTime) {
        var ms = new Date(exitTime) - new Date(entryTime);
        var hours = Math.round(ms / 3600000);
        execHtml += _row('Duration', hours + 'h');
      }
      execEl.innerHTML = execHtml || _row('Status', 'Open');
    }

    if (outcomeEl) {
      var outcomeHtml = '';
      if (exitPrice != null && entryPrice != null) {
        outcomeHtml += _row('Exit', formatPrice(exitPrice));
        var pnlRaw = isLong ? exitPrice - entryPrice : entryPrice - exitPrice;
        var riskDist = sl != null ? Math.abs(entryPrice - sl) : 0;
        var pnlR = riskDist > 0 ? (pnlRaw / riskDist) : null;
        var pnlClass = pnlRaw >= 0 ? 'up' : 'down';
        if (pnlR != null) {
          outcomeHtml += '<div class="info-row"><span class="info-label">P&L</span><span class="' + pnlClass + '">' + (pnlR >= 0 ? '+' : '') + pnlR.toFixed(2) + 'R</span></div>';
        }
      } else {
        outcomeHtml += _row('Status', 'Position open');
      }
      outcomeEl.innerHTML = outcomeHtml;
    }
  }

  /* ---- Chart ---- */
  var chart = null;
  var candleSeries = null;

  function initChart() {
    var container = document.getElementById('chart-container');
    if (!container) return;

    var chartState = window.fxChartCore.createStandardChart(container, {
      decimals: DECIMALS,
    });
    chart = chartState.chart;
    candleSeries = chartState.candleSeries;

    new ResizeObserver(function () {
      chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
    }).observe(container);
  }

  function loadChartData() {
    if (!pair) return Promise.resolve();
    return fetch('/api/chart-data?pair=' + encodeURIComponent(pair) + '&tf=1m')
      .then(function (resp) { return resp.json(); })
      .then(function (data) {
        if (data.error) {
          var banner = document.getElementById('error-banner');
          if (banner) {
            banner.textContent = data.error;
            banner.style.display = 'block';
          }
          return;
        }
        if (data.bars && data.bars.length) {
          candleSeries.setData(data.bars);
        }

        // S/R zone bands
        window.fxChartCore.addZoneBands(chart, data.support, data.resistance, data.bars || []);
      })
      .catch(function () {
        var banner = document.getElementById('error-banner');
        if (banner) {
          banner.textContent = 'Failed to load chart data';
          banner.style.display = 'block';
        }
      });
  }

  function addTradeOverlay() {
    if (!entryPrice || !candleSeries) return;
    var isLong = direction === 'LONG';

    // Markers
    var markers = [];
    if (entryTime) {
      markers.push({
        time: Math.floor(new Date(entryTime).getTime() / 1000),
        position: isLong ? 'belowBar' : 'aboveBar',
        color: isLong ? '#1f7a49' : '#b23b29',
        shape: isLong ? 'arrowUp' : 'arrowDown',
        text: direction + ' @ ' + entryPrice.toFixed(DECIMALS),
      });
    }
    if (exitTime && exitPrice) {
      markers.push({
        time: Math.floor(new Date(exitTime).getTime() / 1000),
        position: isLong ? 'aboveBar' : 'belowBar',
        color: '#d4a017',
        shape: 'circle',
        text: 'Exit @ ' + exitPrice.toFixed(DECIMALS),
      });
    }
    if (markers.length) {
      markers.sort(function (a, b) { return a.time - b.time; });
      candleSeries.setMarkers(markers);
    }

    // Price lines
    candleSeries.createPriceLine({
      price: entryPrice,
      color: '#d4a017',
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Solid,
      axisLabelVisible: true,
      title: 'Entry',
    });
    if (sl) {
      candleSeries.createPriceLine({
        price: sl,
        color: '#b23b29',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'SL',
      });
    }
    if (tp) {
      candleSeries.createPriceLine({
        price: tp,
        color: '#1f7a49',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'TP',
      });
    }
    if (exitPrice) {
      candleSeries.createPriceLine({
        price: exitPrice,
        color: '#d4a017',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'Exit',
      });
    }

    // Scroll to trade
    if (entryTime) {
      var entryTs = Math.floor(new Date(entryTime).getTime() / 1000);
      var exitTs = exitTime ? Math.floor(new Date(exitTime).getTime() / 1000) : entryTs;
      var midpoint = (entryTs + exitTs) / 2;
      var halfSpan = Math.max((exitTs - entryTs) / 2, 12 * 3600);
      chart.timeScale().setVisibleRange({
        from: midpoint - halfSpan * 2,
        to: midpoint + halfSpan * 2,
      });
    }
  }

function loadTradeFromSignalId() {
  if (!signalId) {
    return Promise.resolve();
  }
  return fetch('/api/live-trade?signal_id=' + encodeURIComponent(signalId))
    .then(function (res) {
      if (!res.ok) {
        var banner = document.getElementById('error-banner');
        if (banner) {
          banner.textContent = 'Unable to load trade details from signal_id';
          banner.style.display = 'block';
        }
        return;
      }
      return res.json();
      })
      .then(function (payload) {
        if (!payload || !payload.trade) return;
        applyTradePayload(payload.trade);
        setPageTitle();
      })
      .catch(function () {
        return;
      });
  }

  /* ---- Other trades this day sidebar ---- */
  function fetchOtherTrades() {
    var sidebar = document.getElementById('other-trades-sidebar');
    var el = document.getElementById('other-trades-list');
    if (!el || !entryTime) return;

    var tradeDate = (entryTime || '').slice(0, 10);
    if (!tradeDate) return;
    if (sidebar) sidebar.style.display = '';
    el.innerHTML = '<p style="color:var(--muted);font-size:0.84rem">Loading...</p>';

    fetch('/api/live-diary')
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var trades = (data.trades || []).filter(function (t) {
          var d = (t.signal_time || t.opened_at || '').slice(0, 10);
          return d === tradeDate;
        });
        if (!trades.length) {
          el.innerHTML = '<p style="color:var(--muted);font-size:0.84rem">No other trades this day</p>';
          return;
        }
        trades.sort(function (a, b) {
          return String(a.signal_time || '').localeCompare(String(b.signal_time || ''));
        });

        var dayR = trades.reduce(function (s, t) { return s + (Number(t.pnl_r) || 0); }, 0);
        var dayRCls = dayR > 0 ? 'up' : dayR < 0 ? 'down' : '';
        var html = '<div style="font-size:0.78rem;font-weight:700;padding:6px 0 8px;border-bottom:2px solid var(--line)">';
        html += trades.length + ' trades \u00b7 <span class="' + dayRCls + '">' + (dayR > 0 ? '+' : '') + dayR.toFixed(2) + 'R</span></div>';

        for (var i = 0; i < trades.length; i++) {
          var t = trades[i];
          var rowSignalId = t.signal_id || '';
          var rowSignalTime = t.signal_time || t.opened_at || '';
          var isCurrent = signalId
            ? String(rowSignalId) === signalId
            : (t.pair === pair && rowSignalTime === entryTime);
          var cls = (Number(t.pnl_r) || 0) >= 0 ? 'up' : 'down';
          var pnlR = t.pnl_r != null ? (t.pnl_r > 0 ? '+' : '') + Number(t.pnl_r).toFixed(2) + 'R' : '';
          var hm = function (iso) {
            if (!iso) return '';
            var d = new Date(iso);
            return isNaN(d) ? '' : d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',hour12:false});
          };
          var timeRange = hm(t.opened_at || t.signal_time);
          if (t.closed_at) timeRange += '\u2013' + hm(t.closed_at);
          var dirLabel = (t.direction || '').charAt(0);
          var dirCls = (t.direction || '').toLowerCase();

          var encodedPair = encodeURIComponent(t.pair || '');
          var clickQuery = 'pair=' + encodedPair;
          if (rowSignalId) {
            clickQuery += '&signal_id=' + encodeURIComponent(rowSignalId);
          }
          var clickAttr = isCurrent ? '' : ' onclick="window.location.href=\'/live-trade?' + clickQuery + '\'" style="cursor:pointer"';

          html += '<div style="display:flex;align-items:center;gap:6px;padding:3px 0;font-size:0.8rem;border-bottom:1px solid var(--line)"' + clickAttr + '>';
          html += '<span class="pill pill-' + dirCls + '" style="font-size:0.62rem;padding:1px 5px;min-width:auto">' + dirLabel + '</span>';
          html += '<span style="font-weight:' + (isCurrent ? '700' : '400') + ';min-width:52px">' + (t.pair || '') + '</span>';
          html += '<span style="flex:1;color:var(--muted);font-size:0.74rem">' + timeRange + '</span>';
          html += '<span class="' + cls + '" style="font-weight:600;min-width:48px;text-align:right">' + pnlR + '</span>';
          html += '</div>';
        }
        el.innerHTML = html;
      })
      .catch(function () {
        el.innerHTML = '';
        if (sidebar) sidebar.style.display = 'none';
      });
  }

  /* ---- Init ---- */
  var boot = Promise.resolve();
  if (signalId) {
    boot = boot.then(loadTradeFromSignalId);
  }
  boot.then(function () {
    setPageTitle();
    renderInfo();
    initChart();
    return loadChartData();
  }).then(function () {
    addTradeOverlay();
    fetchOtherTrades();
  });

})();
