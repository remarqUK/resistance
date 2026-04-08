import React, { useCallback, useEffect, useMemo, useRef, useState, memo } from 'react';
import { NavLinks } from '../components/NavLinks';
import '../styles/trade-log.css';
import type { TradeLogResponse } from '../types';

declare const LightweightCharts: any;

function formatSignalTime(value?: string) {
  if (!value) {
    return '';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value).replace('T', ' ').slice(0, 19);
  }
  return date.toLocaleString([], {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    timeZone: 'UTC',
  });
}

function formatNumber(value: any, digits = 5) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '';
  }
  return Number(value).toFixed(digits);
}

/* ---------- Trade chart component ---------- */
interface TradeChartProps {
  pair: string;
  direction?: string;
  entryPrice?: number;
  slPrice?: number;
  tpPrice?: number;
  exitPrice?: number;
  entryTime?: string;
  exitTime?: string;
  decimals?: number;
}

const TradeLogChart = memo(function TradeLogChart({ pair, direction, entryPrice, slPrice, tpPrice, exitPrice, entryTime, exitTime, decimals = 5 }: TradeChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<any>(null);
  const [status, setStatus] = useState('Loading...');

  useEffect(() => {
    const chartApi = (window as any).LightweightCharts;
    const container = containerRef.current;
    if (!container || !chartApi || !pair) {
      setStatus('Chart unavailable');
      return;
    }

    let active = true;
    // Destroy previous chart
    if (chartRef.current) {
      try { chartRef.current.remove(); } catch {}
      chartRef.current = null;
    }

    async function load() {
      try {
        const res = await fetch(`/api/chart-data?pair=${encodeURIComponent(pair)}`);
        const data = await res.json();
        if (!active || data.error) { setStatus(data.error || 'Error'); return; }
        const dec = data.decimals ?? decimals;

        const chart = chartApi.createChart(container, {
          layout: { background: { type: 'solid', color: '#fffaf2' }, textColor: '#5b4b3a' },
          grid: { vertLines: { color: 'rgba(91,75,58,0.08)' }, horzLines: { color: 'rgba(91,75,58,0.08)' } },
          crosshair: { mode: chartApi.CrosshairMode.Normal },
          rightPriceScale: { borderColor: 'rgba(91,75,58,0.18)' },
          timeScale: { borderColor: 'rgba(91,75,58,0.18)', timeVisible: true, secondsVisible: false },
          width: container.clientWidth || 520,
          height: container.clientHeight || 500,
        });
        if (!active) { chart.remove(); return; }
        chartRef.current = chart;

        const seriesOpts: any = {
          upColor: '#1f7a49', downColor: '#b23b29',
          borderUpColor: '#1f7a49', borderDownColor: '#b23b29',
          wickUpColor: '#1f7a49', wickDownColor: '#b23b29',
          priceFormat: { type: 'price', precision: dec, minMove: 1 / Math.pow(10, dec) },
        };
        if (slPrice != null && tpPrice != null && !isNaN(slPrice) && !isNaN(tpPrice)) {
          const lo = Math.min(slPrice, tpPrice);
          const hi = Math.max(slPrice, tpPrice);
          const pad = (hi - lo) * 0.10;
          seriesOpts.autoscaleInfoProvider = () => ({
            priceRange: { minValue: lo - pad, maxValue: hi + pad },
          });
        }
        const series = chart.addCandlestickSeries(seriesOpts);

        if (data.bars?.length) {
          series.setData(data.bars);
        }

        // Price lines
        const line = (price: any, color: string, title: string, style?: number) => {
          if (price == null || isNaN(Number(price))) return;
          series.createPriceLine({ price: Number(price), color, lineWidth: 1, lineStyle: style ?? chartApi.LineStyle.Dotted, axisLabelVisible: true, title });
        };
        line(entryPrice, '#d4a017', 'Entry', chartApi.LineStyle.Solid);
        line(slPrice, '#b23b29', 'SL');
        line(tpPrice, '#1f7a49', 'TP');
        line(exitPrice, '#d4a017', 'Exit', chartApi.LineStyle.Dashed);

        // Markers
        const isLong = (direction || '').toUpperCase() === 'LONG';
        const markers: any[] = [];
        if (entryTime && entryPrice) {
          markers.push({
            time: Math.floor(new Date(entryTime).getTime() / 1000),
            position: isLong ? 'belowBar' : 'aboveBar',
            color: isLong ? '#1f7a49' : '#b23b29',
            shape: isLong ? 'arrowUp' : 'arrowDown',
            text: `${direction} @ ${Number(entryPrice).toFixed(dec)}`,
          });
        }
        if (exitTime && exitPrice) {
          markers.push({
            time: Math.floor(new Date(exitTime).getTime() / 1000),
            position: isLong ? 'aboveBar' : 'belowBar',
            color: '#d4a017',
            shape: 'circle',
            text: `Exit @ ${Number(exitPrice).toFixed(dec)}`,
          });
        }
        if (markers.length) {
          markers.sort((a, b) => a.time - b.time);
          series.setMarkers(markers);
        }

        // Zone bands (excluded from auto-scale when SL/TP set)
        const bandScaleOpt = (slPrice != null && tpPrice != null) ? { autoscaleInfoProvider: () => null } : {};
        if (data.support) {
          const band = chart.addBaselineSeries({ baseValue: { type: 'price', price: data.support.lower }, topFillColor1: 'rgba(31,122,73,0.10)', topFillColor2: 'rgba(31,122,73,0.10)', topLineColor: 'transparent', bottomFillColor1: 'transparent', bottomFillColor2: 'transparent', bottomLineColor: 'transparent', lineWidth: 0, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false, ...bandScaleOpt });
          const p = 365 * 24 * 3600; const now = Math.floor(Date.now() / 1000);
          band.setData([{ time: now - p, value: data.support.upper }, { time: now + p, value: data.support.upper }]);
        }
        if (data.resistance) {
          const band = chart.addBaselineSeries({ baseValue: { type: 'price', price: data.resistance.lower }, topFillColor1: 'rgba(178,59,41,0.10)', topFillColor2: 'rgba(178,59,41,0.10)', topLineColor: 'transparent', bottomFillColor1: 'transparent', bottomFillColor2: 'transparent', bottomLineColor: 'transparent', lineWidth: 0, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false, ...bandScaleOpt });
          const p = 365 * 24 * 3600; const now = Math.floor(Date.now() / 1000);
          band.setData([{ time: now - p, value: data.resistance.upper }, { time: now + p, value: data.resistance.upper }]);
        }

        // Scroll to trade entry or realtime
        if (entryTime) {
          const entryTs = Math.floor(new Date(entryTime).getTime() / 1000);
          const exitTs = exitTime ? Math.floor(new Date(exitTime).getTime() / 1000) : entryTs;
          const midpoint = (entryTs + exitTs) / 2;
          const halfSpan = Math.max((exitTs - entryTs) / 2, 12 * 3600);
          chart.timeScale().setVisibleRange({ from: midpoint - halfSpan * 2, to: midpoint + halfSpan * 2 });
        } else {
          chart.timeScale().scrollToRealTime();
        }

        setStatus('');
        new ResizeObserver(() => {
          chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
        }).observe(container);
      } catch {
        if (active) setStatus('Failed to load chart');
      }
    }

    void load();
    return () => {
      active = false;
      if (chartRef.current) {
        try { chartRef.current.remove(); } catch {}
        chartRef.current = null;
      }
    };
  }, [pair, direction, entryPrice, slPrice, tpPrice, exitPrice, entryTime, exitTime, decimals]);

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%', minHeight: 300 }}>
      {status ? <div style={{ padding: '12px', color: 'var(--muted)', fontSize: '0.84rem' }}>{status}</div> : null}
      <div ref={containerRef} style={{ width: '100%', height: '100%', minHeight: 300 }} />
    </div>
  );
});

/* ---------- Main page ---------- */
export function TradeLogPage() {
  const [pair, setPair] = useState('');
  const [status, setStatus] = useState('');
  const [data, setData] = useState<TradeLogResponse>({ signals: [], pairs: [], count: 0 });
  const [error, setError] = useState('');
  const [selectedSignal, setSelectedSignal] = useState<any | null>(null);
  const initialSelectionDone = useRef(false);

  const load = useCallback(async () => {
    try {
      let url = '/api/trade-log?limit=200';
      if (pair) {
        url += `&pair=${encodeURIComponent(pair)}`;
      }
      if (status) {
        url += `&status=${encodeURIComponent(status)}`;
      }
      const res = await fetch(url);
      const payload = await res.json();
      if (!res.ok) {
        throw new Error(payload.error || 'Unable to load trade log.');
      }
      setData(payload);
      setError('');
    } catch (err: any) {
      setError(err?.message || 'Unable to load trade log.');
    }
  }, [pair, status]);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 30000);
    return () => window.clearInterval(timer);
  }, [load]);

  // Mark initial load complete (no auto-select — user clicks to open chart)
  useEffect(() => {
    if (!initialSelectionDone.current && data.signals?.length) {
      initialSelectionDone.current = true;
    }
  }, [data.signals]);

  const rows = useMemo(() => data.signals || [], [data.signals]);

  const rowKey = (row: any) => row.signal_id || `${row.pair}:${row.signal_time}:${row.direction}`;
  const selectedRowKey = selectedSignal ? rowKey(selectedSignal) : '';

  return (
    <div className="shell trade-log-page">
      <div className="hero">
        <div className="hero-title-row">
          <div>
            <span className="eyebrow">FX support / resistance</span>
            <h1>Trade Log</h1>
            <p className="subtitle">Live signal and execution history. Filter by pair or broker status without leaving the dashboard stack.</p>
          </div>
          <NavLinks current="/trade-log" />
        </div>
      </div>

      <section className="panel">
        <div className="filter-toolbar">
          <div className="field">
            <label htmlFor="trade-log-pair">Ticker</label>
            <select id="trade-log-pair" value={pair} onChange={(event) => setPair(event.target.value)}>
              <option value="">All pairs</option>
              {(data.pairs || []).map((value) => (
                <option key={value} value={value}>{value}</option>
              ))}
            </select>
          </div>

          <div className="field">
            <label htmlFor="trade-log-status">Status</label>
            <select id="trade-log-status" value={status} onChange={(event) => setStatus(event.target.value)}>
              <option value="">All statuses</option>
              <option value="SUBMITTED">Submitted</option>
              <option value="OPEN">Open</option>
              <option value="FILLED">Filled</option>
              <option value="CLOSED">Closed</option>
              <option value="CANCELLED">Cancelled</option>
              <option value="FAILED">Failed</option>
              <option value="SKIPPED">Skipped</option>
              <option value="DETECTED">Detected</option>
            </select>
          </div>

          <button className="toolbar-btn" type="button" onClick={() => void load()}>Refresh</button>
        </div>

        {error ? <div className="empty">Error: {error}</div> : null}

        {!error && !rows.length ? (
          <div className="empty">No signals recorded yet</div>
        ) : null}

        {!error && rows.length ? (
          <>
            <div className="table-wrap" style={{ maxHeight: '80vh', overflow: 'auto' }}>
              <table className="data-table trade-log-table">
                <thead>
                  <tr>
                    <th>Signal Time</th>
                    <th>Pair</th>
                    <th>Dir</th>
                    <th>Status</th>
                    <th>Broker</th>
                    <th>Entry</th>
                    <th>SL</th>
                    <th>TP</th>
                    <th>Bid/Ask</th>
                    <th>Spread</th>
                    <th>Quote</th>
                    <th>P&amp;L</th>
                    <th>P/L R</th>
                    <th>P/L GBP</th>
                    <th>Close Price</th>
                    <th>Close Reason</th>
                    <th>Note</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row: any) => {
                    const spread = row.submit_spread != null ? (Number(row.submit_spread) * 10000).toFixed(1) : '';
                    const bid = row.submit_bid != null ? formatNumber(row.submit_bid, 5) : '';
                    const ask = row.submit_ask != null ? formatNumber(row.submit_ask, 5) : '';
                    const bidAsk = bid && ask ? `${bid}/${ask}` : '';
                    const pnl = row.pnl_pips != null ? `${Number(row.pnl_pips) > 0 ? '+' : ''}${Number(row.pnl_pips).toFixed(1)}` : '';
                    const pnlR = row.pnl_r != null ? `${Number(row.pnl_r) > 0 ? '+' : ''}${Number(row.pnl_r).toFixed(2)}R` : '';
                    const pnlGbp = row.pnl_gbp != null ? `\u00a3${Number(row.pnl_gbp) > 0 ? '+' : ''}${Number(row.pnl_gbp).toFixed(2)}` : '';
                    const pnlRClass = row.pnl_r != null ? (Number(row.pnl_r) >= 0 ? 'up' : 'down') : '';
                    const pnlGbpClass = row.pnl_gbp != null ? (Number(row.pnl_gbp) >= 0 ? 'up' : 'down') : '';
                    const isClosed = String(row.status || '').toUpperCase() === 'CLOSED';
                    const closePrice = row.closed_price != null ? formatNumber(row.closed_price, 5) : '';
                    const closeReason = row.close_reason || '';
                    const rk = rowKey(row);
                    const rowIsSelected = rk === selectedRowKey;
                    return (
                      <tr
                        key={rk}
                        className={`trade-log-row${rowIsSelected ? ' trade-log-row-selected' : ''}`}
                        style={{ cursor: 'pointer' }}
                        onClick={() => setSelectedSignal(rowIsSelected ? null : row)}
                        title="Click to review trade"
                      >
                        <td>
                          {formatSignalTime(row.signal_time)}
                          {isClosed && row.closed_at ? (
                            <div style={{ fontSize: '0.72rem', color: '#8b949e', marginTop: '2px' }}>
                              closed {formatSignalTime(row.closed_at)}
                            </div>
                          ) : null}
                        </td>
                        <td>{row.pair}</td>
                        <td className={`dir-${row.direction || ''}`}>{row.direction}</td>
                        <td className={`status-${row.status || ''}`}>{row.status}</td>
                        <td>{row.broker_order_status || ''}</td>
                        <td>{row.submitted_entry_price != null ? formatNumber(row.submitted_entry_price, 5) : formatNumber(row.entry_price, 5)}</td>
                        <td>{formatNumber(row.sl_price, 5)}</td>
                        <td>{formatNumber(row.tp_price, 5)}</td>
                        <td>{bidAsk}</td>
                        <td>{spread}</td>
                        <td>{row.quote_source || ''}</td>
                        <td>{pnl}</td>
                        <td className={pnlRClass}>{pnlR}</td>
                        <td className={pnlGbpClass}>{pnlGbp}</td>
                        <td>{isClosed ? closePrice : ''}</td>
                        <td>{isClosed ? closeReason : ''}</td>
                        <td className="note" title={row.note || ''}>{row.note || ''}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {selectedSignal ? (
              <div className="trade-chart-overlay-backdrop" onClick={() => setSelectedSignal(null)}>
                <div className="trade-chart-overlay" onClick={(e) => e.stopPropagation()}>
                  <div className="trade-log-chart-header">
                    <h2>{selectedSignal.pair} {selectedSignal.direction}</h2>
                    <button className="toolbar-btn" type="button" onClick={() => setSelectedSignal(null)}>&times; Close</button>
                  </div>
                  <div className="trade-log-chart-body">
                    <TradeLogChart
                      pair={selectedSignal.pair}
                      direction={selectedSignal.direction}
                      entryPrice={selectedSignal.submitted_entry_price ?? selectedSignal.entry_price}
                      slPrice={selectedSignal.sl_price}
                      tpPrice={selectedSignal.tp_price}
                      exitPrice={selectedSignal.closed_price}
                      entryTime={selectedSignal.opened_at || selectedSignal.signal_time}
                      exitTime={selectedSignal.closed_at}
                    />
                  </div>
                  <div className="trade-log-chart-info">
                    <span>Entry: {formatNumber(selectedSignal.submitted_entry_price ?? selectedSignal.entry_price, 5)}</span>
                    <span>SL: {formatNumber(selectedSignal.sl_price, 5)}</span>
                    <span>TP: {formatNumber(selectedSignal.tp_price, 5)}</span>
                    {selectedSignal.closed_price != null ? <span>Exit: {formatNumber(selectedSignal.closed_price, 5)}</span> : null}
                    {selectedSignal.pnl_r != null ? <span className={Number(selectedSignal.pnl_r) >= 0 ? 'up' : 'down'}>{Number(selectedSignal.pnl_r) > 0 ? '+' : ''}{Number(selectedSignal.pnl_r).toFixed(2)}R</span> : null}
                  </div>
                </div>
              </div>
            ) : null}
          </>
        ) : null}
      </section>
    </div>
  );
}
