import React, { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { NavLinks } from '../components/NavLinks';
import type {
  AlertRow,
  DashboardState,
  ExecutionRow,
  LogEntry,
  PairRow,
  PositionRow,
  Signal,
  SummaryState,
} from '../types';

const INITIAL_STATE: DashboardState = {
  summary: {},
  pairs: {},
  signals: [],
  positions: [],
  alerts: [],
  executions: [],
  log: [],
};

const FILL_ENDPOINTS = ['/api/fill', '/fill', '/fill-cache', '/fill_cache'];
const BACKTEST_ENDPOINTS = ['/api/backtest-rerun', '/backtest-rerun'];

function formatNumber(value: any, digits = 5) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '–';
  }
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatSigned(value: any, digits = 1, suffix = '') {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '–';
  }
  const number = Number(value);
  const prefix = number > 0 ? '+' : '';
  return `${prefix}${formatNumber(number, digits)}${suffix}`;
}

function formatTimestamp(isoString?: string | null) {
  if (!isoString) {
    return '–';
  }
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) {
    return '–';
  }
  return date.toLocaleString([], {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZone: 'UTC',
  });
}

function formatDateOnly(isoString?: string | null) {
  if (!isoString) {
    return '–';
  }
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) {
    return '–';
  }
  return date.toLocaleDateString([], {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    timeZone: 'UTC',
  });
}

const PAUSE_ICON = '\u23F8';
const RESUME_ICON = '\u25B6';
const BACKTEST_ICON = '\u21A9';
const BACKTEST_SPINNER_ICON = '\u27F3';
const REBUILD_ICON = '\u21BB';
const RESTART_ICON = '\u21BA';
const STOP_ICON = '\u23F9';
const AUDIT_LOG_ICON = '\uD83D\uDCCB';  // clipboard/log icon
const HEALTH_ICON = '\uD83E\uDE7A';    // stethoscope icon

function badgeClass(value: any) {
  const token = String(value || 'muted').toLowerCase().replaceAll(/[^a-z0-9]+/g, '-');
  return `pill pill-${token}`;
}

function executionBadgeLabel(status?: string, note?: string | null) {
  const normalizedStatus = String(status || '').toUpperCase();
  const normalizedNote = String(note || '').toLowerCase();
  const isFundsIssue = normalizedStatus === 'SKIPPED'
    && (
      normalizedNote.includes('whatif: margin exceeded')
      || normalizedNote.includes('would risk liquidation')
      || normalizedNote.includes('insufficient funds')
      || normalizedNote.includes('insufficient margin')
      || normalizedNote.includes('funds')
    );

  if (isFundsIssue) {
    return { label: 'FUNDS', className: 'pill pill-funds' };
  }
  return { label: normalizedStatus || 'â€“', className: badgeClass(status) };
}

function levelTone(level?: string) {
  const token = String(level || 'info').toLowerCase();
  if (token === 'success') return 'tone-success';
  if (token === 'warning') return 'tone-warning';
  if (token === 'error') return 'tone-error';
  if (token === 'muted') return 'tone-muted';
  return 'tone-info';
}

function formatExecutionPrice(value: any, execution?: ExecutionRow) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '–';
  }
  const digits = execution?.pair?.includes('JPY') ? 3 : 5;
  return formatNumber(value, digits);
}

function executionPnlROrPips(execution: ExecutionRow) {
  if (execution.pnl_r !== null && execution.pnl_r !== undefined && Number.isFinite(Number(execution.pnl_r))) {
    return {
      value: formatSigned(execution.pnl_r, 2, 'R'),
      isUp: Number(execution.pnl_r) >= 0,
    };
  }

  if (execution.pnl_pips !== null && execution.pnl_pips !== undefined && Number.isFinite(Number(execution.pnl_pips))) {
    return {
      value: formatSigned(execution.pnl_pips, 1, ' pips'),
      isUp: Number(execution.pnl_pips) >= 0,
    };
  }

  return {
    value: '-',
    isUp: true,
  };
}
function executionKey(execution: ExecutionRow) {
  return [
    execution.pair || '',
    execution.direction || '',
    execution.order_id || '',
    execution.time || '',
  ].join('|');
}

function executionTimestampMs(execution: ExecutionRow) {
  if (!execution?.time) {
    return null;
  }
  const withOffset = String(execution.time).trim();
  const normalizedTime = withOffset.includes('T')
    ? String(execution.time)
    : withOffset.replace(' ', 'T');
  const noOffsetGapTime = normalizedTime.replace(/\s(?=[+-]\d{2}:\d{2}$)/, '');
  const normalizedMsPrecision = noOffsetGapTime.replace(/(\.\d{3})\d+(?=(?:[+-]\d{2}:\d{2}|$))/, '$1');
  const time = Date.parse(normalizedMsPrecision);
  return Number.isNaN(time) ? null : time;
}

function isClosedExecution(execution: ExecutionRow) {
  const status = String(execution.status || '').toUpperCase();
  return execution.closed_at != null || (status !== '' && status !== 'OPEN');
}

type ExecutionFilterMode = 'all' | 'open' | 'closed';

function executionFilterButtonLabel(mode: ExecutionFilterMode) {
  if (mode === 'open') return 'Open';
  if (mode === 'closed') return 'Closed';
  return 'All';
}

function replayDateForExecution(execution: ExecutionRow) {
  if (!execution?.time) {
    return '';
  }
  const date = new Date(execution.time);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  return date.toISOString().slice(0, 10);
}

function sortPairs(pairs: Record<string, PairRow>, positions: PositionRow[]) {
  const positionPairs = new Set(positions.map((position) => position.pair));

  function nearestZoneDist(row: PairRow) {
    const support = row.support_dist_pct ?? Infinity;
    const resistance = row.resistance_dist_pct ?? Infinity;
    return Math.min(support, resistance);
  }

  return Object.values(pairs).sort((left, right) => {
    const leftSignal = left.signal ? 1 : 0;
    const rightSignal = right.signal ? 1 : 0;
    if (leftSignal !== rightSignal) {
      return rightSignal - leftSignal;
    }

    const leftPosition = positionPairs.has(left.pair) ? 1 : 0;
    const rightPosition = positionPairs.has(right.pair) ? 1 : 0;
    if (leftPosition !== rightPosition) {
      return rightPosition - leftPosition;
    }

    const leftDist = nearestZoneDist(left);
    const rightDist = nearestZoneDist(right);
    if (leftDist !== rightDist) {
      return leftDist - rightDist;
    }

    return left.pair.localeCompare(right.pair);
  });
}

function mergeStateWithMessage(previous: DashboardState, message: any): DashboardState {
  if (message.type === 'bootstrap' || message.type === 'snapshot') {
    return normalizeState(message.state || {}, previous.signals);
  }

  if (message.type === 'pair_update' && message.row?.pair) {
    const nextPairs = {
      ...previous.pairs,
      [message.row.pair]: message.row,
    };
    return {
      ...previous,
      pairs: nextPairs,
      signals: mergeSignalList(nextPairs, previous.signals),
      summary: message.summary || previous.summary,
    };
  }

  if (
    message.type === 'scan_status'
    || message.type === 'backfill_progress'
    || message.type === 'fill_progress'
    || message.type === 'backtest_progress'
  ) {
    return {
      ...previous,
      summary: message.summary || previous.summary,
    };
  }

  if (message.type === 'log_entry') {
    return {
      ...previous,
      log: [...previous.log, message.entry || {}].slice(-80),
    };
  }

  if (message.type === 'error') {
    const entry: LogEntry = {
      ts: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'UTC' }),
      level: 'error',
      message: message.message || 'Unknown live dashboard error',
    };
    return {
      ...previous,
      summary: message.summary || previous.summary,
      log: [...previous.log, entry].slice(-80),
    };
  }

  return previous;
}

async function postFirstAvailable(endpointCandidates: string[], requestInit: RequestInit) {
  let lastError = 'Request failed.';
  for (const endpoint of endpointCandidates) {
    try {
      const res = await fetch(endpoint, requestInit);
      const payload = await res.json().catch(() => ({}));
      if (res.ok) {
        return payload;
      }
      if (res.status !== 404) {
        throw new Error(payload?.message || payload?.error || `HTTP ${res.status}`);
      }
      lastError = payload?.message || payload?.error || `Endpoint not found: ${endpoint}`;
    } catch (error: any) {
      lastError = error?.message || lastError;
    }
  }
  throw new Error(lastError);
}

function currentBacktestButtonText(backtest: Record<string, any>) {
  const status = String(backtest?.status || 'idle');
  const total = Number(backtest?.items_requested || 0);
  const processed = Number(backtest?.items_processed || 0);
  if (status === 'starting' || status === 'running') {
    const pct = total > 0 ? Math.round((processed / total) * 100) : 0;
    return total > 0 ? `Re-running ${processed}/${total} (${pct}%)` : 'Starting...';
  }
  return 'Re-run Backtest';
}

function scanProgressText(summary: SummaryState) {
  const fill = summary.fill || {};
  const backfill = summary.backfill || {};
  const backtest = summary.backtest || {};

  if (fill.status && fill.status !== 'idle') {
    const total = Number(fill.items_requested || 0);
    const processed = Number(fill.items_processed || 0);
    const attempts = Number(fill.attempts || 0);
    const errors = Number(fill.errors || 0);
    const pct = total > 0 ? Math.round((processed / total) * 100) : 0;
    const current = fill.current_item ? ` • ${fill.current_item}` : '';
    const attemptsText = attempts > 0 ? ` • attempts ${attempts}` : '';
    const errorsText = errors > 0 ? ` • errors ${errors}` : '';
    const label = fill.status === 'running' ? 'Fill' : `Fill ${fill.status}`;
    return `${label}: ${processed} of ${total} (${pct}%)${attemptsText}${errorsText}${current}`;
  }

  if (backtest.status && backtest.status !== 'idle' && backtest.status !== 'complete') {
    const total = Number(backtest.items_requested || 0);
    const processed = Number(backtest.items_processed || 0);
    const pct = total > 0 ? Math.round((processed / total) * 100) : 0;
    const current = backtest.current_item ? ` • ${backtest.current_item}` : '';
    return `Backtest: ${processed} of ${total} (${pct}%)${current}`;
  }

  if (summary.status === 'backfilling' && backfill.phase && backfill.phase !== 'done') {
    const phase = backfill.phase === 'zones' ? 'Loading zones' : backfill.phase === 'hourly' ? 'Loading hourly' : 'Scanning';
    const pct = backfill.total > 0 ? Math.round((backfill.completed / backfill.total) * 100) : 0;
    const current = backfill.current_pair ? ` • ${backfill.current_pair}` : '';
    return `${phase}: ${backfill.completed}/${backfill.total} (${pct}%)${current}`;
  }

  return `${summary.pairs_completed || 0} / ${summary.pairs_total || 0} pairs`;
}

function nextTransactionText(now: number) {
  const nextHour = new Date(now);
  nextHour.setHours(nextHour.getHours() + 1, 0, 0, 0);
  const remaining = nextHour.getTime() - now;
  const totalSeconds = Math.max(0, Math.floor(remaining / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return {
    timer: `${minutes}:${String(seconds).padStart(2, '0')}`,
    at: nextHour.toLocaleString([], {
      year: 'numeric',
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      timeZone: 'UTC',
    }),
  };
}

const PRICE_DISPLAY_DECIMALS = 5;
const NEAR_THRESHOLD = 0.3;

function mergeSignalList(pairs: Record<string, PairRow>, previousSignals: Signal[], incomingSignals?: Signal[]) {
  const seed = new Map<string, Signal>();
  for (const signal of previousSignals) {
    seed.set(`${signal.pair}:${signal.direction}`, signal);
  }
  for (const signal of incomingSignals || []) {
    seed.set(`${signal.pair}:${signal.direction}`, signal);
  }

  return Object.values(pairs)
    .filter((pair) => pair.signal)
    .map((pair) => {
      const signal = pair.signal as Signal;
      const preserved = seed.get(`${signal.pair}:${signal.direction}`);
      return {
        ...preserved,
        ...signal,
        size_plan: signal.size_plan ?? preserved?.size_plan ?? null,
        arrived_at: signal.arrived_at ?? preserved?.arrived_at ?? null,
        last_valid_at: signal.last_valid_at ?? preserved?.last_valid_at ?? null,
      };
    })
    .sort((left, right) => left.pair.localeCompare(right.pair));
}

function normalizeState(payload: Partial<DashboardState> | undefined, previousSignals: Signal[] = []): DashboardState {
  const pairs = payload?.pairs || {};
  const signals = mergeSignalList(pairs, previousSignals, payload?.signals || []);
  return {
    summary: payload?.summary || {},
    pairs,
    signals,
    positions: payload?.positions || [],
    alerts: payload?.alerts || [],
    executions: payload?.executions || [],
    log: payload?.log || [],
  };
}

const WatchlistRow = memo(function WatchlistRow({ row }: { row: PairRow }) {
  const signal = row.signal;
  const setupText = signal ? `${signal.zone_type} · ${signal.zone_strength}` : row.note || 'No setup';
  const supportNear = row.support_dist_pct != null && row.support_dist_pct <= NEAR_THRESHOLD;
  const resistanceNear = row.resistance_dist_pct != null && row.resistance_dist_pct <= NEAR_THRESHOLD;

  return (
    <tr>
      <td>
        <a
          href={`/live-trade?pair=${encodeURIComponent(row.pair)}`}
          target="_blank"
          rel="noreferrer"
          className="pair-main pair-link"
          title={`Live chart ${row.pair}`}
        >
          {row.pair}
        </a>
      </td>
      <td><span className={badgeClass(row.state)}>{row.state}</span></td>
      <td className="price">{formatNumber(row.price, row.decimals ?? PRICE_DISPLAY_DECIMALS)}</td>
      <td className={`price${supportNear ? ' zone-near' : ''}`}>{row.support_text || '–'}</td>
      <td className={`price${resistanceNear ? ' zone-near' : ''}`}>{row.resistance_text || '–'}</td>
      <td>{setupText}</td>
      <td>{signal ? <span className={badgeClass(signal.direction)}>{signal.direction}</span> : <span className="pair-sub">–</span>}</td>
    </tr>
  );
});

function ExecutionMiniChart({ execution }: { execution: ExecutionRow }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [status, setStatus] = useState('Loading replay...');

  useEffect(() => {
    const chartApi = window.LightweightCharts;
    const container = containerRef.current;
    const replayDate = replayDateForExecution(execution);
    if (!container || !chartApi) {
      setStatus('Replay chart unavailable.');
      return;
    }
    if (!replayDate) {
      setStatus('No valid replay date for this transaction.');
      return;
    }

    let active = true;
    let chart: any = null;

    async function load() {
      setStatus(`Loading ${execution.pair} ${replayDate}...`);
      try {
        const params = new URLSearchParams({
          pair: execution.pair,
          tf: '1h',
          start: `${replayDate}T00:00:00Z`,
          end: `${replayDate}T23:59:59Z`,
        });
        const res = await fetch(`/api/replay/bars?${params.toString()}`);
        const data = await res.json();
        if (!res.ok) {
          throw new Error(data.error || 'Failed to load replay bars.');
        }

        const bars = (data.bars || [])
          .map((bar: any) => ({
            time: Math.floor(new Date(bar.time).getTime() / 1000),
            open: Number(bar.open),
            high: Number(bar.high),
            low: Number(bar.low),
            close: Number(bar.close),
          }))
          .filter((bar: any) => Number.isFinite(bar.time));

        if (!active) {
          return;
        }
        if (!bars.length) {
          setStatus('No replay bars found for that date.');
          return;
        }

        chart = chartApi.createChart(container, {
          layout: {
            background: { type: 'solid', color: '#fffaf2' },
            textColor: '#5b4b3a',
          },
          grid: {
            vertLines: { color: 'rgba(91, 75, 58, 0.08)' },
            horzLines: { color: 'rgba(91, 75, 58, 0.08)' },
          },
          crosshair: { mode: chartApi.CrosshairMode.Normal },
          rightPriceScale: { borderColor: 'rgba(91, 75, 58, 0.18)' },
          timeScale: {
            borderColor: 'rgba(91, 75, 58, 0.18)',
            timeVisible: true,
            secondsVisible: false,
          },
          width: container.clientWidth || 520,
          height: 220,
        });

        const series = chart.addCandlestickSeries({
          upColor: '#1f7a49',
          downColor: '#b23b29',
          borderUpColor: '#1f7a49',
          borderDownColor: '#b23b29',
          wickUpColor: '#1f7a49',
          wickDownColor: '#b23b29',
        });
        series.setData(bars);
        chart.timeScale().fitContent();

        const createLine = (price: any, color: string, title: string) => {
          if (price === null || price === undefined || Number.isNaN(Number(price))) {
            return;
          }
          series.createPriceLine({
            price: Number(price),
            color,
            lineWidth: 2,
            lineStyle: chartApi.LineStyle.Dashed,
            axisLabelVisible: true,
            title,
          });
        };

        createLine(execution.submitted_entry_price, '#456b8c', 'Entry');
        createLine(execution.submitted_sl_price, '#b23b29', 'SL');
        createLine(execution.submitted_tp_price, '#1f7a49', 'TP');
        setStatus(`${execution.pair} replay for ${replayDate}`);
      } catch (error: any) {
        if (active) {
          setStatus(error?.message || 'Failed to load replay bars.');
        }
      }
    }

    void load();

    return () => {
      active = false;
      if (chart) {
        chart.remove();
      }
    };
  }, [execution]);

  return (
    <div className="mini-detail-wide">
      <div className="chart-status">{status}</div>
      <div ref={containerRef} className="execution-mini-chart" />
    </div>
  );
}

const PositionMiniChart = memo(function PositionMiniChart({ pair, entryPrice, slPrice, tpPrice }: { pair: string; entryPrice?: number; slPrice?: number; tpPrice?: number }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<any>(null);
  const seriesRef = useRef<any>(null);
  const lastBarRef = useRef<{ time: number; open: number; high: number; low: number; close: number } | null>(null);
  const [status, setStatus] = useState('Loading...');

  // Load chart data once
  useEffect(() => {
    const chartApi = window.LightweightCharts;
    const container = containerRef.current;
    if (!container || !chartApi || !pair) {
      setStatus('Chart unavailable');
      return;
    }

    let active = true;

    async function load() {
      try {
        const res = await fetch(`/api/chart-data?pair=${encodeURIComponent(pair)}`);
        const data = await res.json();
        if (!active || data.error) return;

        const chart = chartApi.createChart(container, {
          layout: { background: { type: 'solid', color: '#fffaf2' }, textColor: '#5b4b3a' },
          grid: { vertLines: { color: 'rgba(91,75,58,0.08)' }, horzLines: { color: 'rgba(91,75,58,0.08)' } },
          crosshair: { mode: chartApi.CrosshairMode.Normal },
          rightPriceScale: { borderColor: 'rgba(91,75,58,0.18)' },
          timeScale: { borderColor: 'rgba(91,75,58,0.18)', timeVisible: true, secondsVisible: false },
          width: container.clientWidth || 520,
          height: 260,
        });
        chartRef.current = chart;

        const series = chart.addCandlestickSeries({
          upColor: '#1f7a49', downColor: '#b23b29',
          borderUpColor: '#1f7a49', borderDownColor: '#b23b29',
          wickUpColor: '#1f7a49', wickDownColor: '#b23b29',
        });
        seriesRef.current = series;

        if (data.bars?.length) {
          series.setData(data.bars);
          const last = data.bars[data.bars.length - 1];
          lastBarRef.current = { time: last.time, open: last.open, high: last.high, low: last.low, close: last.close };
        }

        const line = (price: any, color: string, title: string, style?: number) => {
          if (price == null || isNaN(Number(price))) return;
          series.createPriceLine({ price: Number(price), color, lineWidth: 1, lineStyle: style ?? chartApi.LineStyle.Dashed, axisLabelVisible: true, title });
        };
        line(entryPrice, '#d4a017', 'Entry', chartApi.LineStyle.Solid);
        line(slPrice, '#b23b29', 'SL');
        line(tpPrice, '#1f7a49', 'TP');

        // Zone bands
        if (data.support) {
          const band = chart.addBaselineSeries({ baseValue: { type: 'price', price: data.support.lower }, topFillColor1: 'rgba(31,122,73,0.10)', topFillColor2: 'rgba(31,122,73,0.10)', topLineColor: 'transparent', bottomFillColor1: 'transparent', bottomFillColor2: 'transparent', bottomLineColor: 'transparent', lineWidth: 0, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
          const pad = 365*24*3600; const now = Math.floor(Date.now()/1000);
          band.setData([{ time: now-pad, value: data.support.upper }, { time: now+pad, value: data.support.upper }]);
        }
        if (data.resistance) {
          const band = chart.addBaselineSeries({ baseValue: { type: 'price', price: data.resistance.lower }, topFillColor1: 'rgba(178,59,41,0.10)', topFillColor2: 'rgba(178,59,41,0.10)', topLineColor: 'transparent', bottomFillColor1: 'transparent', bottomFillColor2: 'transparent', bottomLineColor: 'transparent', lineWidth: 0, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
          const pad = 365*24*3600; const now = Math.floor(Date.now()/1000);
          band.setData([{ time: now-pad, value: data.resistance.upper }, { time: now+pad, value: data.resistance.upper }]);
        }

        chart.timeScale().scrollToRealTime();
        setStatus('');

        new ResizeObserver(() => {
          chart.applyOptions({ width: container.clientWidth });
        }).observe(container);
      } catch {
        if (active) setStatus('Failed to load chart');
      }
    }

    void load();
    return () => { active = false; if (chartRef.current) { chartRef.current.remove(); chartRef.current = null; } };
  }, [pair, entryPrice, slPrice, tpPrice]);

  // Subscribe to WebSocket for live price updates
  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${protocol}://${window.location.host}/ws`);

    ws.addEventListener('message', (event) => {
      try {
        const msg = JSON.parse(event.data);
        let price: number | null = null;

        if (msg.type === 'pair_update' && msg.row?.pair === pair) {
          price = msg.row.price;
        }
        if ((msg.type === 'bootstrap' || msg.type === 'snapshot') && msg.state?.pairs?.[pair]) {
          price = msg.state.pairs[pair].price;
        }

        if (price != null && price > 0 && seriesRef.current) {
          const now = Math.floor(Date.now() / 1000);
          const hourStart = now - (now % 3600);
          const last = lastBarRef.current;

          if (last && last.time === hourStart) {
            last.high = Math.max(last.high, price);
            last.low = Math.min(last.low, price);
            last.close = price;
            seriesRef.current.update(last);
          } else {
            const bar = { time: hourStart, open: price, high: price, low: price, close: price };
            lastBarRef.current = bar;
            seriesRef.current.update(bar);
          }
        }
      } catch {}
    });

    return () => ws.close();
  }, [pair]);

  return (
    <div className="mini-detail-wide">
      {status ? <div className="chart-status">{status}</div> : null}
      <div ref={containerRef} style={{ width: '100%', marginTop: '8px' }} />
    </div>
  );
});

function AccountChart() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [empty, setEmpty] = useState(false);

  useEffect(() => {
    const chartApi = window.LightweightCharts;
    const container = containerRef.current;
    if (!container || !chartApi) return;

    let active = true;
    let chart: any = null;
    let ro: ResizeObserver | null = null;

    async function load() {
      try {
        const res = await fetch('/api/account-history');
        const data = await res.json();
        const snapshots = data.snapshots || [];
        if (!active) return;
        if (!snapshots.length) { setEmpty(true); return; }

        chart = chartApi.createChart(container, {
          width: container.clientWidth,
          height: 220,
          layout: {
            background: { type: 'solid', color: 'transparent' },
            textColor: '#999',
            fontSize: 11,
          },
          grid: {
            vertLines: { color: 'rgba(0,0,0,0.06)' },
            horzLines: { color: 'rgba(0,0,0,0.06)' },
          },
          rightPriceScale: {
            visible: true,
            borderVisible: true,
            borderColor: 'rgba(0,0,0,0.15)',
          },
          leftPriceScale: {
            visible: true,
            borderVisible: true,
            borderColor: 'rgba(0,0,0,0.15)',
          },
          timeScale: {
            borderVisible: true,
            borderColor: 'rgba(0,0,0,0.15)',
            fixLeftEdge: true,
            fixRightEdge: true,
          },
          crosshair: {
            horzLine: { visible: false, labelVisible: false },
          },
        });

        // Balance line on left scale — smoothed filled area
        const balanceSeries = chart.addAreaSeries({
          lineColor: '#5b9cf6',
          topColor: 'rgba(91,156,246,0.28)',
          bottomColor: 'rgba(91,156,246,0.02)',
          lineWidth: 2,
          lineType: 2,
          priceScaleId: 'left',
          lastValueVisible: true,
          priceLineVisible: false,
          crosshairMarkerVisible: true,
          title: 'Equity',
        });
        balanceSeries.setData(
          snapshots.map((s: any) => ({ time: s.date, value: s.equity ?? s.balance }))
        );

        // Daily P&L histogram on right scale
        const pnlSeries = chart.addHistogramSeries({
          priceScaleId: 'right',
          lastValueVisible: false,
          priceLineVisible: false,
          title: 'Daily P&L',
        });
        pnlSeries.setData(
          snapshots
            .filter((s: any) => s.daily_pnl_gbp != null)
            .map((s: any) => ({
              time: s.date,
              value: s.daily_pnl_gbp,
              color: s.daily_pnl_gbp >= 0 ? 'rgba(38,166,91,0.7)' : 'rgba(214,69,65,0.7)',
            }))
        );

        chart.timeScale().fitContent();

        ro = new ResizeObserver(() => {
          if (chart) chart.applyOptions({ width: container.clientWidth });
        });
        ro.observe(container);
      } catch {
        if (active) setEmpty(true);
      }
    }

    void load();
    return () => {
      active = false;
      if (ro) { ro.disconnect(); ro = null; }
      if (chart) { chart.remove(); chart = null; }
    };
  }, []);

  return (
    <section className="panel" style={{ marginBottom: '1rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '4px' }}>
        <span className="meta-label" style={{ fontSize: '0.78rem' }}>Account equity &amp; daily P&amp;L (GBP)</span>
      </div>
      {empty
        ? <div className="empty" style={{ fontSize: '0.82rem', padding: '12px 0' }}>No account history yet.</div>
        : <div ref={containerRef} style={{ width: '100%' }} />
      }
    </section>
  );
}


export function DashboardPage() {
  const [viewState, setViewState] = useState<DashboardState>(INITIAL_STATE);
  const [connectionState, setConnectionState] = useState<'connecting' | 'live' | 'disconnected' | 'error'>('connecting');
  const [executionTogglePending, setExecutionTogglePending] = useState(false);
  const [executionFilterMode, setExecutionFilterMode] = useState<ExecutionFilterMode>('all');
  const [selectedExecutionKey, setSelectedExecutionKey] = useState<string | null>(null);
  const [selectedPositionKey, setSelectedPositionKey] = useState<string | null>(null);
  const [closingPositionKey, setClosingPositionKey] = useState<string | null>(null);
  const [nowTick, setNowTick] = useState(() => Date.now());
  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const queueRef = useRef<any[]>([]);
  const frameRef = useRef<number | null>(null);

  const pushLog = useCallback((entry: LogEntry) => {
    const nextEntry: LogEntry = {
      ts: entry.ts || new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'UTC' }),
      level: entry.level || 'info',
      message: entry.message || '',
    };
    setViewState((previous) => ({
      ...previous,
      log: [...previous.log, nextEntry].slice(-80),
    }));
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => setNowTick(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!selectedExecutionKey) {
      return;
    }
    const cutoff = nowTick - (72 * 60 * 60 * 1000);
    const isSelectedVisible = viewState.executions.some((execution) => {
      const key = executionKey(execution);
      if (key !== selectedExecutionKey) {
        return false;
      }
      const executionTime = executionTimestampMs(execution);
      if (executionTime === null || executionTime < cutoff) {
        return false;
      }
      if (executionFilterMode === 'open') {
        return !isClosedExecution(execution);
      }
      if (executionFilterMode === 'closed') {
        return isClosedExecution(execution);
      }
      return true;
    });

    if (!isSelectedVisible) {
      setSelectedExecutionKey(null);
    }
  }, [selectedExecutionKey, nowTick, executionFilterMode, viewState.executions]);

  useEffect(() => {
    function flushQueue() {
      frameRef.current = null;
      const queued = queueRef.current.splice(0, queueRef.current.length);
      if (!queued.length) {
        return;
      }
      setViewState((previous) => queued.reduce((nextState, message) => mergeStateWithMessage(nextState, message), previous));
    }

    function scheduleFlush() {
      if (frameRef.current !== null) {
        return;
      }
      frameRef.current = window.requestAnimationFrame(flushQueue);
    }

    function connect() {
      if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
        return;
      }
      const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
      const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);
      socketRef.current = socket;
      setConnectionState('connecting');

      socket.addEventListener('open', () => {
        setConnectionState('live');
      });

      socket.addEventListener('message', (event) => {
        try {
          const message = JSON.parse(event.data);
          queueRef.current.push(message);
          scheduleFlush();
        } catch (_error) {
          setConnectionState('error');
          pushLog({ level: 'error', message: 'Received malformed dashboard message' });
        }
      });

      socket.addEventListener('close', () => {
        setConnectionState('disconnected');
        socketRef.current = null;
        if (reconnectTimerRef.current === null) {
          reconnectTimerRef.current = window.setTimeout(() => {
            reconnectTimerRef.current = null;
            connect();
          }, 1500);
        }
      });

      socket.addEventListener('error', () => {
        setConnectionState('disconnected');
        socket.close();
      });
    }

    connect();

    return () => {
      if (frameRef.current !== null) {
        window.cancelAnimationFrame(frameRef.current);
      }
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
      }
      if (socketRef.current) {
        socketRef.current.close();
      }
    };
  }, [pushLog]);

  const summary = viewState.summary || {};
  const sortedRows = useMemo(() => sortPairs(viewState.pairs, viewState.positions), [viewState.pairs, viewState.positions]);
  const signals = useMemo(() => viewState.signals || [], [viewState.signals]);
  const nextTransaction = useMemo(() => nextTransactionText(nowTick), [nowTick]);
  const filteredExecutions = useMemo(() => {
    const cutoff = nowTick - (72 * 60 * 60 * 1000);
    const filtered = viewState.executions.filter((execution) => {
      const executionTime = executionTimestampMs(execution);
      if (executionTime === null || executionTime < cutoff) {
        return false;
      }
      if (executionFilterMode === 'open') {
        return !isClosedExecution(execution);
      }
      if (executionFilterMode === 'closed') {
        return isClosedExecution(execution);
      }
      return true;
    });

    return filtered
      .slice()
      .sort((left, right) => (executionTimestampMs(right) ?? 0) - (executionTimestampMs(left) ?? 0));
  }, [executionFilterMode, nowTick, viewState.executions]);

  const scanStatus = summary.status || 'Starting';
  const isScanLive = scanStatus === 'live';

  const executionModeLabel = summary.execution_mode_label
    || (summary.execution_mode === 'intrabar' ? 'Intrabar (minute bars)' : 'Next-bar (completed hourly)');
  const executionModeText = summary.execution_available
    ? (summary.execution_paused
      ? `${executionModeLabel} · Execution paused`
      : `${executionModeLabel} · ${summary.execution_enabled ? 'Live execution enabled' : 'Scan only'}`)
    : `${executionModeLabel} · Scan only`;

  const toggleExecution = useCallback(async () => {
    if (!summary.execution_available || executionTogglePending) {
      return;
    }
    setExecutionTogglePending(true);
    try {
      const res = await fetch('/api/execution-mode', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ paused: !Boolean(summary.execution_paused) }),
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(payload?.error || 'Unable to update execution mode.');
      }
      if (payload.state) {
        setViewState(normalizeState(payload.state, viewState.signals));
      }
    } catch (error: any) {
      pushLog({ level: 'error', message: error?.message || 'Unable to update execution mode.' });
    } finally {
      setExecutionTogglePending(false);
    }
  }, [executionTogglePending, pushLog, summary.execution_available, summary.execution_paused, viewState.signals]);

  const startFill = useCallback(async () => {
    if (!window.confirm('Run cache fill now?')) {
      return;
    }
    try {
      const payload = await postFirstAvailable(FILL_ENDPOINTS, { method: 'POST' });
      pushLog({ level: 'success', message: payload?.message || 'Cache fill started.' });
    } catch (error: any) {
      const message = error?.message || 'Unable to start cache fill.';
      pushLog({ level: 'error', message });
      window.alert(`Unable to start cache fill: ${message}`);
    }
  }, [pushLog]);

  const closeTrackedPosition = useCallback(async (position: PositionRow) => {
    const positionKey = `${position.pair}:${position.direction}`;
    if (closingPositionKey === positionKey) {
      return;
    }
    if (!window.confirm(`Close ${position.direction} ${position.pair} now?`)) {
      return;
    }

    setClosingPositionKey(positionKey);
    try {
      const res = await fetch('/api/position-close', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ pair: position.pair, direction: position.direction }),
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(payload?.error || 'Unable to close position.');
      }
      pushLog({ level: 'success', message: payload?.message || `Close request sent for ${position.pair}.` });
    } catch (error: any) {
      const message = error?.message || 'Unable to close position.';
      pushLog({ level: 'error', message });
      window.alert(`Unable to close position: ${message}`);
    } finally {
      setClosingPositionKey(null);
    }
  }, [closingPositionKey, pushLog]);

  const rerunBacktest = useCallback(async () => {
    if (!window.confirm('Re-run full backtest now?')) {
      return;
    }
    try {
      const payload = await postFirstAvailable(BACKTEST_ENDPOINTS, { method: 'POST' });
      pushLog({ level: 'success', message: payload?.message || 'Backtest rerun started.' });
    } catch (error: any) {
      const message = error?.message || 'Unable to start backtest rerun.';
      pushLog({ level: 'error', message });
      window.alert(`Unable to start backtest rerun: ${message}`);
    }
  }, [pushLog]);

  const [rebuildState, setRebuildState] = useState<'idle' | 'building' | 'done' | 'error'>('idle');
  const rebuildUI = useCallback(async () => {
    setRebuildState('building');
    pushLog({ level: 'info', message: 'Rebuilding React UI...' });
    try {
      const res = await fetch('/api/rebuild-ui', { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        setRebuildState('done');
        pushLog({ level: 'success', message: data.message || 'Build complete. Reloading...' });
        setTimeout(() => { window.location.href = window.location.pathname + '?t=' + Date.now(); }, 500);
      } else {
        setRebuildState('error');
        pushLog({ level: 'error', message: data.message || 'Build failed.' });
      }
    } catch (error: any) {
      setRebuildState('error');
      pushLog({ level: 'error', message: error?.message || 'Build request failed.' });
    }
  }, [pushLog]);

  const restartServer = useCallback(async () => {
    try {
      await fetch('/api/restart', { method: 'POST' });
      pushLog({ level: 'success', message: 'Restart sent.' });
    } catch (_) {
      // Expected — server dies before response completes
    }
  }, [pushLog]);

  const stopServer = useCallback(async () => {
    if (!window.confirm('Stop the live server?')) {
      return;
    }
    try {
      const res = await fetch('/api/shutdown', { method: 'POST' });
      if (!res.ok) {
        const message = await res.text().catch(() => 'Shutdown request failed.');
        throw new Error(message || `HTTP ${res.status}`);
      }
      pushLog({ level: 'success', message: 'Shutdown sent.' });
    } catch (error: any) {
      const message = error?.message || 'Unable to stop server.';
      pushLog({ level: 'error', message });
      window.alert(`Unable to stop server: ${message}`);
    }
  }, [pushLog]);

  const backfillPairStatus = summary.backfill?.pair_status || {};
  const backfillPairs = Object.keys(backfillPairStatus).sort();

  return (
    <div className="shell">
      {(connectionState === 'disconnected' || connectionState === 'connecting' || summary.status === 'backfilling' || summary.status === 'starting') && (
        <div style={{
          position: 'fixed', inset: 0, zIndex: 9999,
          background: 'rgba(26, 21, 16, 0.92)',
          display: 'flex', flexDirection: 'column',
          alignItems: 'center', justifyContent: 'center',
          color: '#e8dcc8', fontFamily: 'inherit',
        }}>
          <div style={{fontSize: '1.4rem', fontWeight: 700, marginBottom: '12px'}}>
            {connectionState === 'disconnected' ? 'Server disconnected'
              : connectionState === 'connecting' ? 'Connecting to server...'
              : 'Server warming up'}
          </div>
          {(summary.status === 'backfilling' || summary.status === 'starting') && summary.backfill ? (() => {
            const bf = summary.backfill || {};
            const completed = Number(bf.completed || 0);
            const total = Number(bf.total || 0);
            const pct = total > 0 ? Math.round((completed / total) * 100) : 0;
            const phase = bf.phase || 'loading';
            const currentPair = bf.current_pair || '';
            return (
              <>
                <div style={{fontSize: '0.9rem', color: '#a69882', marginBottom: '16px'}}>
                  {phase === 'bars' ? 'Fetching market data' : phase === 'zones' ? 'Computing zones' : phase === 'scan' ? 'Running initial scan' : 'Loading'}
                  {currentPair ? ` — ${currentPair}` : ''}
                </div>
                <div style={{width: '300px', height: '6px', background: 'rgba(166, 152, 130, 0.2)', borderRadius: '3px', overflow: 'hidden', marginBottom: '8px'}}>
                  <div style={{width: `${pct}%`, height: '100%', background: '#d4a017', borderRadius: '3px', transition: 'width 0.3s'}} />
                </div>
                <div style={{fontSize: '0.85rem', color: '#a69882'}}>{pct}% ({completed}/{total})</div>
              </>
            );
          })() : (
            <div style={{fontSize: '0.9rem', color: '#a69882'}}>
              {connectionState === 'disconnected' ? 'Reconnecting automatically — the server may be restarting' : 'Connecting...'}
            </div>
          )}
          <div style={{marginTop: '20px', width: '40px', height: '40px', border: '3px solid #a69882', borderTopColor: 'transparent', borderRadius: '50%', animation: 'spin 1s linear infinite'}} />
          <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
        </div>
      )}
      <header className="hero">
        <div className="hero-title-row">
          <div>
            <h1><span className="eyebrow">FX support / resistance scanner</span>Forex Sentinel</h1>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '6px' }}>
            <NavLinks current="/" orientation="horizontal" />
            <div className="hero-actions hero-middle-actions" style={{ display: 'flex', flexDirection: 'row', gap: '12px' }}>
          <a
            href="/position-health"
            target="_blank"
            className="toolbar-btn hero-top-action-link"
            title="Position health"
            aria-label="Position health"
            style={{ textDecoration: 'none', fontSize: '1.05rem' }}
          >
            {HEALTH_ICON}
          </a>
          <a
            href="/order-audit-log"
            target="_blank"
            className="toolbar-btn hero-top-action-link"
            title="Order audit log"
            aria-label="Order audit log"
            style={{ textDecoration: 'none', fontSize: '1.05rem' }}
          >
            {AUDIT_LOG_ICON}
          </a>
          {summary.execution_available ? (
            <button
              id="trade-toggle-btn"
              type="button"
              className={`toolbar-btn hero-top-action-link hero-toggle-btn ${summary.execution_paused ? 'is-paused' : ''}`}
              onClick={() => void toggleExecution()}
              title={executionTogglePending ? 'Updating execution state' : summary.execution_paused ? 'Resume entries' : 'Pause entries'}
              aria-label={executionTogglePending ? 'Updating execution state' : summary.execution_paused ? 'Resume entries' : 'Pause entries'}
            >
              {executionTogglePending ? BACKTEST_SPINNER_ICON : summary.execution_paused ? RESUME_ICON : PAUSE_ICON}
            </button>
          ) : null}
          <button
            id="rerun-backtest-btn"
            type="button"
            className="toolbar-btn hero-top-action-link cobalt"
            onClick={() => void rerunBacktest()}
            disabled={String(summary.backtest?.status || 'idle') === 'starting' || String(summary.backtest?.status || 'idle') === 'running'}
            title={currentBacktestButtonText(summary.backtest || {})}
            aria-label={currentBacktestButtonText(summary.backtest || {})}
          >
            {String(summary.backtest?.status || 'idle') === 'starting' || String(summary.backtest?.status || 'idle') === 'running'
              ? BACKTEST_SPINNER_ICON
              : BACKTEST_ICON}
          </button>
          <button
            type="button"
            className="toolbar-btn hero-top-action-link blue"
            onClick={() => void rebuildUI()}
            disabled={rebuildState === 'building'}
            title="Rebuild UI"
            aria-label="Rebuild UI"
          >
            {rebuildState === 'building' ? BACKTEST_SPINNER_ICON : REBUILD_ICON}
          </button>
          <button
            type="button"
            className="toolbar-btn hero-top-action-link gold"
            onClick={() => void restartServer()}
            title="Restart"
            aria-label="Restart"
          >
            {RESTART_ICON}
          </button>
          <button
            id="stop-server-btn"
            type="button"
            className="toolbar-btn hero-top-action-link red"
            onClick={() => void stopServer()}
            title="Stop"
            aria-label="Stop"
          >
            {STOP_ICON}
          </button>
            </div>
          </div>
        </div>
      </header>

      <main className="board live-marketboard">
        <section className="left-side-section">
          <section className="metrics-grid" id="metrics-grid">
            <article className="metric-card">
              <span className="meta-label">Scan state</span>
              {isScanLive ? (
                <div className="metric-detail">
                  <span id="scan-status" className="pill pill-live">{scanStatus}</span>
                </div>
              ) : (
                <strong id="scan-status">{scanStatus}</strong>
              )}
              <span id="scan-progress" className="metric-detail">{scanProgressText(summary)}</span>
            </article>
            <article className="metric-card">
              <span className="meta-label">Signals</span>
              <strong id="signal-count">{signals.length || summary.signal_count || 0}</strong>
              <span id="pending-count" className="metric-detail">{summary.pending_count || 0} pending blockers</span>
            </article>
            <article className="metric-card">
              <span className="meta-label">Tracked positions</span>
              <strong id="position-count">{viewState.positions.length || summary.position_count || 0}</strong>
              <span id="execution-mode" className="metric-detail">{summary.execution_enabled ? 'Live execution enabled' : summary.execution_paused ? 'Execution paused' : 'Scan only'}</span>
            </article>
          </section>

          <AccountChart />

          <section className="panel panel-watchlist">
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Pair</th>
                    <th>State</th>
                    <th>Price</th>
                    <th>Support</th>
                    <th>Resistance</th>
                    <th>Setup</th>
                    <th>Signal</th>
                  </tr>
                </thead>
                <tbody id="watchlist-body">
                  {!sortedRows.length && summary.status === 'backfilling' && backfillPairs.length ? backfillPairs.map((pair) => {
                    const status = backfillPairStatus[pair] || 'pending';
                    const label = status === 'ready' ? 'live' : status === 'pending' ? 'wait' : 'connecting';
                    return (
                      <tr key={pair}>
                        <td><span className="pair-main">{pair}</span></td>
                        <td><span className={badgeClass(label)}>{status}</span></td>
                        <td colSpan={5} className="price" style={{ opacity: 0.5 }}>{status}</td>
                      </tr>
                    );
                  }) : null}
                  {!sortedRows.length && !(summary.status === 'backfilling' && backfillPairs.length) ? (
                    <tr><td colSpan={7} className="empty">Waiting for first scan.</td></tr>
                  ) : null}
                  {sortedRows.map((row) => <WatchlistRow key={row.pair} row={row} />)}
                </tbody>
              </table>
            </div>
          </section>
        </section>
        <section className="side-column">
          <section className="panel">
            <div className="panel-subhead">Tracked Positions</div>
            <div id="positions-list" className="stack-list compact-list">
              {!viewState.positions.length ? <div className="empty-card">No tracked positions.</div> : viewState.positions.map((position) => {
                const posKey = `${position.pair}:${position.direction}`;
                const pnlUp = Number(position.pnl_pips || 0) >= 0;
                const posSelected = posKey === selectedPositionKey;
                const dec = position.decimals ?? PRICE_DISPLAY_DECIMALS;
                return (
                  <article
                    key={posKey}
                    className={`mini-card mini-card-clickable ${posSelected ? 'mini-card-selected' : ''}`}
                    onClick={() => setSelectedPositionKey((c) => c === posKey ? null : posKey)}
                  >
                    <div className="mini-head" style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                      <div className="mini-head-copy">
                        <strong>{position.pair}</strong>
                        {' '}
                        <span className={badgeClass(position.direction)}>{position.direction}</span>
                        {' '}
                        <span className="pair-sub">{Number(position.size || 0).toLocaleString()} units</span>
                      </div>
                      {position.status !== 'OK' ? <span className={badgeClass(position.status)}>{position.status}</span> : null}
                    </div>
                    <div className="mini-meta" style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:'6px 12px'}}>
                      <div><span className="value-label">Entry</span><span className="value">{formatNumber(position.entry_price, dec)}</span></div>
                      <div><span className="value-label">SL</span><span className="value">{formatNumber(position.sl_price, dec)}</span></div>
                      <div><span className="value-label">TP</span><span className="value">{formatNumber(position.tp_price, dec)}</span></div>
                      <div><span className="value-label">Current</span><span className="value">{formatNumber(position.current_price, dec)}</span></div>
                      <div><span className="value-label">P/L pips</span><span className={`value ${pnlUp ? 'up' : 'down'}`}>{formatSigned(position.pnl_pips, 1, ' pips')}</span></div>
                      <div><span className="value-label">P/L</span><span className={`value ${pnlUp ? 'up' : 'down'}`}>{(() => {
                        const amt = position.pnl_amount != null ? `${position.pnl_amount >= 0 ? '+' : ''}${position.account_currency || '\u00a3'}${Number(position.pnl_amount).toFixed(2)}` : '\u2013';
                        const riskDist = position.entry_price && position.sl_price ? Math.abs(position.entry_price - position.sl_price) : 0;
                        const pnlDist = position.entry_price && position.current_price
                          ? (position.direction === 'LONG' ? position.current_price - position.entry_price : position.entry_price - position.current_price)
                          : 0;
                        const r = riskDist > 0 ? (pnlDist / riskDist) : null;
                        const rStr = r != null ? ` (${r >= 0 ? '+' : ''}${r.toFixed(2)}R)` : '';
                        return `${amt}${rStr}`;
                      })()}</span></div>
                    </div>
                    {posSelected ? (
                      <div onClick={(e) => e.stopPropagation()}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '6px', marginTop: '10px', marginBottom: '4px' }}>
                          <div style={{ display: 'flex', gap: '6px' }}>
                            <button
                              type="button"
                              onClick={() => setSelectedPositionKey(null)}
                              style={{ background: 'none', border: '1px solid var(--line)', borderRadius: '4px', padding: '2px 8px', cursor: 'pointer', fontSize: '0.75rem', color: 'var(--muted)' }}
                            >
                              Hide
                            </button>
                            <a
                              href={`/live-trade?pair=${encodeURIComponent(position.pair)}`}
                              style={{ background: 'none', border: '1px solid var(--line)', borderRadius: '4px', padding: '2px 8px', cursor: 'pointer', fontSize: '0.75rem', color: 'var(--accent)', textDecoration: 'none', display: 'inline-flex', alignItems: 'center' }}
                            >
                              View Chart
                            </a>
                          </div>
                          <button
                            type="button"
                            disabled={closingPositionKey === posKey}
                            onClick={() => void closeTrackedPosition(position)}
                            style={{
                              background: 'rgba(178, 59, 41, 0.12)',
                              border: '1px solid #b23b29',
                              borderRadius: '4px',
                              padding: '2px 8px',
                              cursor: closingPositionKey === posKey ? 'not-allowed' : 'pointer',
                              fontSize: '0.75rem',
                              color: '#b23b29',
                            }}
                          >
                            {closingPositionKey === posKey ? 'Closing...' : 'Close Trade'}
                          </button>
                        </div>
                        <PositionMiniChart
                          pair={position.pair}
                          entryPrice={position.entry_price}
                          slPrice={position.sl_price}
                          tpPrice={position.tp_price}
                        />
                      </div>
                    ) : null}
                  </article>
                );
              })}
            </div>
          </section>

          <section className="panel">
            <div className="panel-subhead">Active Signals</div>
            <div id="signals-list" className="stack-list">
              {!signals.length ? <div className="empty-card">No active signals.</div> : signals.map((signal) => {
                const plan = signal.size_plan || {};
                return (
                  <article key={`${signal.pair}:${signal.direction}`} className="signal-card">
                    <div className="signal-head" style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                      <div className="mini-head-copy">
                        <strong>{signal.pair}</strong>
                        {' '}
                        <span className={badgeClass(signal.direction)}>{signal.direction}</span>
                        {' '}
                        <span className="pair-sub">{signal.zone_type || 'setup'} · {signal.zone_strength || '–'}</span>
                      </div>
                      <span className={badgeClass('signal')}>SIGNAL</span>
                    </div>
                    <div className="signal-meta">
                      <div><span className="value-label">Entry</span><span className="value">{formatNumber(signal.entry_price, signal.decimals ?? PRICE_DISPLAY_DECIMALS)}</span></div>
                      <div><span className="value-label">Stop</span><span className="value">{formatNumber(signal.sl_price, signal.decimals ?? PRICE_DISPLAY_DECIMALS)}</span></div>
                      <div><span className="value-label">Target</span><span className="value">{formatNumber(signal.tp_price, signal.decimals ?? PRICE_DISPLAY_DECIMALS)}</span></div>
                      <div><span className="value-label">Units</span><span className="value">{plan.units ? Number(plan.units).toLocaleString() : '–'}</span></div>
                      <div><span className="value-label">Risk</span><span className="value">{plan.risk_amount ? `${formatNumber(plan.risk_amount, 2)} ${plan.account_currency || ''}` : '–'}</span></div>
                      <div><span className="value-label">Notional</span><span className="value">{plan.notional_account ? `${formatNumber(plan.notional_account, 0)} ${plan.account_currency || ''}` : '–'}</span></div>
                    </div>
                    <div className="signal-meta" style={{ marginTop: '0.85rem' }}>
                      <div><span className="value-label">Arrived</span><span className="value">{formatTimestamp(signal.arrived_at)}</span></div>
                      <div><span className="value-label">Last valid</span><span className="value">{formatTimestamp(signal.last_valid_at)}</span></div>
                    </div>
                  </article>
                );
              })}
            </div>
          </section>

          <section className="panel transactions-panel">
            <div className="split-panel">
              <div className="split-panel-section">
                <div className="panel-subhead">Transactions</div>
                <div className="execution-filter-toolbar">
                  {(['all', 'open', 'closed'] as const).map((mode) => (
                    <button
                      key={mode}
                      type="button"
                      className={`execution-filter-btn ${executionFilterMode === mode ? 'is-active' : ''}`}
                      onClick={() => setExecutionFilterMode(mode)}
                    >
                      {executionFilterButtonLabel(mode)}
                    </button>
                  ))}
                </div>
              <div id="executions-list" className="stack-list compact-list">
                {!filteredExecutions.length ? <div className="empty-card">No execution activity in the last 72 hours.</div> : filteredExecutions.map((execution) => {
                  const key = executionKey(execution);
                  const selected = key === selectedExecutionKey;
                  const pnlSummary = executionPnlROrPips(execution);
                  const isClosed = execution.closed_at !== null && execution.closed_at !== undefined;
                  return (
                    <article
                      key={key}
                      className={`mini-card mini-card-clickable ${selected ? 'mini-card-selected' : ''}`}
                      data-execution-key={key}
                      onClick={() => setSelectedExecutionKey((current) => current === key ? null : key)}
                    >
                      <div className="mini-head" style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                        <div className="mini-head-copy">
                          <strong>{execution.pair}</strong>
                          {' '}
                          <span className={badgeClass(execution.direction)}>{execution.direction}</span>
                          {' '}
                          <span className="pair-sub">{Number(execution.units || 0).toLocaleString()} units</span>
                        </div>
                        {(() => {
                          const badge = executionBadgeLabel(execution.status, execution.note);
                          return (
                            <span
                              className={execution.status === 'OPEN' ? 'pill pill-live' : badge.className}
                              style={execution.status === 'OPEN' ? { background: '#1f7a49', color: '#fff' } : undefined}
                            >
                              {execution.status === 'OPEN' ? 'OPEN' : badge.label}
                            </span>
                          );
                        })()}
                      </div>
                      <div className="mini-meta" style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:'6px 12px'}}>
                        <div><span className="value-label">Entry</span><span className="value">{formatExecutionPrice(execution.submitted_entry_price ?? execution.opened_price, execution)}</span></div>
                        <div><span className="value-label">SL</span><span className="value">{formatExecutionPrice(execution.submitted_sl_price, execution)}</span></div>
                        <div><span className="value-label">TP</span><span className="value">{formatExecutionPrice(execution.submitted_tp_price, execution)}</span></div>
                        {pnlSummary.value !== '\u2013' ? (
                          <div><span className="value-label">P/L</span><span className={`value ${pnlSummary.isUp ? 'up' : 'down'}`}>{pnlSummary.value}</span></div>
                        ) : null}
                        {execution.pnl_amount != null ? (
                          <div><span className="value-label">P/L £</span><span className={`value ${Number(execution.pnl_amount) >= 0 ? 'up' : 'down'}`}>{Number(execution.pnl_amount) >= 0 ? '+' : ''}{execution.account_currency || '\u00a3'}{Number(execution.pnl_amount).toFixed(2)}</span></div>
                        ) : null}
                        {isClosed && execution.closed_price !== null && execution.closed_price !== undefined ? (
                          <div><span className="value-label">Close</span><span className="value">{formatExecutionPrice(execution.closed_price, execution)}</span></div>
                        ) : null}
                        {isClosed && execution.close_reason ? (
                          <div><span className="value-label">Reason</span><span className="value">{execution.close_reason}</span></div>
                        ) : null}
                        {execution.note ? (
                          <div style={{gridColumn:'1 / -1', display:'flex', justifyContent:'space-between', gap:'0.5rem'}}>
                            <div style={{overflow:'hidden'}}><span className="value-label">Note</span><span className="value" style={{overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap'}}>{execution.note}</span></div>
                            <div style={{flexShrink:0}}><span className="value-label">When</span><span className="value">{formatTimestamp(execution.time)} · #{execution.order_id || '-'}</span></div>
                          </div>
                        ) : (
                          <div style={{gridColumn:'1 / -1'}}><span className="value-label">When</span><span className="value">{formatTimestamp(execution.time)} · #{execution.order_id || '-'}</span></div>
                        )}
                      </div>
                      {selected ? (
                        <div className="mini-detail" onClick={(e) => e.stopPropagation()}>
                          <button type="button" onClick={() => setSelectedExecutionKey(null)} style={{float:'right',background:'none',border:'1px solid var(--line)',borderRadius:'4px',padding:'2px 8px',cursor:'pointer',fontSize:'0.75rem',color:'var(--muted)',marginBottom:'4px'}}>Close</button>
                          <div><span className="value-label">Ticker</span><span className="value">{execution.pair || '–'}</span></div>
                          <div><span className="value-label">Date</span><span className="value">{formatDateOnly(execution.time)}</span></div>
                          <div><span className="value-label">Entry</span><span className="value">{formatExecutionPrice(execution.submitted_entry_price, execution)}</span></div>
                          <div><span className="value-label">SL / TP</span><span className="value">{formatExecutionPrice(execution.submitted_sl_price, execution)} / {formatExecutionPrice(execution.submitted_tp_price, execution)}</span></div>
                          <ExecutionMiniChart execution={execution} />
                        </div>
                      ) : null}
                    </article>
                  );
                })}
              </div>
            </div>
          </div>
          </section>

          <section className="panel panel-log">
            <div className="panel-head">
              <div>
                <p className="panel-kicker">Flow</p>
                <h2>Event log</h2>
              </div>
            </div>
            <div id="log-list" className="log-list">
              {!viewState.log.length ? <div className="empty-card">No events yet.</div> : [...viewState.log].reverse().map((entry, index) => (
                <article key={`${entry.ts || 'log'}:${index}`} className={`log-row ${levelTone(entry.level)}`}>
                  <p>{entry.message}</p>
                  <time>{entry.ts || ''}</time>
                </article>
              ))}
            </div>
          </section>
        </section>
      </main>
    </div>
  );
}





