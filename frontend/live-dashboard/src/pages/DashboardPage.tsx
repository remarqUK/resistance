import React, { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
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
  });
}

function badgeClass(value: any) {
  const token = String(value || 'muted').toLowerCase().replaceAll(/[^a-z0-9]+/g, '-');
  return `pill pill-${token}`;
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

function executionKey(execution: ExecutionRow) {
  return [
    execution.pair || '',
    execution.direction || '',
    execution.order_id || '',
    execution.time || '',
  ].join('|');
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
      ts: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
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
          href={`/chart?pair=${encodeURIComponent(row.pair)}`}
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

export function DashboardPage() {
  const [viewState, setViewState] = useState<DashboardState>(INITIAL_STATE);
  const [connectionState, setConnectionState] = useState<'connecting' | 'live' | 'disconnected' | 'error'>('connecting');
  const [executionTogglePending, setExecutionTogglePending] = useState(false);
  const [selectedExecutionKey, setSelectedExecutionKey] = useState<string | null>(null);
  const [nowTick, setNowTick] = useState(() => Date.now());
  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const queueRef = useRef<any[]>([]);
  const frameRef = useRef<number | null>(null);

  const pushLog = useCallback((entry: LogEntry) => {
    const nextEntry: LogEntry = {
      ts: entry.ts || new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
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
    if (!viewState.executions.some((execution) => executionKey(execution) === selectedExecutionKey)) {
      setSelectedExecutionKey(null);
    }
  }, [selectedExecutionKey, viewState.executions]);

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

  const connectionPill = useMemo(() => {
    if (connectionState === 'disconnected') {
      return { className: 'pill pill-disconnected', label: 'Disconnected' };
    }
    if (connectionState === 'error') {
      return { className: 'pill pill-disconnected', label: 'Error' };
    }
    if (summary.status === 'backfilling') {
      return { className: 'pill pill-connecting', label: 'Backfilling' };
    }
    if (summary.status === 'live') {
      return { className: 'pill pill-live', label: 'Live' };
    }
    if (summary.status === 'error') {
      return { className: 'pill pill-disconnected', label: 'Error' };
    }
    if (connectionState === 'connecting') {
      return { className: 'pill pill-muted', label: 'Connecting' };
    }
    return { className: 'pill pill-muted', label: summary.status || 'Starting' };
  }, [connectionState, summary.status]);

  const sizingSummary = useMemo(() => {
    if (summary.balance !== null && summary.balance !== undefined) {
      const currency = summary.account_currency ? ` ${summary.account_currency}` : '';
      const risk = summary.risk_pct !== null && summary.risk_pct !== undefined
        ? ` · Risk ${formatNumber(summary.risk_pct, 2)}%`
        : '';
      return `${formatNumber(summary.balance, 2)}${currency}${risk}`;
    }
    if (summary.mode) {
      return summary.mode;
    }
    return 'Resolving';
  }, [summary.account_currency, summary.balance, summary.mode, summary.risk_pct]);

  const executionModeText = summary.execution_available
    ? (summary.execution_paused ? 'Execution paused' : 'Live execution enabled')
    : 'Scan only';

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
      <header className="hero">
        <div className="hero-title-row">
          <div>
            <h1><span className="eyebrow">FX support / resistance scanner</span>Live Market Board</h1>
            <p className="subtitle" id="hero-sizing"><span id="sizing-summary">{sizingSummary}</span> · <span id="strategy-label">{summary.strategy_label || 'Strategy preset'}</span></p>
          </div>
          <div><span id="connection-pill" className={connectionPill.className}>{connectionPill.label}</span></div>
          <div>
            {summary.execution_available ? (
              <button
                id="trade-toggle-btn"
                type="button"
                className="toolbar-btn hero-toggle-btn"
                onClick={() => void toggleExecution()}
              >
                {executionTogglePending ? 'Updating...' : summary.execution_paused ? 'Resume Entries' : 'Pause Entries'}
              </button>
            ) : null}
          </div>
          <div><button id="fill-cache-btn" type="button" className="toolbar-btn" style={{ background: '#257aab', borderColor: '#257aab' }} onClick={() => void startFill()}>Fill</button></div>
          <div><button id="rerun-backtest-btn" type="button" className="toolbar-btn" style={{ background: '#3a6a8c', borderColor: '#3a6a8c' }} onClick={() => void rerunBacktest()} disabled={String(summary.backtest?.status || 'idle') === 'starting' || String(summary.backtest?.status || 'idle') === 'running'}>{currentBacktestButtonText(summary.backtest || {})}</button></div>
          <div><button id="stop-server-btn" type="button" className="toolbar-btn" style={{ color: '#fff', background: '#b23b29', borderColor: '#b23b29' }} onClick={() => void stopServer()}>Stop Server</button></div>
          <div className="hero-actions hero-actions-vertical hero-links-column">
            <a href="/trade-log" target="_blank" rel="noreferrer" className="hero-action">Trade Log</a>
            <a href="/replay" target="_blank" rel="noreferrer" className="hero-action">Strategy Replay</a>
            <a href="/backtest-trades" target="_blank" rel="noreferrer" className="hero-action">All Backtest Trades</a>
            <a href="/backtest-diary" target="_blank" rel="noreferrer" className="hero-action">Trade Diary</a>
          </div>
        </div>
      </header>

      <section className="metrics-grid" id="metrics-grid">
        <article className="metric-card">
          <span className="meta-label">Scan state</span>
          <strong id="scan-status">{summary.status || 'Starting'}</strong>
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
          <span id="execution-mode" className="metric-detail">{executionModeText}</span>
        </article>
        <article className="metric-card">
          <span className="meta-label">Next transaction</span>
          <strong id="next-transaction-timer">{nextTransaction.timer}</strong>
          <span id="next-transaction-at" className="metric-detail">{nextTransaction.at}</span>
        </article>
      </section>

      <main className="board live-marketboard">
        <section className="panel panel-watchlist">
          <p className="panel-note" style={{ maxWidth: 'none', textAlign: 'left' }}>Signals and tracked pairs float to the top while updates stream in per pair.</p>
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
        <section className="side-column">
          <section className="panel">
            <div id="signals-list" className="stack-list">
              {!signals.length ? <div className="empty-card">No active signals.</div> : signals.map((signal) => {
                const plan = signal.size_plan || {};
                return (
                  <article key={`${signal.pair}:${signal.direction}`} className="signal-card">
                    <div className="signal-head">
                      <div>
                        <strong>{signal.pair}</strong>
                        <span className="pair-sub">{signal.zone_type || 'setup'} · {signal.zone_strength || '–'}</span>
                      </div>
                      <span className={badgeClass(signal.direction)}>{signal.direction}</span>
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

          <section className="panel">
            <div id="positions-list" className="stack-list">
              {!viewState.positions.length ? <div className="empty-card">No tracked positions.</div> : viewState.positions.map((position) => {
                const pnlUp = Number(position.pnl_pips || 0) >= 0;
                const badge = position.status === 'OK' ? position.direction : position.status || 'OK';
                return (
                  <article key={`${position.pair}:${position.direction}:${position.entry_price ?? ''}`} className="position-card">
                    <div className="position-head">
                      <div>
                        <strong>{position.pair}</strong>
                        <span className="pair-sub">{Number(position.size || 0).toLocaleString()} units</span>
                      </div>
                      <span className={badgeClass(badge)}>{badge}</span>
                    </div>
                    <div className="position-meta">
                      <div><span className="value-label">Entry</span><span className="value">{formatNumber(position.entry_price, position.decimals ?? PRICE_DISPLAY_DECIMALS)}</span></div>
                      <div><span className="value-label">Current</span><span className="value">{formatNumber(position.current_price, position.decimals ?? PRICE_DISPLAY_DECIMALS)}</span></div>
                      <div><span className="value-label">Direction</span><span className="value">{position.direction}</span></div>
                      <div><span className="value-label">P/L</span><span className={`value ${pnlUp ? 'up' : 'down'}`}>{formatSigned(position.pnl_pips, 1, ' pips')}</span></div>
                    </div>
                  </article>
                );
              })}
            </div>
          </section>
        </section>

        <section className="panel">
          <div className="split-panel">
            <div className="split-panel-section">
              <div className="panel-subhead">Exit alerts</div>
              <div id="alerts-list" className="stack-list compact-list">
                {!viewState.alerts.length ? <div className="empty-card">No exit alerts.</div> : viewState.alerts.map((alert) => (
                  <article key={`${alert.pair}:${alert.direction}:${alert.exit_reason}:${alert.current_price ?? ''}`} className="mini-card">
                    <div className="mini-head">
                      <div>
                        <strong>{alert.pair}</strong>
                        <span className="pair-sub">{alert.direction}</span>
                      </div>
                      <span className={badgeClass('exit')}>{alert.exit_reason}</span>
                    </div>
                    <div className="mini-meta">
                      <div><span className="value-label">Current</span><span className="value">{formatNumber(alert.current_price, alert.decimals ?? PRICE_DISPLAY_DECIMALS)}</span></div>
                      <div><span className="value-label">P/L</span><span className={`value ${Number(alert.pnl_pips || 0) >= 0 ? 'up' : 'down'}`}>{formatSigned(alert.pnl_pips, 1, ' pips')}</span></div>
                    </div>
                  </article>
                ))}
              </div>
            </div>
            <div className="split-panel-section">
              <div className="panel-subhead">Transactions</div>
              <div id="executions-list" className="stack-list compact-list">
                {!viewState.executions.length ? <div className="empty-card">No execution activity.</div> : [...viewState.executions].reverse().map((execution) => {
                  const key = executionKey(execution);
                  const selected = key === selectedExecutionKey;
                  return (
                    <article
                      key={key}
                      className={`mini-card mini-card-clickable ${selected ? 'mini-card-selected' : ''}`}
                      data-execution-key={key}
                      onClick={() => setSelectedExecutionKey((current) => current === key ? null : key)}
                    >
                      <div className="mini-head">
                        <div className="mini-head-copy">
                          <strong>{execution.pair}</strong>
                          <span className="pair-sub">{execution.direction} - {Number(execution.units || 0).toLocaleString()} units</span>
                        </div>
                        <span className={badgeClass(execution.status)}>{execution.status}</span>
                      </div>
                      <div className="mini-meta mini-meta-single">
                        <div><span className="value-label">When / Order</span><span className="value">{formatTimestamp(execution.time)} - #{execution.order_id || '-'}</span></div>
                      </div>
                      <div className="mini-note">{execution.note || '-'}</div>
                      {selected ? (
                        <div className="mini-detail">
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
      </main>
    </div>
  );
}
