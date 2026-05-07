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
  currency_balances: {},
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

function formatQualityPct(score: number | null | undefined): string {
  if (score === null || score === undefined || Number.isNaN(Number(score))) {
    return '–';
  }
  return `${Math.round(Number(score) * 100)}%`;
}

const QUALITY_BAR_SEGMENTS = 6;

function QualityBar({
  score,
  showPct = true,
  compact = false,
}: {
  score: number | null | undefined;
  showPct?: boolean;
  compact?: boolean;
}) {
  const raw = score === null || score === undefined ? NaN : Number(score);
  const normalized = Number.isFinite(raw) ? Math.max(0, Math.min(1, raw)) : 0;
  const filled = Math.round(normalized * QUALITY_BAR_SEGMENTS);
  const hasScore = Number.isFinite(raw);
  const color = !hasScore
    ? 'rgba(91,75,58,0.35)'
    : normalized < 0.33
      ? '#b23b29'
      : normalized < 0.67
        ? '#d4a017'
        : '#1f7a49';
  const segW = compact ? 4 : 6;
  const segH = compact ? 8 : 10;
  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: compact ? 4 : 6,
        verticalAlign: 'middle',
      }}
      title={`Signal quality: ${formatQualityPct(score)}`}
    >
      <span style={{ display: 'inline-flex', gap: 2 }}>
        {Array.from({ length: QUALITY_BAR_SEGMENTS }).map((_, i) => (
          <span
            key={i}
            style={{
              width: segW,
              height: segH,
              background: i < filled ? color : 'rgba(91,75,58,0.15)',
              borderRadius: 1,
            }}
          />
        ))}
      </span>
      {showPct ? (
        <span style={{ fontSize: compact ? '0.75em' : '0.85em', color: '#5b4b3a' }}>
          {formatQualityPct(score)}
        </span>
      ) : null}
    </span>
  );
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

function formatDurationSeconds(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '0s';
  }
  const seconds = Math.max(0, Math.round(Number(value)));
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  if (minutes < 60) {
    return remainder ? `${minutes}m ${remainder}s` : `${minutes}m`;
  }
  const hours = Math.floor(minutes / 60);
  const minuteRemainder = minutes % 60;
  return minuteRemainder ? `${hours}h ${minuteRemainder}m` : `${hours}h`;
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

function backfillPhaseLabel(phase?: string) {
  if (phase === 'zones') return 'Computing zones';
  if (phase === 'hourly') return 'Loading hourly';
  if (phase === 'bars') return 'Fetching market data';
  if (phase === 'seed') return 'Scanning gaps and seeding cache';
  if (phase === 'scan') return 'Running initial scan';
  if (phase === 'done') return 'Ready';
  return 'Loading';
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
const STATEMENT_ICON = '\uD83D\uDCB0';  // money bag icon
const DASHBOARD_PAIR_UPDATE_MS = 1_000;

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
  const activeStatuses = new Set(['OPEN', 'PARTIAL', 'SUBMITTED', 'PRESUBMITTED', 'FILLED', 'EXIT_SIGNAL']);
  return execution.closed_at != null || (status !== '' && !activeStatuses.has(status));
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

  if (message.type === 'positions_update') {
    return {
      ...previous,
      positions: message.positions || [],
      alerts: message.alerts || [],
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

const DATA_HEALTH_COLORS: Record<string, string> = {
  ok: '#33cc66',
  warn: '#ffaa00',
  stale: '#ff4444',
  closed: '#6b7280',
  starting: '#6b7280',
  unknown: '#6b7280',
};

function formatAgeSeconds(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds)) return '—';
  if (seconds < 90) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  const hours = Math.floor(seconds / 3600);
  const mins = Math.round((seconds % 3600) / 60);
  return mins ? `${hours}h${mins}m` : `${hours}h`;
}

function dataHealthMessage(health: SummaryState['data_health']): string {
  if (!health) return 'Data feed status unknown';
  const missing = (health.missing_pairs || []);
  const worstAge = formatAgeSeconds(health.worst_age_seconds ?? null);
  if (health.pipeline_status === 'persistence_not_started') {
    return `DB writer not started; persisted bars are ${worstAge} behind`;
  }
  if (health.pipeline_status === 'persistence_stopped') {
    return `DB writer stopped; persisted bars are ${worstAge} behind`;
  }
  if (health.pipeline_status === 'persistence_error') {
    return `DB writer error; persisted bars are ${worstAge} behind`;
  }
  switch (health.overall) {
    case 'ok':
      return `Data feed healthy (worst lag ${worstAge})`;
    case 'warn':
      return `Data feed lagging — worst pair ${health.worst_pair || '?'} ${worstAge} behind`;
    case 'stale':
      if (missing.length) {
        return `Data feed stalled — ${missing.length} pair(s) missing, worst ${health.worst_pair || '?'} ${worstAge} behind`;
      }
      return `Data feed stalled — worst pair ${health.worst_pair || '?'} ${worstAge} behind`;
    case 'closed':
      return 'FX market closed — data feed suspended';
    case 'starting':
      return 'Data feed initialising — waiting for first bars to land';
    case 'unknown':
    default:
      return 'Data feed status unknown';
  }
}

function DataHealthDot({ health }: { health: SummaryState['data_health'] }) {
  const overall = health?.overall ?? 'unknown';
  const color = DATA_HEALTH_COLORS[overall] ?? DATA_HEALTH_COLORS.unknown;
  const title = dataHealthMessage(health);
  return (
    <span
      title={title}
      aria-label={title}
      role="status"
      style={{
        display: 'inline-block',
        width: 10,
        height: 10,
        borderRadius: '50%',
        backgroundColor: color,
        marginLeft: 6,
        verticalAlign: 'middle',
        boxShadow: overall === 'stale' ? `0 0 6px ${color}` : 'none',
      }}
    />
  );
}

function DataHealthBanner({ health }: { health: SummaryState['data_health'] }) {
  if (!health || health.overall !== 'stale') return null;
  const pipelineIssue = String(health.pipeline_status || '').startsWith('persistence');
  const suffix = pipelineIssue
    ? 'DB writer will auto-restart; check terminal logs if this does not clear.'
    : 'check TWS / IBKR data subscription.';
  return (
    <div
      role="alert"
      style={{
        marginBottom: 12,
        padding: '10px 14px',
        borderRadius: 6,
        backgroundColor: 'rgba(255, 68, 68, 0.12)',
        border: '1px solid rgba(255, 68, 68, 0.5)',
        color: '#ff6a6a',
        fontWeight: 600,
      }}
    >
      ⚠ {dataHealthMessage(health)} — {suffix}
    </div>
  );
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
    const phase = backfillPhaseLabel(backfill.phase);
    const pct = backfill.total > 0 ? Math.round((backfill.completed / backfill.total) * 100) : 0;
    const current = backfill.current_pair ? ` • ${backfill.current_pair}` : '';
    return `${phase}: ${backfill.completed}/${backfill.total} (${pct}%)${current}`;
  }

  return `${summary.pairs_completed || 0} / ${summary.pairs_total || 0} pairs`;
}

function scanQueueTitle(summary: SummaryState) {
  const backlog = summary.scan_backlog;
  const busy = Number(backlog?.busy_count || 0);
  const pending = Number(backlog?.pending_count || 0);
  if (busy || pending) {
    return `${busy + pending} active`;
  }
  return 'Idle';
}

function scanQueueDetail(summary: SummaryState) {
  const backlog = summary.scan_backlog;
  const busy = Number(backlog?.busy_count || 0);
  const pending = Number(backlog?.pending_count || 0);
  const coalesced = Number(backlog?.coalesced_recent_count ?? backlog?.coalesced_count ?? 0);
  const coalescedWindow = Number(backlog?.coalesced_window_seconds || 60);
  const failed = Number(backlog?.failed_count || 0);
  const oldest = backlog?.oldest_pending_age_seconds;
  const pairNames = [
    ...(backlog?.busy_pairs || []).map((item) => item.pair),
    ...(backlog?.pending_pairs || []).map((item) => item.pair),
  ];
  const uniquePairs = Array.from(new Set(pairNames)).slice(0, 4);
  const pairText = uniquePairs.length ? ` • ${uniquePairs.join(', ')}` : '';
  const lagText = pending > 0 ? ` • oldest ${formatDurationSeconds(oldest)}` : '';
  const coalescedText = coalescedWindow === 60
    ? ` • coalesced ${coalesced}/min`
    : ` • coalesced ${coalesced}/${formatDurationSeconds(coalescedWindow)}`;
  const failedText = failed > 0 ? ` • failed ${failed}` : '';
  return `busy ${busy} • pending ${pending}${lagText}${coalescedText}${failedText}${pairText}`;
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
    currency_balances: payload?.currency_balances || {},
  };
}

const WatchlistRow = memo(function WatchlistRow({ row }: { row: PairRow }) {
  const signal = row.signal;
  const setupLabel = signal ? `${signal.zone_type} · ${signal.zone_strength}` : row.note || 'No setup';
  const setupCell = signal ? (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
      <span>{setupLabel}</span>
      <QualityBar score={signal.quality_score} compact />
    </span>
  ) : (
    <span>{setupLabel}</span>
  );
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
      <td>{setupCell}</td>
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

const CurrencyBalanceWarning = memo(function CurrencyBalanceWarning({ balances, baseCurrency, positions }: { balances: Record<string, number>; baseCurrency: string; positions: any[] }) {
  const [busy, setBusy] = useState<string | null>(null);
  const [result, setResult] = useState<Record<string, { ok: boolean; msg: string }>>({});
  const [submitted, setSubmitted] = useState<Set<string>>(new Set());

  // Determine which currencies are expected from tracked positions
  const positionCurrencies = new Set<string>();
  if (positions) {
    for (const pos of positions) {
      const pair = (pos.pair || '').toUpperCase();
      if (pair.length === 6) {
        positionCurrencies.add(pair.slice(0, 3));
        positionCurrencies.add(pair.slice(3, 6));
      }
    }
  }

  const nonBase = Object.entries(balances)
    .filter(([c, v]) => c !== baseCurrency && Math.abs(v) > 50)
    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]));
  if (!nonBase.length) return null;

  const residual = nonBase.filter(([c]) => !positionCurrencies.has(c));
  const expected = nonBase.filter(([c]) => positionCurrencies.has(c));

  const neutralize = async (currency: string, amount: number) => {
    if (submitted.has(currency)) return;
    const action = amount > 0 ? 'Sell' : 'Buy';
    const qty = Math.abs(amount).toLocaleString(undefined, { maximumFractionDigits: 0 });
    if (!confirm(`${action} ${qty} ${currency} to neutralize this balance?`)) return;
    setBusy(currency);
    setSubmitted((s) => new Set(s).add(currency));
    setResult((r) => { const next = { ...r }; delete next[currency]; return next; });
    try {
      const res = await fetch('/api/neutralize-currency', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ currency, amount }),
      });
      const data = await res.json();
      if (!res.ok) {
        setResult((r) => ({ ...r, [currency]: { ok: false, msg: data.error || 'Failed' } }));
        setSubmitted((s) => { const next = new Set(s); next.delete(currency); return next; });
      } else {
        setResult((r) => ({ ...r, [currency]: { ok: true, msg: data.message || 'Submitted' } }));
      }
    } catch (err: any) {
      setResult((r) => ({ ...r, [currency]: { ok: false, msg: err?.message || 'Network error' } }));
      setSubmitted((s) => { const next = new Set(s); next.delete(currency); return next; });
    } finally {
      setBusy(null);
    }
  };

  return (
    <section className="panel" style={{ marginBottom: '12px', background: residual.length ? 'rgba(178,59,41,0.06)' : 'rgba(91,75,58,0.04)', border: residual.length ? '1px solid rgba(178,59,41,0.2)' : '1px solid rgba(91,75,58,0.12)' }}>
      <div className="panel-subhead" style={{ color: residual.length ? '#b23b29' : 'var(--muted)' }}>Currency Balances</div>
      {residual.length > 0 ? (
        <>
          <div style={{ fontSize: '0.8rem', color: '#b23b29', marginBottom: '6px', fontWeight: 600 }}>
            Residual balances
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', fontSize: '0.82rem', marginBottom: expected.length ? '10px' : '0' }}>
            {residual.map(([currency, amount]) => (
              <div key={currency}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div>
                    <span style={{ fontWeight: 600 }}>{currency}</span>{' '}
                    <span style={{ color: amount >= 0 ? '#1f7a49' : '#b23b29' }}>
                      {amount >= 0 ? '+' : ''}{Number(amount).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </span>
                  </div>
                  <button
                    type="button"
                    disabled={busy === currency || submitted.has(currency)}
                    onClick={() => void neutralize(currency, amount)}
                    style={{ background: 'rgba(178,59,41,0.10)', border: '1px solid #b23b29', borderRadius: '4px', padding: '1px 8px', cursor: busy === currency || submitted.has(currency) ? 'not-allowed' : 'pointer', fontSize: '0.72rem', color: submitted.has(currency) ? '#8b949e' : '#b23b29', whiteSpace: 'nowrap' }}
                  >
                    {busy === currency ? 'Submitting...' : submitted.has(currency) ? 'Done' : `${amount > 0 ? 'Sell' : 'Buy'} ${Math.abs(amount).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
                  </button>
                </div>
                {result[currency] ? (
                  <div style={{ fontSize: '0.72rem', marginTop: '2px', color: result[currency].ok ? '#1f7a49' : '#b23b29' }}>
                    {result[currency].msg}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        </>
      ) : null}
      {expected.length > 0 ? (
        <>
          {residual.length > 0 ? <div style={{ fontSize: '0.75rem', color: 'var(--muted)', marginBottom: '4px' }}>From tracked positions</div> : null}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(100px, 1fr))', gap: '4px 12px', fontSize: '0.82rem' }}>
            {expected.map(([currency, amount]) => (
              <div key={currency} style={{ opacity: 0.7 }}>
                <span style={{ fontWeight: 600 }}>{currency}</span>{' '}
                <span style={{ color: amount >= 0 ? '#1f7a49' : '#b23b29' }}>
                  {amount >= 0 ? '+' : ''}{Number(amount).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                </span>
              </div>
            ))}
          </div>
        </>
      ) : null}
    </section>
  );
});

const PositionMiniChart = memo(function PositionMiniChart({
  pair,
  entryPrice,
  entryTime,
  direction,
  slPrice,
  tpPrice,
  livePrice,
  decimals = 4,
}: {
  pair: string;
  entryPrice?: number;
  entryTime?: string;
  direction?: string;
  slPrice?: number;
  tpPrice?: number;
  livePrice?: number | null;
  decimals?: number;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<any>(null);
  const seriesRef = useRef<any>(null);
  const lastBarRef = useRef<{ time: number; open: number; high: number; low: number; close: number } | null>(null);
  const lastLiveUpdateRef = useRef(0);
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
    let resizeObserver: ResizeObserver | null = null;

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

        const seriesOpts: any = {
          upColor: '#1f7a49', downColor: '#b23b29',
          borderUpColor: '#1f7a49', borderDownColor: '#b23b29',
          wickUpColor: '#1f7a49', wickDownColor: '#b23b29',
          priceFormat: { type: 'price', precision: decimals, minMove: 1 / Math.pow(10, decimals) },
        };
        // Constrain Y-axis to SL-TP range
        const hasSlTp = slPrice != null && tpPrice != null && !isNaN(slPrice) && !isNaN(tpPrice);
        if (hasSlTp) {
          const lo = Math.min(slPrice!, tpPrice!);
          const hi = Math.max(slPrice!, tpPrice!);
          const pad = (hi - lo) * 0.10;
          seriesOpts.autoscaleInfoProvider = () => ({
            priceRange: { minValue: lo - pad, maxValue: hi + pad },
            margins: { above: 0, below: 0 },
          });
        }
        const series = chart.addCandlestickSeries(seriesOpts);
        seriesRef.current = series;

        if (data.bars?.length) {
          series.setData(data.bars);
          const last = data.bars[data.bars.length - 1];
          lastBarRef.current = { time: last.time, open: last.open, high: last.high, low: last.low, close: last.close };

          // Entry marker on the bar closest to entry time
          if (entryTime) {
            const entryTs = Math.floor(new Date(entryTime).getTime() / 1000);
            let bestBar = data.bars[0];
            let bestDiff = Math.abs(bestBar.time - entryTs);
            for (const bar of data.bars) {
              const diff = Math.abs(bar.time - entryTs);
              if (diff < bestDiff) { bestBar = bar; bestDiff = diff; }
            }
            const isLong = direction === 'LONG';
            series.setMarkers([{
              time: bestBar.time,
              position: isLong ? 'belowBar' : 'aboveBar',
              color: '#d4a017',
              shape: isLong ? 'arrowUp' : 'arrowDown',
              text: 'Entry',
            }]);
          }
        }

        const line = (price: any, color: string, title: string, style?: number) => {
          if (price == null || isNaN(Number(price))) return;
          series.createPriceLine({ price: Number(price), color, lineWidth: 1, lineStyle: style ?? chartApi.LineStyle.Dotted, axisLabelVisible: true, title });
        };
        line(entryPrice, '#d4a017', 'Entry', chartApi.LineStyle.Solid);
        line(slPrice, '#b23b29', 'SL');
        line(tpPrice, '#1f7a49', 'TP');

        // Zone bands (exclude from auto-scale so SL/TP constrain the Y range)
        const bandScaleOpt = (slPrice != null && tpPrice != null) ? { autoscaleInfoProvider: () => null } : {};
        if (data.support) {
          const band = chart.addBaselineSeries({ baseValue: { type: 'price', price: data.support.lower }, topFillColor1: 'rgba(31,122,73,0.10)', topFillColor2: 'rgba(31,122,73,0.10)', topLineColor: 'transparent', bottomFillColor1: 'transparent', bottomFillColor2: 'transparent', bottomLineColor: 'transparent', lineWidth: 0, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false, ...bandScaleOpt });
          const pad = 365*24*3600; const now = Math.floor(Date.now()/1000);
          band.setData([{ time: now-pad, value: data.support.upper }, { time: now+pad, value: data.support.upper }]);
        }
        if (data.resistance) {
          const band = chart.addBaselineSeries({ baseValue: { type: 'price', price: data.resistance.lower }, topFillColor1: 'rgba(178,59,41,0.10)', topFillColor2: 'rgba(178,59,41,0.10)', topLineColor: 'transparent', bottomFillColor1: 'transparent', bottomFillColor2: 'transparent', bottomLineColor: 'transparent', lineWidth: 0, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false, ...bandScaleOpt });
          const pad = 365*24*3600; const now = Math.floor(Date.now()/1000);
          band.setData([{ time: now-pad, value: data.resistance.upper }, { time: now+pad, value: data.resistance.upper }]);
        }

        chart.timeScale().scrollToRealTime();
        setStatus('');

        resizeObserver = new ResizeObserver(() => {
          if (!active || chartRef.current !== chart) {
            return;
          }
          chart.applyOptions({ width: container.clientWidth });
        });
        resizeObserver.observe(container);
      } catch {
        if (active) setStatus('Failed to load chart');
      }
    }

    void load();
    return () => {
      active = false;
      if (resizeObserver) {
        resizeObserver.disconnect();
        resizeObserver = null;
      }
      seriesRef.current = null;
      lastBarRef.current = null;
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  }, [pair, entryPrice, entryTime, direction, slPrice, tpPrice, decimals]);

  useEffect(() => {
    if (livePrice == null || !seriesRef.current) {
      return;
    }

    const now = Date.now();
    const elapsed = now - lastLiveUpdateRef.current;
    if (elapsed < DASHBOARD_PAIR_UPDATE_MS) {
      return;
    }
    lastLiveUpdateRef.current = now;

    const price = Number(livePrice);
    if (!Number.isFinite(price) || price <= 0) {
      return;
    }

    const nowSec = Math.floor(now / 1000);
    const hourStart = nowSec - (nowSec % 3600);
    const last = lastBarRef.current;

    if (last && last.time === hourStart && seriesRef.current) {
      last.high = Math.max(last.high, price);
      last.low = Math.min(last.low, price);
      last.close = price;
      seriesRef.current.update(last);
    } else if (seriesRef.current) {
      const bar = { time: hourStart, open: price, high: price, low: price, close: price };
      lastBarRef.current = bar;
      seriesRef.current.update(bar);
    }
  }, [livePrice, pair]);

  return (
    <div className="mini-detail-wide">
      {status ? <div className="chart-status">{status}</div> : null}
      <div ref={containerRef} style={{ width: '100%', marginTop: '8px' }} />
    </div>
  );
});

function DailyPnlLabel({ closedPnl, positions }: { closedPnl?: number; positions?: any[] }) {
  const realised = closedPnl ?? 0;
  const unrealised = (positions || []).reduce((s: number, p: any) => s + (p.pnl_amount || 0), 0);
  const total = realised + unrealised;
  if (realised === 0 && unrealised === 0) return null;
  const up = total >= 0;
  return (
    <span className="metric-detail" style={{ fontWeight: 600, color: up ? '#1f7a49' : '#b23b29' }}>
      Day: {up ? '+' : ''}&pound;{total.toFixed(2)}
    </span>
  );
}

function AccountChart() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const tooltipRef = useRef<HTMLDivElement | null>(null);
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

        // Tooltip on crosshair hover
        chart.subscribeCrosshairMove((param: any) => {
          const tip = tooltipRef.current;
          if (!tip) return;
          if (!param.time || !param.point || param.point.x < 0 || param.point.y < 0) {
            tip.style.display = 'none';
            return;
          }
          const equity = param.seriesData?.get(balanceSeries);
          const pnl = param.seriesData?.get(pnlSeries);
          const eqVal = equity?.value ?? equity?.close;
          const pnlVal = pnl?.value;
          if (eqVal == null) { tip.style.display = 'none'; return; }
          let text = `Equity: \u00a3${Number(eqVal).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
          if (pnlVal != null) {
            const sign = pnlVal >= 0 ? '+' : '';
            text += `  |  P&L: ${sign}\u00a3${Number(pnlVal).toFixed(2)}`;
          }
          tip.textContent = text;
          tip.style.display = 'block';
        });

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
        : (
          <div style={{ position: 'relative' }}>
            <div ref={containerRef} style={{ width: '100%' }} />
            <div ref={tooltipRef} style={{ display: 'none', position: 'absolute', top: '4px', left: '8px', fontSize: '0.76rem', color: '#5b4b3a', background: 'rgba(255,250,242,0.9)', padding: '2px 8px', borderRadius: '4px', pointerEvents: 'none', zIndex: 10, fontWeight: 600 }} />
          </div>
        )
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
  const [selectedSignalKey, setSelectedSignalKey] = useState<string | null>(null);
  const [closingPositionKey, setClosingPositionKey] = useState<string | null>(null);
  const [liquidatingPositionKey, setLiquidatingPositionKey] = useState<string | null>(null);
  const [showStatement, setShowStatement] = useState(false);
  const [statementData, setStatementData] = useState<any>(null);
  const [statementLoading, setStatementLoading] = useState(false);
  const [statementError, setStatementError] = useState('');
  const [reconciliation, setReconciliation] = useState<any[]>([]);
  const [nowTick, setNowTick] = useState(() => Date.now());
  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const queueRef = useRef<any[]>([]);
  const frameRef = useRef<number | null>(null);
  const pairUpdateRef = useRef<Map<string, any>>(new Map());
  const pairUpdateTimerRef = useRef<number | null>(null);
  const lastPairUpdateFlushRef = useRef(0);

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

    function flushPairUpdates() {
      const updates = Array.from(pairUpdateRef.current.values());
      pairUpdateRef.current.clear();
      pairUpdateTimerRef.current = null;
      lastPairUpdateFlushRef.current = Date.now();
      if (!updates.length) {
        return;
      }
      setViewState((previous) => updates.reduce((nextState, message) => mergeStateWithMessage(nextState, message), previous));
      if (pairUpdateRef.current.size > 0) {
        schedulePairUpdateFlush();
      }
    }

    function schedulePairUpdateFlush() {
      if (pairUpdateTimerRef.current !== null) {
        return;
      }

      const elapsed = Date.now() - lastPairUpdateFlushRef.current;
      const delay = Math.max(0, DASHBOARD_PAIR_UPDATE_MS - elapsed);
      pairUpdateTimerRef.current = window.setTimeout(() => {
        flushPairUpdates();
      }, delay);
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
          if (message.type === 'pair_update' && message.row?.pair) {
            pairUpdateRef.current.set(message.row.pair, message);
            schedulePairUpdateFlush();
            return;
          }
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
      if (pairUpdateTimerRef.current !== null) {
        window.clearTimeout(pairUpdateTimerRef.current);
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

  useEffect(() => {
    if (!showStatement) return;
    setStatementLoading(true);
    setStatementError('');
    Promise.all([
      fetch('/api/daily-statement').then(r => r.json()),
      fetch('/api/daily-reconciliation').then(r => r.json()),
    ])
      .then(([stmtData, reconData]) => {
        if (stmtData.error) setStatementError(stmtData.error);
        else setStatementData(stmtData);
        if (reconData.rows) setReconciliation(reconData.rows);
      })
      .catch(e => setStatementError(e.message || 'Failed to load'))
      .finally(() => setStatementLoading(false));
  }, [showStatement]);

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

  const liquidateLivePosition = useCallback(async (position: PositionRow) => {
    const positionKey = `${position.pair}:${position.direction}`;
    if (liquidatingPositionKey === positionKey) {
      return;
    }
    const closeSide = position.direction === 'SHORT' ? 'BUY' : 'SELL';
    const size = Number(position.size || 0).toLocaleString();
    if (!window.confirm(
      `Liquidate live IBKR ${position.direction} ${position.pair}?\n\n`
      + `This cancels working ${position.pair} orders, then submits ${closeSide} for the verified live IBKR size.`
      + (size !== '0' ? `\nCurrent dashboard size: ${size} units.` : '')
    )) {
      return;
    }

    setLiquidatingPositionKey(positionKey);
    try {
      const res = await fetch('/api/live-position-liquidate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ pair: position.pair, direction: position.direction }),
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(payload?.error || 'Unable to liquidate live position.');
      }
      const cancelled = payload?.result?.cancelled_order_ids || [];
      const suffix = cancelled.length ? ` Cancelled orders: ${cancelled.join(', ')}.` : '';
      pushLog({ level: 'success', message: `${payload?.message || `Liquidation request sent for ${position.pair}.`}${suffix}` });
    } catch (error: any) {
      const message = error?.message || 'Unable to liquidate live position.';
      pushLog({ level: 'error', message });
      window.alert(`Unable to liquidate live position: ${message}`);
    } finally {
      setLiquidatingPositionKey(null);
    }
  }, [liquidatingPositionKey, pushLog]);

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
  const warmupPhaseLabel = backfillPhaseLabel(summary.backfill?.phase);

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
              : `Server warming up • ${warmupPhaseLabel}`}
          </div>
          {(summary.status === 'backfilling' || summary.status === 'starting') && summary.backfill ? (() => {
            const bf = summary.backfill || {};
            const completed = Number(bf.completed || 0);
            const total = Number(bf.total || 0);
            const pct = total > 0 ? Math.round((completed / total) * 100) : 0;
            const phase = bf.phase || 'loading';
            const currentPair = bf.current_pair || '';
            const currentDetail = bf.current_detail || '';
            return (
              <>
                <div style={{fontSize: '0.9rem', color: '#a69882', marginBottom: '16px'}}>
                  {backfillPhaseLabel(phase)}
                  {currentPair ? ` — ${currentPair}` : ''}
                  {currentDetail ? ` • ${currentDetail}` : ''}
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
            <h1><span className="eyebrow">FX support / resistance scanner</span>IBKR Forex</h1>
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
          <button
            type="button"
            className="toolbar-btn hero-top-action-link"
            onClick={() => setShowStatement(true)}
            title="Daily account statement"
            aria-label="Daily account statement"
            style={{ fontSize: '1.05rem' }}
          >
            {STATEMENT_ICON}
          </button>
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
          <DataHealthBanner health={summary.data_health} />
          <section className="metrics-grid" id="metrics-grid">
            <article className="metric-card">
              <span className="meta-label">Scan state</span>
              {isScanLive ? (
                <div className="metric-detail" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span id="scan-status" className="pill pill-live">{scanStatus}</span>
                  <DailyPnlLabel closedPnl={summary.daily_closed_pnl} positions={viewState.positions} />
                </div>
              ) : (
                <strong id="scan-status">{scanStatus}</strong>
              )}
              <span id="scan-progress" className="metric-detail">
                {scanProgressText(summary)}
                {' '}
                <button
                  type="button"
                  onClick={() => { fetch('/api/housekeeping', { method: 'POST' }).catch(() => {}); }}
                  title="Force housekeeping sync now"
                  style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: '0.78rem', color: 'var(--muted)', padding: '0 2px' }}
                >&#x27F3;</button>
              </span>
            </article>
            <article className="metric-card">
              <span className="meta-label">Scan queue</span>
              <strong id="scan-backlog-count">{scanQueueTitle(summary)}</strong>
              <span id="scan-backlog-detail" className="metric-detail">
                {scanQueueDetail(summary)}
              </span>
            </article>
            <article className="metric-card">
              <span className="meta-label">
                Signals
                <DataHealthDot health={summary.data_health} />
              </span>
              <strong id="signal-count">{signals.length || summary.signal_count || 0}</strong>
              <span id="pending-count" className="metric-detail">
                {summary.pending_count || 0} pending blockers
                {summary.pending_pairs?.length ? ` • ${summary.pending_pairs.join(', ')}` : ''}
              </span>
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
          <CurrencyBalanceWarning balances={viewState.currency_balances} baseCurrency={viewState.summary?.account_currency || 'GBP'} positions={viewState.positions} />
          <section className="panel">
            <div className="panel-subhead" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
              <span>Tracked Positions</span>
              {viewState.positions.length > 0 ? (() => {
                const totalAmt = viewState.positions.reduce((s: number, p: any) => s + (p.pnl_amount || 0), 0);
                const totalPips = viewState.positions.reduce((s: number, p: any) => s + (p.pnl_pips || 0), 0);
                const up = totalAmt >= 0;
                return (
                  <span style={{ fontSize: '0.78rem', fontWeight: 600, color: up ? '#1f7a49' : '#b23b29' }}>
                    {up ? '+' : ''}{totalPips.toFixed(1)} pips / {up ? '+' : ''}&pound;{totalAmt.toFixed(2)}
                  </span>
                );
              })() : null}
            </div>
            <div id="positions-list" className="stack-list compact-list">
              {!viewState.positions.length ? <div className="empty-card">No tracked positions.</div> : viewState.positions.map((position) => {
                const posKey = `${position.pair}:${position.direction}`;
                const pnlUp = Number(position.pnl_pips || 0) >= 0;
                const posSelected = posKey === selectedPositionKey;
                const dec = position.decimals ?? PRICE_DISPLAY_DECIMALS;
                const canLiquidateLive = position.position_source === 'ibkr_position';
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
                        {position.is_remainder ? (
                          <span className="pill" style={{marginLeft:'4px',fontSize:'0.65rem',padding:'1px 5px',background:'rgba(31,122,73,0.12)',color:'#1f7a49',border:'1px solid rgba(31,122,73,0.3)'}}>{Math.round((1 - (position.position_fraction ?? 0.5)) * 100)}% banked</span>
                        ) : position.position_fraction != null && position.position_fraction < 1 ? (
                          <span className="pill" style={{marginLeft:'4px',fontSize:'0.65rem',padding:'1px 5px',background:'rgba(210,168,23,0.12)',color:'#a68812',border:'1px solid rgba(210,168,23,0.3)'}}>Partial</span>
                        ) : null}
                        {position.entry_time ? <span className="pair-sub" style={{marginLeft:'auto'}}>{(() => {
                          const d = new Date(position.entry_time);
                          const now = Date.now();
                          const diffMs = now - d.getTime();
                          if (diffMs < 0 || isNaN(diffMs)) return d.toLocaleString();
                          const mins = Math.floor(diffMs / 60000);
                          if (mins < 60) return `${mins}m ago`;
                          const hrs = Math.floor(mins / 60);
                          if (hrs < 24) return `${hrs}h ${mins % 60}m ago`;
                          const days = Math.floor(hrs / 24);
                          return `${days}d ${hrs % 24}h ago`;
                        })()}</span> : null}
                      </div>
                      {position.status !== 'OK' ? <span className={badgeClass(position.status)}>{position.status}</span> : null}
                    </div>
                    <div className="mini-meta" style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:'6px 12px'}}>
                      <div><span className="value-label">Entry</span><span className="value">{formatNumber(position.entry_price, dec)}</span></div>
                      <div><span className="value-label">SL{position.sl_at_breakeven ? ' (BE)' : ''}</span><span className="value">{formatNumber(position.sl_price, dec)}</span></div>
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
                              href={`/chart?pair=${encodeURIComponent(position.pair)}`}
                              style={{ background: 'none', border: '1px solid var(--line)', borderRadius: '4px', padding: '2px 8px', cursor: 'pointer', fontSize: '0.75rem', color: 'var(--accent)', textDecoration: 'none', display: 'inline-flex', alignItems: 'center' }}
                            >
                              View Chart
                            </a>
                          </div>
                          <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                            <button
                              type="button"
                              disabled={!canLiquidateLive || liquidatingPositionKey === posKey}
                              onClick={() => void liquidateLivePosition(position)}
                              title={canLiquidateLive ? 'Cancel pair orders, then close the verified live IBKR position' : 'Only available for live IBKR position rows'}
                              style={{
                                background: canLiquidateLive ? 'rgba(178, 59, 41, 0.16)' : 'rgba(91, 75, 58, 0.08)',
                                border: canLiquidateLive ? '1px solid #b23b29' : '1px solid var(--line)',
                                borderRadius: '4px',
                                padding: '2px 8px',
                                cursor: (!canLiquidateLive || liquidatingPositionKey === posKey) ? 'not-allowed' : 'pointer',
                                fontSize: '0.75rem',
                                color: canLiquidateLive ? '#b23b29' : 'var(--muted)',
                              }}
                            >
                              {liquidatingPositionKey === posKey ? 'Liquidating...' : 'Liquidate IBKR'}
                            </button>
                            <button
                              type="button"
                              disabled={closingPositionKey === posKey}
                              onClick={() => void closeTrackedPosition(position)}
                              style={{
                                background: 'rgba(178, 59, 41, 0.08)',
                                border: '1px solid rgba(178, 59, 41, 0.55)',
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
                        </div>
                        <PositionMiniChart
                          pair={position.pair}
                          entryPrice={position.entry_price}
                          entryTime={position.entry_time}
                          direction={position.direction}
                          slPrice={position.sl_price}
                          tpPrice={position.tp_price}
                          livePrice={viewState.pairs[position.pair]?.price}
                          decimals={dec}
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
                const sigKey = `${signal.pair}:${signal.direction}`;
                const sigSelected = sigKey === selectedSignalKey;
                const dec = signal.decimals ?? PRICE_DISPLAY_DECIMALS;
                return (
                  <article
                    key={sigKey}
                    className={`signal-card mini-card-clickable ${sigSelected ? 'mini-card-selected' : ''}`}
                    onClick={() => setSelectedSignalKey((c) => c === sigKey ? null : sigKey)}
                    style={{ cursor: 'pointer' }}
                  >
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
                      <div><span className="value-label">Entry</span><span className="value">{formatNumber(signal.entry_price, dec)}</span></div>
                      <div><span className="value-label">Stop</span><span className="value">{formatNumber(signal.sl_price, dec)}</span></div>
                      <div><span className="value-label">Target</span><span className="value">{formatNumber(signal.tp_price, dec)}</span></div>
                      <div><span className="value-label">Quality</span><span className="value"><QualityBar score={signal.quality_score} /></span></div>
                      <div><span className="value-label">Units</span><span className="value">{plan.units ? Number(plan.units).toLocaleString() : '–'}</span></div>
                      <div><span className="value-label">Risk</span><span className="value">{plan.risk_amount ? `${formatNumber(plan.risk_amount, 2)} ${plan.account_currency || ''}` : '–'}</span></div>
                      <div><span className="value-label">Notional</span><span className="value">{plan.notional_account ? `${formatNumber(plan.notional_account, 0)} ${plan.account_currency || ''}` : '–'}</span></div>
                    </div>
                    <div className="signal-meta" style={{ marginTop: '0.85rem' }}>
                      <div><span className="value-label">Arrived</span><span className="value">{formatTimestamp(signal.arrived_at)}</span></div>
                      <div><span className="value-label">Last valid</span><span className="value">{formatTimestamp(signal.last_valid_at)}</span></div>
                    </div>
                    {sigSelected ? (
                      <div onClick={(e) => e.stopPropagation()} style={{ marginTop: '8px' }}>
                        <PositionMiniChart
                          pair={signal.pair}
                          entryPrice={signal.entry_price}
                          entryTime={signal.signal_time || signal.arrived_at}
                          direction={signal.direction}
                          slPrice={signal.sl_price}
                          tpPrice={signal.tp_price}
                          livePrice={viewState.pairs[signal.pair]?.price}
                          decimals={dec}
                        />
                      </div>
                    ) : null}
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
                          <div><span className="value-label">Reason</span><span className="value">{(() => {
                            const r = execution.close_reason;
                            if (r === 'PARTIAL_TP') return `Partial TP (${Math.round((1 - (execution.position_fraction ?? 0.5)) * 100)}% banked)`;
                            if (r === 'TP') return 'Take Profit';
                            if (r === 'SL') return 'Stop Loss';
                            if (r === 'EARLY_EXIT') return 'Early Exit (zone break)';
                            if (r === 'SIDEWAYS') return 'Sideways (no progress)';
                            if (r === 'TIME') return 'Time Exit (max hold)';
                            if (r === 'FRIDAY') return 'Friday Close';
                            if (r === 'EXTERNAL_CLOSE') return 'Closed externally';
                            if (r === 'MANUAL') return 'Manual close';
                            return r;
                          })()}</span></div>
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

      {showStatement ? (
        <div
          style={{ position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
          onClick={() => setShowStatement(false)}
        >
          <div
            style={{ background: 'var(--card-bg, #fffaf2)', borderRadius: '12px', padding: '20px 24px', maxWidth: 680, width: '95%', maxHeight: '85vh', overflow: 'auto', boxShadow: '0 8px 32px rgba(0,0,0,0.2)' }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
              <h2 style={{ margin: 0, fontSize: '1.1rem' }}>Daily Account Statement</h2>
              <button type="button" className="toolbar-btn" onClick={() => setShowStatement(false)}>Close</button>
            </div>

            {/* Daily P&L Reconciliation — always shown if available */}
            {reconciliation.length ? (
              <div style={{ marginBottom: '18px' }}>
                <h3 style={{ fontSize: '0.88rem', margin: '0 0 8px' }}>Daily P&L Reconciliation</h3>
                <table className="data-table" style={{ width: '100%', fontSize: '0.8rem' }}>
                  <thead><tr><th>Date</th><th style={{ textAlign: 'right' }}>Balance</th><th style={{ textAlign: 'right' }}>Trade P&L</th><th style={{ textAlign: 'right' }}>Actual Change</th><th style={{ textAlign: 'right' }}>Hidden Cost</th></tr></thead>
                  <tbody>
                    {reconciliation.slice(-7).map((r: any) => (
                      <tr key={r.date}>
                        <td>{r.date}</td>
                        <td style={{ textAlign: 'right' }}>{Number(r.balance).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                        <td style={{ textAlign: 'right', color: r.trade_pnl >= 0 ? '#1f7a49' : '#b23b29' }}>{r.trade_pnl >= 0 ? '+' : ''}{r.trade_pnl.toFixed(2)}</td>
                        <td style={{ textAlign: 'right', color: r.actual_change >= 0 ? '#1f7a49' : '#b23b29' }}>{r.actual_change >= 0 ? '+' : ''}{r.actual_change.toFixed(2)}</td>
                        <td style={{ textAlign: 'right', fontWeight: 700, color: r.hidden_cost >= -5 ? 'var(--muted)' : '#b23b29' }}>{r.hidden_cost >= 0 ? '+' : ''}{r.hidden_cost.toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}

            {statementLoading ? <div style={{ padding: '32px 0', textAlign: 'center', color: 'var(--muted)' }}>Loading statement from IBKR... (5-10s)</div> : null}
            {statementError ? <div style={{ padding: '16px 0', color: '#b23b29' }}>Error: {statementError}</div> : null}

            {!statementLoading && !statementError && statementData ? (() => {
              const t = statementData.totals || {};
              const live = statementData.live || {};
              return (
                <div style={{ fontSize: '0.84rem' }}>
                  {/* Summary */}
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: '10px', marginBottom: '18px', padding: '12px', background: 'rgba(91,75,58,0.04)', borderRadius: '8px' }}>
                    <div><div style={{ color: 'var(--muted)', fontSize: '0.72rem', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Realized P&L</div><div style={{ fontWeight: 700, color: t.total_realized_pnl >= 0 ? '#1f7a49' : '#b23b29' }}>{t.total_realized_pnl >= 0 ? '+' : ''}{t.total_realized_pnl?.toFixed(2)}</div></div>
                    <div><div style={{ color: 'var(--muted)', fontSize: '0.72rem', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Commissions</div><div style={{ fontWeight: 700, color: '#b23b29' }}>-{t.total_commissions?.toFixed(2)}</div></div>
                    <div><div style={{ color: 'var(--muted)', fontSize: '0.72rem', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Interest</div><div style={{ fontWeight: 700, color: t.total_interest >= 0 ? '#1f7a49' : '#b23b29' }}>{t.total_interest?.toFixed(2)}</div></div>
                    <div><div style={{ color: 'var(--muted)', fontSize: '0.72rem', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Fees</div><div style={{ fontWeight: 700, color: '#b23b29' }}>-{t.total_fees?.toFixed(2)}</div></div>
                    {live.current_equity != null ? <div><div style={{ color: 'var(--muted)', fontSize: '0.72rem', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Current Equity</div><div style={{ fontWeight: 700 }}>{live.account_currency || '\u00a3'}{Number(live.current_equity).toFixed(2)}</div></div> : null}
                  </div>

                  {/* Cash Transactions */}
                  {statementData.cash_transactions?.length ? (
                    <div style={{ marginBottom: '16px' }}>
                      <h3 style={{ fontSize: '0.88rem', margin: '0 0 8px' }}>Cash Transactions</h3>
                      <table className="data-table" style={{ width: '100%', fontSize: '0.8rem' }}>
                        <thead><tr><th>Type</th><th>Description</th><th>Currency</th><th style={{ textAlign: 'right' }}>Amount</th></tr></thead>
                        <tbody>
                          {statementData.cash_transactions.map((tx: any, i: number) => (
                            <tr key={i}>
                              <td>{tx.type}</td>
                              <td style={{ maxWidth: 240, overflow: 'hidden', textOverflow: 'ellipsis' }} title={tx.description}>{tx.description}</td>
                              <td>{tx.currency}</td>
                              <td style={{ textAlign: 'right', color: tx.amount >= 0 ? '#1f7a49' : '#b23b29' }}>{tx.amount >= 0 ? '+' : ''}{Number(tx.amount).toFixed(2)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : null}

                  {/* Interest */}
                  {statementData.interest?.length ? (
                    <div style={{ marginBottom: '16px' }}>
                      <h3 style={{ fontSize: '0.88rem', margin: '0 0 8px' }}>Interest Accruals</h3>
                      <table className="data-table" style={{ width: '100%', fontSize: '0.8rem' }}>
                        <thead><tr><th>Currency</th><th>Date</th><th style={{ textAlign: 'right' }}>Amount</th></tr></thead>
                        <tbody>
                          {statementData.interest.map((row: any, i: number) => (
                            <tr key={i}>
                              <td>{row.currency}</td>
                              <td>{row.date}</td>
                              <td style={{ textAlign: 'right', color: row.amount >= 0 ? '#1f7a49' : '#b23b29' }}>{row.amount >= 0 ? '+' : ''}{Number(row.amount).toFixed(4)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : null}

                  {/* Trades */}
                  {statementData.trades?.length ? (
                    <div>
                      <h3 style={{ fontSize: '0.88rem', margin: '0 0 8px' }}>Trades ({statementData.trades.length})</h3>
                      <table className="data-table" style={{ width: '100%', fontSize: '0.8rem' }}>
                        <thead><tr><th>Pair</th><th>Side</th><th>Qty</th><th>Price</th><th style={{ textAlign: 'right' }}>Commission</th><th style={{ textAlign: 'right' }}>P&L</th></tr></thead>
                        <tbody>
                          {statementData.trades.map((t: any, i: number) => (
                            <tr key={i}>
                              <td>{t.pair}</td>
                              <td>{t.side}</td>
                              <td>{Number(t.quantity).toLocaleString()}</td>
                              <td>{t.price}</td>
                              <td style={{ textAlign: 'right', color: '#b23b29' }}>-{Number(t.commission).toFixed(2)}</td>
                              <td style={{ textAlign: 'right', color: t.realized_pnl >= 0 ? '#1f7a49' : '#b23b29' }}>{t.realized_pnl >= 0 ? '+' : ''}{Number(t.realized_pnl).toFixed(2)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : null}
                </div>
              );
            })() : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}





