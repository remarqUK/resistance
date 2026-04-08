import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { NavLinks } from '../components/NavLinks';
import '../styles/live-vs-backtest.css';

interface CombinedTrade {
  source: 'live' | 'backtest';
  pair: string;
  direction?: string;
  status?: string;
  entryTime: string;
  entryTs: number;
  exitTime?: string;
  entryPrice?: number;
  slPrice?: number;
  tpPrice?: number;
  pnlPips?: number;
  pnlR?: number;
  pnlGbp?: number;
  note?: string;
}

interface TimelineBucket {
  id: string;
  pair: string;
  entryTs: number;
  live?: CombinedTrade[];
  backtest?: CombinedTrade[];
}

function parseTime(value?: string | null): number | null {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.getTime();
}

function parseNumber(value: any): number | undefined {
  if (value === null || value === undefined || value === '') {
    return undefined;
  }
  const number = Number(value);
  return Number.isNaN(number) ? undefined : number;
}

function formatTime(value?: string | null, fallback = '') {
  if (!value) {
    return fallback;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return fallback;
  }
  return parsed.toLocaleString([], {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    timeZone: 'UTC',
  });
}

function formatNumber(value?: number | null, digits = 5) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '-';
  }
  return Number(value).toFixed(digits);
}

function formatSigned(value?: number | null, digits = 2, suffix = '') {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '-';
  }
  return `${value > 0 ? '+' : ''}${Number(value).toFixed(digits)}${suffix}`;
}

function normalizeLiveTrade(row: any): CombinedTrade | null {
  const pair = String(row?.pair || '').trim().toUpperCase();
  const entryTime = row?.opened_at || row?.signal_time || '';
  const entryTs = parseTime(entryTime);
  if (!pair || entryTs === null) {
    return null;
  }

  return {
    source: 'live',
    pair,
    direction: row?.direction,
    status: row?.status,
    entryTime: String(entryTime),
    entryTs,
    exitTime: row?.closed_at || row?.exit_at || undefined,
    entryPrice: parseNumber(row?.submitted_entry_price ?? row?.entry_price),
    slPrice: parseNumber(row?.sl_price),
    tpPrice: parseNumber(row?.tp_price),
    pnlPips: parseNumber(row?.pnl_pips),
    pnlR: parseNumber(row?.pnl_r),
    pnlGbp: parseNumber(row?.pnl_gbp),
    note: row?.note,
  };
}

function normalizeBacktestTrade(row: any): CombinedTrade | null {
  const pair = String(row?.pair || '').trim().toUpperCase();
  const entryTime = row?.entry_time || row?.exit_time || '';
  const entryTs = parseTime(entryTime);
  if (!pair || entryTs === null) {
    return null;
  }

  return {
    source: 'backtest',
    pair,
    direction: row?.direction,
    entryTime: String(entryTime),
    entryTs,
    exitTime: row?.exit_time || row?.closed_at || undefined,
    entryPrice: parseNumber(row?.entry_price),
    slPrice: parseNumber(row?.sl_price),
    tpPrice: parseNumber(row?.tp_price),
    pnlPips: parseNumber(row?.pnl_pips),
    pnlR: parseNumber(row?.pnl_r),
    pnlGbp: parseNumber(row?.pnl_amount),
    note: row?.exit_reason,
  };
}

function TradeCard({
  trade,
  source,
  showLabel,
}: {
  trade: CombinedTrade;
  source: 'live' | 'backtest';
  showLabel: boolean;
}) {
  const direction = trade.direction ? trade.direction.toUpperCase() : '';
  const sideClass = source === 'live' ? 'lvb-live-card' : 'lvb-backtest-card';
  const directionClass = direction ? `dir-${direction}` : '';

  return (
    <div className={`lvb-trade-card ${sideClass}`}>
      <div className="lvb-trade-header">
        <span className={`lvb-trade-direction ${directionClass}`}>{direction || '-'}</span>
        {showLabel && <span className="lvb-trade-source">{source === 'live' ? 'Live' : 'Backtest'}</span>}
      </div>
      <div className="lvb-trade-body">
        <div><strong>Pair</strong><span>{trade.pair}</span></div>
        {trade.status && <div><strong>Status</strong><span>{trade.status}</span></div>}
        {trade.entryPrice !== undefined ? <div><strong>Entry</strong><span>{formatNumber(trade.entryPrice, 5)}</span></div> : null}
        {trade.slPrice !== undefined ? <div><strong>SL</strong><span>{formatNumber(trade.slPrice, 5)}</span></div> : null}
        {trade.tpPrice !== undefined ? <div><strong>TP</strong><span>{formatNumber(trade.tpPrice, 5)}</span></div> : null}
        {trade.exitTime ? <div><strong>Exit</strong><span>{formatTime(trade.exitTime, '-')}</span></div> : null}
        {trade.pnlR !== undefined ? <div><strong>P/L R</strong><span className={trade.pnlR >= 0 ? 'up' : 'down'}>{formatSigned(trade.pnlR, 2, 'R')}</span></div> : null}
        {trade.pnlPips !== undefined ? <div><strong>P/L pips</strong><span className={trade.pnlPips >= 0 ? 'up' : 'down'}>{formatSigned(trade.pnlPips, 1, '')}</span></div> : null}
        {trade.pnlGbp !== undefined ? <div><strong>P/L GBP</strong><span className={trade.pnlGbp >= 0 ? 'up' : 'down'}>{formatSigned(trade.pnlGbp, 2, '')}</span></div> : null}
      </div>
      {trade.note ? <div className="lvb-trade-note" title={trade.note}>{trade.note}</div> : null}
    </div>
  );
}

export function LiveVsBacktestPage() {
  const [error, setError] = useState('');
  const [liveTrades, setLiveTrades] = useState<any[]>([]);
  const [backtestTrades, setBacktestTrades] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [tradeLogRes, backtestRes] = await Promise.all([
        fetch('/api/trade-log?limit=200'),
        fetch('/api/backtest/trades'),
      ]);

      const tradeLogPayload = await tradeLogRes.json();
      const backtestPayload = await backtestRes.json();

      if (!tradeLogRes.ok) {
        throw new Error(tradeLogPayload?.error || 'Unable to load live trades.');
      }
      if (!backtestRes.ok) {
        throw new Error(backtestPayload?.error || 'Unable to load backtest trades.');
      }

      setLiveTrades(Array.isArray(tradeLogPayload?.signals) ? tradeLogPayload.signals : []);
      setBacktestTrades(Array.isArray(backtestPayload?.trades) ? backtestPayload.trades : []);
      setError('');
    } catch (err: any) {
      setError(err?.message || 'Unable to load trade data.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 30000);
    return () => window.clearInterval(timer);
  }, [load]);

  const buckets = useMemo(() => {
    const bucketsByKey = new Map<string, TimelineBucket>();
    const addTrade = (trade: CombinedTrade) => {
      const timeKey = Math.floor(trade.entryTs / 1000);
      const key = `${trade.pair}|${timeKey}`;
      const bucket = bucketsByKey.get(key) || {
        id: key,
        pair: trade.pair,
        entryTs: trade.entryTs,
      } as TimelineBucket;

      if (trade.source === 'live') {
        bucket.live = [...(bucket.live || []), trade];
      } else {
        bucket.backtest = [...(bucket.backtest || []), trade];
      }

      bucketsByKey.set(key, bucket);
    };

    liveTrades
      .map((row) => normalizeLiveTrade(row))
      .forEach((trade) => {
        if (trade) addTrade(trade);
      });

    backtestTrades
      .map((row) => normalizeBacktestTrade(row))
      .forEach((trade) => {
        if (trade) addTrade(trade);
      });

    return Array.from(bucketsByKey.values()).sort((a, b) => {
      if (a.entryTs !== b.entryTs) {
        return b.entryTs - a.entryTs;
      }
      return a.pair.localeCompare(b.pair);
    });
  }, [liveTrades, backtestTrades]);

  const rowCount = buckets.reduce((acc, row) => acc + (row.live?.length || 0) + (row.backtest?.length || 0), 0);
  const hasRows = rowCount > 0;
  const liveCount = liveTrades.length;
  const backtestCount = backtestTrades.length;

  return (
    <div className="shell live-vs-backtest-page">
      <div className="hero">
        <div className="hero-title-row">
          <div>
            <span className="eyebrow">FX support / resistance</span>
            <h1>Live vs Backtest</h1>
            <p className="subtitle">
              Merged entry timeline: live trades on the left, backtest trades on the right.
              Matching pair/time entries are shown in the center.
            </p>
          </div>
          <NavLinks current="/live-vs-backtest" />
        </div>
      </div>

      <section className="panel">
        <div className="filter-toolbar" style={{ justifyContent: 'space-between' }}>
          <div className="lvb-metrics">
            <div className="field lvb-metric">
              <label>Total grouped rows</label>
              <div className="metric">{loading ? 'Refreshing...' : `${buckets.length}`}</div>
            </div>
            <div className="field lvb-metric">
              <label>Live rows</label>
              <div className="metric">{loading ? '...' : `${liveCount}`}</div>
            </div>
            <div className="field lvb-metric">
              <label>Backtest rows</label>
              <div className="metric">{loading ? '...' : `${backtestCount}`}</div>
            </div>
          </div>
          <button className="toolbar-btn" type="button" onClick={() => void load()}>
            Refresh
          </button>
        </div>

        {error ? <div className="empty">Error: {error}</div> : null}

        {!error && !hasRows ? (
          <div className="empty">{loading ? 'Loading...' : 'No merged trades found yet.'}</div>
        ) : null}

        {!error && hasRows ? (
          <>
            <div className="lvb-legend">
              <span className="lvb-legend-item lvb-live-ledger">Live on left</span>
              <span className="lvb-legend-item lvb-vs-ledger">Matched pair + timestamp in the center</span>
              <span className="lvb-legend-item lvb-backtest-ledger">Backtest on right</span>
            </div>
            <div className="lvb-timeline">
            {buckets.map((bucket) => {
              const liveCards = bucket.live || [];
              const backtestCards = bucket.backtest || [];
              const isBoth = liveCards.length > 0 && backtestCards.length > 0;
              const rowLabel = formatTime(
                liveCards[0]?.entryTime || backtestCards[0]?.entryTime,
                'Unknown time',
              );

              return (
                <div
                  key={bucket.id}
                  className={`lvb-timeline-row ${isBoth ? 'lvb-timeline-row-both' : ''}`}
                >
                  <div className="lvb-timeline-col lvb-timeline-col-live">
                    {liveCards.length ? (
                      liveCards.map((trade) => (
                        <TradeCard key={`${trade.pair}-${trade.entryTime}-${trade.direction}-${trade.pnlR ?? ''}-${trade.status ?? ''}`} trade={trade} source="live" showLabel />
                      ))
                    ) : (
                      <div className="lvb-empty-slot" />
                    )}
                  </div>

                  <div className="lvb-timeline-col lvb-timeline-col-middle">
                    <div className="lvb-time">{rowLabel}</div>
                    {isBoth ? <div className="lvb-vs-pill">VS</div> : null}
                    {isBoth ? <div className="lvb-pair">{bucket.pair}</div> : null}
                  </div>

                  <div className="lvb-timeline-col lvb-timeline-col-backtest">
                    {backtestCards.length ? (
                      backtestCards.map((trade) => (
                        <TradeCard key={`${trade.pair}-${trade.entryTime}-${trade.direction}-${trade.pnlR ?? ''}-${trade.pnlPips ?? ''}`} trade={trade} source="backtest" showLabel />
                      ))
                    ) : (
                      <div className="lvb-empty-slot" />
                    )}
                  </div>
                </div>
              );
            })}
            </div>
          </>
        ) : null}
      </section>
    </div>
  );
}
