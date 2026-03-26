import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { NavLinks } from '../components/NavLinks';
import '../styles/trade-log.css';
import type { TradeLogResponse } from '../types';

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
  });
}

function formatNumber(value: any, digits = 5) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '';
  }
  return Number(value).toFixed(digits);
}

export function TradeLogPage() {
  const [pair, setPair] = useState('');
  const [status, setStatus] = useState('');
  const [data, setData] = useState<TradeLogResponse>({ signals: [], pairs: [], count: 0 });
  const [error, setError] = useState('');
  const [selectedSignal, setSelectedSignal] = useState<any | null>(null);

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

  const rows = useMemo(() => data.signals || [], [data.signals]);
  const chartUrl = useMemo(() => {
    if (!selectedSignal?.pair) return '';
    const targetDate = (selectedSignal.closed_at || selectedSignal.signal_time || '').slice(0, 10);
    const entry = selectedSignal.signal_time || '';
    const params = new URLSearchParams({ pair: String(selectedSignal.pair).toUpperCase() });
    if (targetDate) {
      params.set('date', targetDate);
    }
    if (entry) {
      params.set('entry', String(entry));
    }
    return `/replay?${params.toString()}`;
  }, [selectedSignal]);

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
          <div className="table-wrap">
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
                  const isClosed = String(row.status || '').toUpperCase() === 'CLOSED';
                  const closePrice = row.closed_price != null ? formatNumber(row.closed_price, 5) : '';
                  const closeReason = row.close_reason || '';
                  const rowIsSelected = selectedSignal && row.signal_id
                    ? row.signal_id === selectedSignal.signal_id
                    : false;
                  return (
                    <tr
                      key={row.signal_id || `${row.pair}:${row.signal_time}:${row.direction}`}
                      className={`trade-log-row${rowIsSelected ? ' trade-log-row-selected' : ''}`}
                      style={{ cursor: 'pointer' }}
                      onClick={() => {
                        if (row.opened_price != null || row.closed_price != null) {
                          const p = new URLSearchParams({ pair: row.pair });
                          if (row.opened_price != null) p.set('entry_price', row.opened_price);
                          if (row.opened_at || row.signal_time) p.set('entry_time', row.opened_at || row.signal_time);
                          if (row.closed_price != null) p.set('exit_price', row.closed_price);
                          if (row.closed_at) p.set('exit_time', row.closed_at);
                          if (row.submitted_sl_price != null) p.set('sl', row.submitted_sl_price);
                          else if (row.sl_price != null) p.set('sl', row.sl_price);
                          if (row.submitted_tp_price != null) p.set('tp', row.submitted_tp_price);
                          else if (row.tp_price != null) p.set('tp', row.tp_price);
                          if (row.direction) p.set('direction', row.direction);
                          window.location.href = `/live-trade?${p.toString()}`;
                        } else {
                          setSelectedSignal(rowIsSelected ? null : row);
                        }
                      }}
                      title="Click to review trade"
                    >
                      <td>{formatSignalTime(row.signal_time)}</td>
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
                      <td>{isClosed ? closePrice : ''}</td>
                      <td>{isClosed ? closeReason : ''}</td>
                      <td className="note" title={row.note || ''}>{row.note || ''}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>

      {selectedSignal ? (
        <section className="panel trade-log-chart-panel">
          <div className="trade-log-chart-header">
            <h2>Trade chart: {selectedSignal.pair}</h2>
            <button
              className="toolbar-btn"
              type="button"
              onClick={() => setSelectedSignal(null)}
            >
              Hide chart
            </button>
          </div>
          <iframe
            className="trade-log-chart-frame"
            src={chartUrl}
            title={`Replay for ${selectedSignal.pair}`}
          />
        </section>
      ) : null}
    </div>
  );
}
