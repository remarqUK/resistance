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
    if (!selectedSignal) return '';
    const params = new URLSearchParams();
    if (selectedSignal.pair) {
      params.set('pair', String(selectedSignal.pair).toUpperCase());
    }
    if (selectedSignal.signal_id) {
      params.set('signal_id', String(selectedSignal.signal_id));
      return `/chart?${params.toString()}`;
    }
    return params.toString() ? `/chart?${params.toString()}` : '';
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
                  const rowKey = row.signal_id || `${row.pair}:${row.signal_time}:${row.direction}`;
                  const selectedRowKey = selectedSignal
                    ? (selectedSignal.signal_id || `${selectedSignal.pair}:${selectedSignal.signal_time}:${selectedSignal.direction}`)
                    : '';
                  const rowIsSelected = !!selectedSignal && rowKey === selectedRowKey;
                  return (
                    <tr
                      key={row.signal_id || `${row.pair}:${row.signal_time}:${row.direction}`}
                      className={`trade-log-row${rowIsSelected ? ' trade-log-row-selected' : ''}`}
                      style={{ cursor: 'pointer' }}
                        onClick={() => {
                          setSelectedSignal(rowIsSelected ? null : row);
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
        ) : null}
      </section>

      {selectedSignal ? (
        <section className="panel trade-log-chart-panel">
          <div className="trade-log-chart-header">
            <h2>Trade chart: {selectedSignal.pair || 'Signal'}</h2>
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
            title={`Trade chart for ${selectedSignal.pair || 'signal'}`}
          />
        </section>
      ) : null}
    </div>
  );
}
