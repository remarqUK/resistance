import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { NavLinks } from '../components/NavLinks';
import '../styles/trade-log.css';

interface AuditEntry {
  id: number;
  event_ts: string;
  function_name: string;
  pair: string | null;
  direction: string | null;
  action: string;
  request_json: string | null;
  response_json: string | null;
  error: string | null;
  duration_ms: number | null;
  order_ids: string | null;
}

interface AuditLogResponse {
  entries: AuditEntry[];
  pairs: string[];
  actions: string[];
  count: number;
}

function formatTs(value?: string) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 19);
  return date.toLocaleString([], {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZone: 'UTC',
  });
}

export function OrderAuditLogPage() {
  const [pair, setPair] = useState('');
  const [action, setAction] = useState('');
  const [data, setData] = useState<AuditLogResponse>({ entries: [], pairs: [], actions: [], count: 0 });
  const [error, setError] = useState('');
  const [expandedId, setExpandedId] = useState<number | null>(null);

  const load = useCallback(async () => {
    try {
      let url = '/api/order-audit-log?limit=200';
      if (pair) url += `&pair=${encodeURIComponent(pair)}`;
      if (action) url += `&action=${encodeURIComponent(action)}`;
      const res = await fetch(url);
      const payload = await res.json();
      if (!res.ok) throw new Error(payload.error || 'Unable to load audit log.');
      setData(payload);
      setError('');
    } catch (err: any) {
      setError(err?.message || 'Unable to load audit log.');
    }
  }, [pair, action]);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 30000);
    return () => window.clearInterval(timer);
  }, [load]);

  const rows = useMemo(() => data.entries || [], [data.entries]);

  return (
    <div className="shell trade-log-page">
      <div className="hero">
        <div className="hero-title-row">
          <div>
            <span className="eyebrow">FX support / resistance</span>
            <h1>Order Audit Log</h1>
            <p className="subtitle">Every IBKR API request and response, persisted for post-mortem debugging.</p>
          </div>
          <NavLinks current="/order-audit-log" />
        </div>
      </div>

      <section className="panel">
        <div className="filter-toolbar">
          <div className="field">
            <label htmlFor="audit-pair">Pair</label>
            <select id="audit-pair" value={pair} onChange={(e) => setPair(e.target.value)}>
              <option value="">All pairs</option>
              {(data.pairs || []).map((p) => (
                <option key={p} value={p}>{p}</option>
              ))}
            </select>
          </div>

          <div className="field">
            <label htmlFor="audit-action">Action</label>
            <select id="audit-action" value={action} onChange={(e) => setAction(e.target.value)}>
              <option value="">All actions</option>
              {(data.actions || []).map((a) => (
                <option key={a} value={a}>{a}</option>
              ))}
            </select>
          </div>

          <button className="toolbar-btn" type="button" onClick={() => void load()}>Refresh</button>
        </div>

        {error ? <div className="empty">Error: {error}</div> : null}

        {!error && !rows.length ? (
          <div className="empty">No audit log entries yet</div>
        ) : null}

        {!error && rows.length ? (
          <div className="table-wrap">
            <table className="data-table trade-log-table">
              <thead>
                <tr>
                  <th>Time (UTC)</th>
                  <th>Function</th>
                  <th>Pair</th>
                  <th>Dir</th>
                  <th>Action</th>
                  <th>Order IDs</th>
                  <th>Duration</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
                  const isExpanded = expandedId === row.id;
                  const hasError = !!row.error;
                  return (
                    <React.Fragment key={row.id}>
                      <tr
                        className={`trade-log-row${isExpanded ? ' trade-log-row-selected' : ''}`}
                        style={{ cursor: 'pointer' }}
                        onClick={() => setExpandedId(isExpanded ? null : row.id)}
                        title="Click to expand request/response"
                      >
                        <td>{formatTs(row.event_ts)}</td>
                        <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>{row.function_name}</td>
                        <td>{row.pair || ''}</td>
                        <td className={row.direction ? `dir-${row.direction}` : ''}>{row.direction || ''}</td>
                        <td>
                          <span className={`pill pill-${row.action}`}>{row.action}</span>
                        </td>
                        <td style={{ fontFamily: 'monospace', fontSize: '0.75rem', wordBreak: 'break-all', maxWidth: '120px' }}>{(row.order_ids || '').replace(/,/g, ', ')}</td>
                        <td>{row.duration_ms != null ? `${row.duration_ms.toFixed(0)}ms` : ''}</td>
                        <td style={{ color: hasError ? '#f85149' : undefined, maxWidth: '300px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={row.error || ''}>
                          {row.error || ''}
                        </td>
                      </tr>
                      {isExpanded ? (
                        <tr className="trade-log-row-selected">
                          <td colSpan={8} style={{ padding: '12px 16px' }}>
                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
                              <div>
                                <strong style={{ display: 'block', marginBottom: '6px', fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Request</strong>
                                <pre style={{ margin: 0, fontSize: '0.78rem', whiteSpace: 'pre-wrap', wordBreak: 'break-all', background: 'rgba(0,0,0,0.03)', padding: '8px', borderRadius: '4px' }}>
                                  {row.request_json ? JSON.stringify(JSON.parse(row.request_json), null, 2) : '(none)'}
                                </pre>
                              </div>
                              <div>
                                <strong style={{ display: 'block', marginBottom: '6px', fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Response</strong>
                                <pre style={{ margin: 0, fontSize: '0.78rem', whiteSpace: 'pre-wrap', wordBreak: 'break-all', background: 'rgba(0,0,0,0.03)', padding: '8px', borderRadius: '4px' }}>
                                  {row.response_json ? JSON.stringify(JSON.parse(row.response_json), null, 2) : '(none)'}
                                </pre>
                              </div>
                            </div>
                            {row.error ? (
                              <div style={{ marginTop: '12px' }}>
                                <strong style={{ display: 'block', marginBottom: '6px', fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.1em', color: '#f85149' }}>Error</strong>
                                <pre style={{ margin: 0, fontSize: '0.78rem', whiteSpace: 'pre-wrap', color: '#f85149', background: 'rgba(248,81,73,0.05)', padding: '8px', borderRadius: '4px' }}>
                                  {row.error}
                                </pre>
                              </div>
                            ) : null}
                          </td>
                        </tr>
                      ) : null}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>
    </div>
  );
}
