import React, { useCallback, useEffect, useState } from 'react';
import { NavLinks } from '../components/NavLinks';
import '../styles/trade-log.css';

interface Position {
  pair: string;
  direction: string;
  size: number;
  avg_cost: number;
  has_brackets: boolean;
  bracket_orders: BracketOrder[];
}

interface BracketOrder {
  pair: string;
  action: string;
  quantity: number;
  order_type: string;
  lmt_price: number;
  aux_price: number;
  order_id: number | null;
  parent_id: number | null;
  oca_group: string;
  tif: string;
  status: string;
  order_ref: string;
}

interface ClosedTrade {
  pair: string;
  direction: string;
  status: string;
  entry_price: number;
  sl_price: number;
  tp_price: number;
  closed_price: number | null;
  close_reason: string | null;
  close_source: string | null;
  detected_at: string;
  closed_at: string;
  pnl_pips: number | null;
  pnl_amount: number | null;
}

interface HealthData {
  positions: Position[];
  open_orders: BracketOrder[];
  orphaned_orders: BracketOrder[];
  closed_trades: ClosedTrade[];
  hours: number;
}

function formatPrice(value: number | null | undefined, pair?: string) {
  if (value == null) return '';
  const isJpy = pair ? pair.includes('JPY') : false;
  return value.toFixed(isJpy ? 3 : 5);
}

function formatSize(size: number) {
  const abs = Math.abs(size);
  if (abs >= 1000) return `${(abs / 1000).toFixed(0)}K`;
  return abs.toFixed(0);
}

function formatTs(value?: string) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 19);
  return date.toLocaleString([], {
    month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    timeZone: 'UTC',
  });
}

const SPINNER = (
  <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', padding: '60px 0' }}>
    <div style={{ width: '40px', height: '40px', border: '3px solid #a69882', borderTopColor: 'transparent', borderRadius: '50%', animation: 'spin 1s linear infinite' }} />
    <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
  </div>
);

export function PositionHealthPage() {
  const [data, setData] = useState<HealthData | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/position-health?hours=12');
      const payload = await res.json();
      if (!res.ok) throw new Error(payload.error || 'Unable to load position health.');
      setData(payload);
      setError('');
    } catch (err: any) {
      setError(err?.message || 'Unable to load position health.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 30000);
    return () => window.clearInterval(timer);
  }, [load]);

  const positions = data?.positions || [];
  const orphaned = data?.orphaned_orders || [];
  const closed = data?.closed_trades || [];
  const totalPnl = closed.reduce((sum, t) => sum + (t.pnl_pips || 0), 0);
  const totalAmount = closed.reduce((sum, t) => sum + (t.pnl_amount || 0), 0);
  const winners = closed.filter(t => (t.pnl_pips || 0) > 0).length;
  const losers = closed.filter(t => (t.pnl_pips || 0) < 0).length;

  return (
    <div className="shell trade-log-page">
      <div className="hero">
        <div className="hero-title-row">
          <div>
            <span className="eyebrow">FX support / resistance</span>
            <h1>Position Health</h1>
            <p className="subtitle">Live IBKR positions, bracket protection status, and recent trade history.</p>
          </div>
          <NavLinks current="/position-health" />
        </div>
      </div>

      {loading && !data ? SPINNER : null}
      {error ? <div className="empty" style={{ padding: '20px' }}>Error: {error}</div> : null}

      {data ? (
        <>
          {/* Open Positions */}
          <section className="panel" style={{ marginBottom: '20px' }}>
            <h2 style={{ margin: '0 0 12px', fontSize: '1.05rem' }}>
              Open Positions
              <span style={{ fontWeight: 'normal', fontSize: '0.85rem', marginLeft: '10px', color: '#8b949e' }}>
                {positions.length} position{positions.length !== 1 ? 's' : ''}
              </span>
              {loading ? <span style={{ marginLeft: '8px', fontSize: '0.75rem', color: '#a69882' }}>{'\u27F3'}</span> : null}
            </h2>
            {!positions.length ? (
              <div className="empty">No open positions</div>
            ) : (
              <div className="table-wrap">
                <table className="data-table trade-log-table">
                  <thead>
                    <tr>
                      <th>Pair</th>
                      <th>Direction</th>
                      <th>Size</th>
                      <th>Avg Cost</th>
                      <th>Brackets</th>
                      <th>TP Price</th>
                      <th>SL Price</th>
                      <th>TP Order</th>
                      <th>SL Order</th>
                      <th>OCA Group</th>
                    </tr>
                  </thead>
                  <tbody>
                    {positions.map((pos) => {
                      const tp = pos.bracket_orders.find(o => o.order_type === 'LMT');
                      const sl = pos.bracket_orders.find(o => o.order_type === 'STP');
                      return (
                        <tr key={`${pos.pair}:${pos.direction}`}>
                          <td style={{ fontWeight: 600 }}>{pos.pair}</td>
                          <td className={`dir-${pos.direction}`}>{pos.direction}</td>
                          <td>{formatSize(pos.size)}</td>
                          <td>{formatPrice(pos.avg_cost, pos.pair)}</td>
                          <td>
                            {pos.has_brackets ? (
                              <span style={{ color: '#3fb950' }}>Protected</span>
                            ) : (
                              <span style={{ color: '#f85149', fontWeight: 600 }}>UNPROTECTED</span>
                            )}
                          </td>
                          <td>{tp ? formatPrice(tp.lmt_price, pos.pair) : ''}</td>
                          <td>{sl ? formatPrice(sl.aux_price, pos.pair) : ''}</td>
                          <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>{tp?.order_id || ''}</td>
                          <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>{sl?.order_id || ''}</td>
                          <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>{tp?.oca_group || sl?.oca_group || ''}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          {/* Orphaned Orders */}
          {orphaned.length > 0 ? (
            <section className="panel" style={{ marginBottom: '20px', borderLeft: '3px solid #f85149' }}>
              <h2 style={{ margin: '0 0 12px', fontSize: '1.05rem', color: '#f85149' }}>
                Orphaned Orders
                <span style={{ fontWeight: 'normal', fontSize: '0.85rem', marginLeft: '10px' }}>
                  {orphaned.length} order{orphaned.length !== 1 ? 's' : ''} with no matching position
                </span>
              </h2>
              <div className="table-wrap">
                <table className="data-table trade-log-table">
                  <thead>
                    <tr>
                      <th>Pair</th>
                      <th>Action</th>
                      <th>Type</th>
                      <th>Qty</th>
                      <th>Price</th>
                      <th>Order ID</th>
                      <th>Status</th>
                      <th>Ref</th>
                    </tr>
                  </thead>
                  <tbody>
                    {orphaned.map((o) => (
                      <tr key={o.order_id}>
                        <td>{o.pair}</td>
                        <td>{o.action}</td>
                        <td>{o.order_type}</td>
                        <td>{formatSize(o.quantity)}</td>
                        <td>{formatPrice(o.order_type === 'STP' ? o.aux_price : o.lmt_price, o.pair)}</td>
                        <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>{o.order_id}</td>
                        <td>{o.status}</td>
                        <td style={{ fontFamily: 'monospace', fontSize: '0.75rem', maxWidth: '200px', overflow: 'hidden', textOverflow: 'ellipsis' }}>{o.order_ref}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          {/* Closed Trades */}
          <section className="panel">
            <h2 style={{ margin: '0 0 12px', fontSize: '1.05rem' }}>
              Closed Trades
              <span style={{ fontWeight: 'normal', fontSize: '0.85rem', marginLeft: '10px', color: '#8b949e' }}>
                last {data.hours}h &mdash; {closed.length} trade{closed.length !== 1 ? 's' : ''}
                {closed.length > 0 ? (
                  <>
                    {' '}&mdash;{' '}
                    <span style={{ color: '#3fb950' }}>{winners}W</span>
                    {' / '}
                    <span style={{ color: '#f85149' }}>{losers}L</span>
                    {' '}&mdash;{' '}
                    <span style={{ color: totalPnl >= 0 ? '#3fb950' : '#f85149', fontWeight: 600 }}>
                      {totalPnl >= 0 ? '+' : ''}{totalPnl.toFixed(1)} pips
                    </span>
                    {totalAmount !== 0 ? (
                      <>
                        {' '}&mdash;{' '}
                        <span style={{ color: totalAmount >= 0 ? '#3fb950' : '#f85149', fontWeight: 600 }}>
                          {totalAmount >= 0 ? '+' : ''}&pound;{totalAmount.toFixed(2)}
                        </span>
                      </>
                    ) : null}
                  </>
                ) : null}
              </span>
            </h2>
            {!closed.length ? (
              <div className="empty">No closed trades in the last {data.hours} hours</div>
            ) : (
              <div className="table-wrap">
                <table className="data-table trade-log-table">
                  <thead>
                    <tr>
                      <th>Closed At</th>
                      <th>Pair</th>
                      <th>Dir</th>
                      <th>Entry</th>
                      <th>Close</th>
                      <th>SL</th>
                      <th>TP</th>
                      <th>P&amp;L Pips</th>
                      <th>P&amp;L &pound;</th>
                      <th>Reason</th>
                      <th>Source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {closed.map((t, i) => {
                      const pnl = t.pnl_pips;
                      const pnlClass = pnl != null ? (pnl >= 0 ? 'up' : 'down') : '';
                      const pnlStr = pnl != null ? `${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}` : '';
                      const amt = t.pnl_amount;
                      const amtClass = amt != null ? (amt >= 0 ? 'up' : 'down') : '';
                      const amtStr = amt != null ? `${amt >= 0 ? '+' : ''}\u00a3${amt.toFixed(2)}` : '';
                      return (
                        <tr key={`${t.pair}:${t.closed_at}:${i}`}>
                          <td>{formatTs(t.closed_at)}</td>
                          <td style={{ fontWeight: 600 }}>{t.pair}</td>
                          <td className={`dir-${t.direction}`}>{t.direction}</td>
                          <td>{formatPrice(t.entry_price, t.pair)}</td>
                          <td>{formatPrice(t.closed_price, t.pair)}</td>
                          <td>{formatPrice(t.sl_price, t.pair)}</td>
                          <td>{formatPrice(t.tp_price, t.pair)}</td>
                          <td className={pnlClass} style={{ fontWeight: 600 }}>{pnlStr}</td>
                          <td className={amtClass} style={{ fontWeight: 600 }}>{amtStr}</td>
                          <td>{t.close_reason || ''}</td>
                          <td style={{ fontSize: '0.78rem', color: '#8b949e' }}>{t.close_source || ''}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </>
      ) : null}
    </div>
  );
}
