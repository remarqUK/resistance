import React, { useEffect } from 'react';
import { NavLinks } from '../components/NavLinks';

export function BacktestTradesPage() {
  useEffect(() => {
    if (window.__fxBacktestTradesScriptLoaded) return;
    const script = document.createElement('script');
    script.src = '/static/backtest_trades.js';
    script.async = false;
    script.onload = () => { window.__fxBacktestTradesScriptLoaded = true; };
    document.body.appendChild(script);
  }, []);

  return (
    <div className="shell">
      <div className="hero">
        <div className="hero-title-row">
          <div>
            <span className="eyebrow">FX support / resistance</span>
            <h1>Backtest Trades</h1>
          </div>
          <NavLinks current="/backtest-trades" />
        </div>
        <p className="subtitle">Read-only backtest fills from cached runs. Filter by pair before replaying the current results.</p>
      </div>

      <section className="panel">
        <div className="filter-toolbar">
          <div className="field">
            <label htmlFor="backtest-filter">Backtest</label>
            <select id="backtest-filter">
              <option value="">Loading runs...</option>
            </select>
          </div>
          <div className="field">
            <label htmlFor="pair-filter">Ticker</label>
            <select id="pair-filter">
              <option value="">All pairs</option>
            </select>
          </div>
          <button id="load-btn" className="toolbar-btn" type="button">Load</button>
        </div>
        <div id="summary" className="page-summary"></div>
        <div className="trade-table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th data-sort-key="pair" data-sort-label="Pair">Pair</th>
                <th data-sort-key="entry_time" data-sort-label="Entry">Entry</th>
                <th data-sort-key="exit_time" data-sort-label="Exit">Exit</th>
                <th data-sort-key="direction" data-sort-label="Direction">Direction</th>
                <th data-sort-key="entry_price" data-sort-label="Entry / Exit">Entry / Exit</th>
                <th data-sort-key="pnl_r" data-sort-label="P/L R">P/L R</th>
                <th data-sort-key="balance_after" data-sort-label="Balance">Balance</th>
                <th data-sort-key="exit_reason" data-sort-label="Reason">Reason</th>
              </tr>
            </thead>
            <tbody id="trades-body">
              <tr><td colSpan={8} className="empty">Loading...</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
