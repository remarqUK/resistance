import React, { useEffect } from 'react';
import '../styles/replay.css';

export function ReplayPage() {
  useEffect(() => {
    if (window.__fxReplayScriptLoaded) {
      return;
    }
    const script = document.createElement('script');
    script.src = '/static/replay.js';
    script.async = false;
    script.onload = () => {
      window.__fxReplayScriptLoaded = true;
    };
    document.body.appendChild(script);
  }, []);

  return (
    <div className="shell">
      <div className="hero">
        <div className="hero-title-row">
          <div>
            <span className="eyebrow">FX support / resistance</span>
            <h1>Strategy Replay</h1>
          </div>
          <div className="hero-actions hero-actions-vertical hero-links-column">
            <a href="/" className="hero-action">Back to Live Board</a>
            <a href="/trade-log" className="hero-action">Trade Log</a>
            <a href="/backtest-trades" className="hero-action">All Backtest Trades</a>
            <a href="/backtest-diary" className="hero-action">Trade Diary</a>
          </div>
        </div>
      </div>

      <div className="panel replay-panel">
        <div className="replay-controls">
          <div className="field">
            <label htmlFor="pair-select">Pair</label>
            <select id="pair-select"></select>
          </div>
          <div className="field">
            <label htmlFor="date-input">Date</label>
            <input type="date" id="date-input" />
          </div>
          <div className="field">
            <label htmlFor="preset-select">Profile</label>
            <select id="preset-select">
              <option value="high_volume">high_volume</option>
            </select>
          </div>
          <div className="field">
            <label htmlFor="tf-select">Timeframe</label>
            <select id="tf-select">
              <option value="1m">1 min</option>
              <option value="1h">1 hour</option>
            </select>
          </div>
          <button className="btn btn-primary" id="load-btn" type="button">Load</button>
          <button className="btn btn-primary btn-info" id="refresh-btn" type="button">Update Data</button>
        </div>

        <div className="error-banner" id="error-banner"></div>
        <div id="tf-fallback-banner" className="info-banner info-banner-info">
          No minute data cached for this pair. Showing hourly bars. Click &quot;Update Data&quot; to fetch minute data from IBKR.
        </div>
        <div id="incomplete-banner" className="info-banner info-banner-warning">
          Trading day in progress — data is incomplete and will update on next load.
        </div>

        <div id="chart-container"></div>

        <div id="playback-row" className="playback-row">
          <button className="btn-icon" id="prev-trade-btn" title="Previous trade" aria-label="Previous trade" type="button">&larr;</button>
          <div className="bar-label" id="trade-nav-label">No trade selected</div>
          <button className="btn-icon" id="next-trade-btn" title="Next trade" aria-label="Next trade" type="button">&rarr;</button>
        </div>
      </div>

      <div className="info-grid" id="info-grid">
        <div className="info-card">
          <h3>Simulated Trades</h3>
          <div className="trade-list" id="trade-list"></div>
        </div>
        <div className="info-card" id="trade-info">
          <h3>Trade State</h3>
          <div id="trade-details"></div>
        </div>
        <div className="info-card" id="summary-info">
          <h3>Day Summary</h3>
          <div id="summary-details"></div>
        </div>
        <div className="info-card" id="bar-info">
          <h3>Current Bar</h3>
          <div id="bar-details"></div>
        </div>
      </div>
    </div>
  );
}
