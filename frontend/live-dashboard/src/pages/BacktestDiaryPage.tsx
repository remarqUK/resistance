import React, { useEffect } from 'react';
import { NavLinks } from '../components/NavLinks';

export function BacktestDiaryPage() {
  useEffect(() => {
    if (window.__fxBacktestDiaryScriptLoaded) return;
    const sharedScript = document.createElement('script');
    sharedScript.src = '/static/diary_shared.js';
    sharedScript.async = false;
    sharedScript.onload = () => {
      const pageScript = document.createElement('script');
      pageScript.src = '/static/backtest_diary.js';
      pageScript.async = false;
      pageScript.onload = () => { window.__fxBacktestDiaryScriptLoaded = true; };
      document.body.appendChild(pageScript);
    };
    document.body.appendChild(sharedScript);
  }, []);

  return (
    <div className="shell">
      <div className="hero">
        <div className="hero-title-row">
          <div>
            <span className="eyebrow">FX support / resistance</span>
            <h1>Backtest Diary</h1>
          </div>
          <NavLinks current="/backtest-diary" />
        </div>
        <p className="subtitle">Calendar view of daily backtest P/L, then drill into a day to inspect trades.</p>
      </div>

      <section className="panel">
        <div className="filter-toolbar">
          <div className="field">
            <label htmlFor="backtest-filter">Backtest</label>
            <select id="backtest-filter">
              <option value="">Loading runs...</option>
            </select>
          </div>
          <button id="load-btn" className="toolbar-btn" type="button">Load Diary</button>
          <div className="toolbar-meta" id="month-range"></div>
          <div className="toolbar-meta" id="summary"></div>
        </div>

        <div style={{ display: 'flex', gap: '1rem', alignItems: 'flex-start' }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div className="diary-calendar" id="diary-calendar">
              <div className="empty-card empty">Load diary data to render calendar.</div>
            </div>
          </div>

          <div style={{ width: 320, flexShrink: 0 }}>
            <div className="diary-selected-date" id="selected-date" style={{ marginBottom: '8px' }}>Select a day to view trades.</div>
            <div id="diary-body" style={{ fontSize: '0.84rem' }}>
              <div style={{ color: 'var(--muted)', padding: '8px 0' }}>Select a date.</div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
