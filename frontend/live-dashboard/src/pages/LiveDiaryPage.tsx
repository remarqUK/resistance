import React, { useEffect } from 'react';
import { NavLinks } from '../components/NavLinks';

export function LiveDiaryPage() {
  useEffect(() => {
    if (window.__fxLiveDiaryScriptLoaded) {
      return;
    }

    const sharedScript = document.createElement('script');
    sharedScript.src = '/static/diary_shared.js';
    sharedScript.async = false;
    sharedScript.onload = () => {
      const pageScript = document.createElement('script');
      pageScript.src = '/static/live_diary.js';
      pageScript.async = false;
      pageScript.onload = () => {
        window.__fxLiveDiaryScriptLoaded = true;
      };
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
            <h1>Live Diary</h1>
          </div>
          <NavLinks current="/live-diary" />
        </div>
        <p className="subtitle">Calendar view of daily live P/L from executed trades.</p>
      </div>

      <section className="panel">
        <div className="filter-toolbar">
          <button id="load-btn" className="toolbar-btn" type="button">Load Diary</button>
          <div className="toolbar-meta" id="month-range"></div>
          <div className="toolbar-meta" id="summary"></div>
        </div>

        <div style={{ display: 'flex', gap: '1rem', alignItems: 'flex-start' }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div className="diary-calendar" id="diary-calendar">
              <div className="empty-card empty">Loading live trade diary...</div>
            </div>
          </div>

          <div style={{ width: 420, flexShrink: 0 }}>
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
