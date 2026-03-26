import React, { useEffect } from 'react';
import { NavLinks } from '../components/NavLinks';
import '../styles/live-trade.css';

export function LiveTradePage() {
  useEffect(() => {
    if (window.__fxLiveTradeScriptLoaded) {
      return;
    }
    const script = document.createElement('script');
    script.src = '/static/live_trade.js';
    script.async = false;
    script.onload = () => {
      window.__fxLiveTradeScriptLoaded = true;
    };
    document.body.appendChild(script);
  }, []);

  return (
    <div className="shell">
      <div className="hero">
        <div className="hero-title-row">
          <div>
            <span className="eyebrow">FX support / resistance</span>
            <h1>Live Trade Review</h1>
          </div>
          <NavLinks current="/live-trade" />
        </div>
      </div>

      <div style={{display:'flex',gap:'14px',alignItems:'flex-start',marginBottom:'18px'}}>
        <div className="panel" style={{flex:1,minWidth:0,marginBottom:0}}>
          <div className="error-banner" id="error-banner"></div>
          <div id="chart-container"></div>
        </div>

        <div id="other-trades-sidebar" className="info-card" style={{width:'280px',flexShrink:0,maxHeight:'620px',overflowY:'auto',display:'none'}}>
          <h3>Other Trades This Day</h3>
          <div className="trade-list" id="other-trades-list" style={{maxHeight:'none'}}></div>
        </div>
      </div>

      <div className="info-grid" id="info-grid">
        <div className="info-card">
          <h3>Trade Details</h3>
          <div id="trade-details"></div>
        </div>
        <div className="info-card">
          <h3>Execution</h3>
          <div id="execution-details"></div>
        </div>
        <div className="info-card">
          <h3>Outcome</h3>
          <div id="outcome-details"></div>
        </div>
      </div>
    </div>
  );
}
