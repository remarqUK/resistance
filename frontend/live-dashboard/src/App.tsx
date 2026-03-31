import React from 'react';
import { Route, Routes } from 'react-router-dom';
import { BacktestDiaryPage } from './pages/BacktestDiaryPage';
import { BacktestTradesPage } from './pages/BacktestTradesPage';
import { DashboardPage } from './pages/DashboardPage';
import { LiveDiaryPage } from './pages/LiveDiaryPage';
import { LiveTradePage } from './pages/LiveTradePage';
import { ReplayPage } from './pages/ReplayPage';
import { TradeLogPage } from './pages/TradeLogPage';
import { OrderAuditLogPage } from './pages/OrderAuditLogPage';
import { PositionHealthPage } from './pages/PositionHealthPage';

export function App() {
  return (
    <Routes>
      <Route path="/" element={<DashboardPage />} />
      <Route path="/trade-log" element={<TradeLogPage />} />
      <Route path="/order-audit-log" element={<OrderAuditLogPage />} />
      <Route path="/position-health" element={<PositionHealthPage />} />
      <Route path="/replay" element={<ReplayPage />} />
      <Route path="/live-diary" element={<LiveDiaryPage />} />
      <Route path="/live-trade" element={<LiveTradePage />} />
      <Route path="/backtest-trades" element={<BacktestTradesPage />} />
      <Route path="/backtest-diary" element={<BacktestDiaryPage />} />
      <Route path="*" element={<DashboardPage />} />
    </Routes>
  );
}
