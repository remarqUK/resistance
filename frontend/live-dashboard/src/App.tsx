import React from 'react';
import { Route, Routes } from 'react-router-dom';
import { DashboardPage } from './pages/DashboardPage';
import { ReplayPage } from './pages/ReplayPage';
import { TradeLogPage } from './pages/TradeLogPage';

export function App() {
  return (
    <Routes>
      <Route path="/" element={<DashboardPage />} />
      <Route path="/trade-log" element={<TradeLogPage />} />
      <Route path="/replay" element={<ReplayPage />} />
      <Route path="*" element={<DashboardPage />} />
    </Routes>
  );
}
