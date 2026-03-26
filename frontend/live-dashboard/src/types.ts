export interface SizePlan {
  pair?: string;
  direction?: string;
  units?: number;
  risk_amount?: number;
  risk_pct?: number;
  balance?: number;
  account_currency?: string;
  risk_per_unit_account?: number;
  notional_account?: number;
}

export interface Signal {
  time?: string;
  pair: string;
  direction: string;
  entry_price?: number;
  sl_price?: number;
  tp_price?: number;
  zone_upper?: number;
  zone_lower?: number;
  zone_strength?: string;
  zone_type?: string;
  decimals?: number;
  arrived_at?: string | null;
  last_valid_at?: string | null;
  size_plan?: SizePlan | null;
}

export interface PairRow {
  pair: string;
  name?: string;
  decimals?: number;
  price?: number | null;
  state: string;
  note?: string;
  support_text?: string;
  resistance_text?: string;
  support_lower?: number | null;
  support_upper?: number | null;
  support_strength?: string | null;
  resistance_lower?: number | null;
  resistance_upper?: number | null;
  resistance_strength?: string | null;
  support_dist_pct?: number | null;
  resistance_dist_pct?: number | null;
  signal?: Signal | null;
}

export interface PositionRow {
  pair: string;
  direction: string;
  size?: number;
  entry_price?: number;
  current_price?: number;
  pnl_pips?: number;
  status?: string;
  decimals?: number;
}

export interface AlertRow {
  pair: string;
  direction: string;
  exit_reason: string;
  current_price?: number;
  pnl_pips?: number;
  decimals?: number;
}

export interface ExecutionRow {
  pair: string;
  direction: string;
  units?: number;
  status: string;
  time?: string;
  order_id?: string | number | null;
  note?: string;
  submitted_entry_price?: number | null;
  submitted_sl_price?: number | null;
  submitted_tp_price?: number | null;
  pnl_pips?: number | null;
  pnl_r?: number | null;
  closed_price?: number | null;
  closed_at?: string | null;
  close_reason?: string | null;
}

export interface LogEntry {
  ts?: string;
  level?: string;
  message?: string;
}

export interface SummaryState {
  status?: string;
  pairs_total?: number;
  pairs_completed?: number;
  signal_count?: number;
  pending_count?: number;
  position_count?: number;
  execution_enabled?: boolean;
  execution_available?: boolean;
  execution_paused?: boolean;
  execution_mode?: string;
  execution_mode_label?: string;
  strategy_label?: string;
  mode?: string;
  url?: string;
  balance?: number | null;
  account_currency?: string | null;
  risk_pct?: number | null;
  backfill?: Record<string, any>;
  fill?: Record<string, any>;
  backtest?: Record<string, any>;
}

export interface DashboardState {
  summary: SummaryState;
  pairs: Record<string, PairRow>;
  signals: Signal[];
  positions: PositionRow[];
  alerts: AlertRow[];
  executions: ExecutionRow[];
  log: LogEntry[];
}

export interface TradeLogResponse {
  signals: Array<Record<string, any>>;
  pairs: string[];
  count: number;
}
