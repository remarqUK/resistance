"""Configuration and shared defaults."""

# Top 10 most traded currency pairs.
# `ticker` is the internal cache/data-source key retained for compatibility
# with the existing SQLite dataset and IBKR adapter.
PAIRS = {
    # Majors
    'EURUSD': {'ticker': 'EURUSD=X', 'pip': 0.0001, 'name': 'EUR/USD', 'decimals': 5},
    'USDJPY': {'ticker': 'JPY=X',    'pip': 0.01,   'name': 'USD/JPY', 'decimals': 3},
    'GBPUSD': {'ticker': 'GBPUSD=X', 'pip': 0.0001, 'name': 'GBP/USD', 'decimals': 5},
    'USDCHF': {'ticker': 'CHF=X',    'pip': 0.0001, 'name': 'USD/CHF', 'decimals': 5},
    'AUDUSD': {'ticker': 'AUDUSD=X', 'pip': 0.0001, 'name': 'AUD/USD', 'decimals': 5},
    'USDCAD': {'ticker': 'CAD=X',    'pip': 0.0001, 'name': 'USD/CAD', 'decimals': 5},
    'NZDUSD': {'ticker': 'NZDUSD=X', 'pip': 0.0001, 'name': 'NZD/USD', 'decimals': 5},
    # Major crosses
    'EURGBP': {'ticker': 'EURGBP=X', 'pip': 0.0001, 'name': 'EUR/GBP', 'decimals': 5},
    'EURJPY': {'ticker': 'EURJPY=X', 'pip': 0.01,   'name': 'EUR/JPY', 'decimals': 3},
    'GBPJPY': {'ticker': 'GBPJPY=X', 'pip': 0.01,   'name': 'GBP/JPY', 'decimals': 3},
    # Additional liquid crosses
    'AUDJPY': {'ticker': 'AUDJPY=X', 'pip': 0.01,   'name': 'AUD/JPY', 'decimals': 3},
    'CADJPY': {'ticker': 'CADJPY=X', 'pip': 0.01,   'name': 'CAD/JPY', 'decimals': 3},
    'CHFJPY': {'ticker': 'CHFJPY=X', 'pip': 0.01,   'name': 'CHF/JPY', 'decimals': 3},
    'EURAUD': {'ticker': 'EURAUD=X', 'pip': 0.0001, 'name': 'EUR/AUD', 'decimals': 5},
    'EURCAD': {'ticker': 'EURCAD=X', 'pip': 0.0001, 'name': 'EUR/CAD', 'decimals': 5},
    'EURCHF': {'ticker': 'EURCHF=X', 'pip': 0.0001, 'name': 'EUR/CHF', 'decimals': 5},
    'GBPAUD': {'ticker': 'GBPAUD=X', 'pip': 0.0001, 'name': 'GBP/AUD', 'decimals': 5},
    'GBPCAD': {'ticker': 'GBPCAD=X', 'pip': 0.0001, 'name': 'GBP/CAD', 'decimals': 5},
    'GBPCHF': {'ticker': 'GBPCHF=X', 'pip': 0.0001, 'name': 'GBP/CHF', 'decimals': 5},
    'AUDNZD': {'ticker': 'AUDNZD=X', 'pip': 0.0001, 'name': 'AUD/NZD', 'decimals': 5},
    'NZDJPY': {'ticker': 'NZDJPY=X', 'pip': 0.01,   'name': 'NZD/JPY', 'decimals': 3},
    'AUDCAD': {'ticker': 'AUDCAD=X', 'pip': 0.0001, 'name': 'AUD/CAD', 'decimals': 5},
}

# Zone detection defaults (daily chart)
DEFAULT_PIVOT_WINDOW = 5
DEFAULT_CLUSTER_TOL = 0.08
DEFAULT_MAJOR_TOUCHES = 3
DEFAULT_ZONE_HISTORY_DAYS = 180
DEFAULT_MAX_ZONE_WIDTH_PCT = 0.35

# Shipped default strategy profile and execution model (corrected engine)
DEFAULT_RR_RATIO = 1.2
DEFAULT_SL_BUFFER_PCT = 0.15
DEFAULT_EARLY_EXIT_R = 0.5
DEFAULT_COOLDOWN_BARS = 2
DEFAULT_MIN_ENTRY_CANDLE_BODY_PCT = 0.15
DEFAULT_MOMENTUM_LOOKBACK = 2
DEFAULT_MAX_CORRELATED_TRADES = 4
DEFAULT_EXECUTION_SPREAD_PIPS = 0.6
DEFAULT_STOP_SLIPPAGE_PIPS = 0.2

STRATEGY_PRESETS = {
    'source': {
        'rr_ratio': 1.0,
        'sl_buffer_pct': 0.15,
        'early_exit_r': 0.4,
        'cooldown_bars': 2,
        'min_entry_candle_body_pct': 0.15,
        'momentum_lookback': 2,
        'max_correlated_trades': 3,
    },
    'balanced': {
        'rr_ratio': DEFAULT_RR_RATIO,
        'sl_buffer_pct': DEFAULT_SL_BUFFER_PCT,
        'early_exit_r': DEFAULT_EARLY_EXIT_R,
        'cooldown_bars': DEFAULT_COOLDOWN_BARS,
        'min_entry_candle_body_pct': DEFAULT_MIN_ENTRY_CANDLE_BODY_PCT,
        'momentum_lookback': DEFAULT_MOMENTUM_LOOKBACK,
        'max_correlated_trades': DEFAULT_MAX_CORRELATED_TRADES,
    },
    'aggressive': {
        'rr_ratio': 1.2,
        'sl_buffer_pct': 0.10,
        'early_exit_r': 0.5,
        'cooldown_bars': 2,
        'min_entry_candle_body_pct': 0.10,
        'momentum_lookback': 2,
        'max_correlated_trades': 4,
    },
    'optimized': {
        'rr_ratio': 1.3,
        'sl_buffer_pct': 0.15,
        'early_exit_r': 0.35,
        'cooldown_bars': 1,
        'min_entry_candle_body_pct': 0.15,
        'momentum_lookback': 2,
        'max_correlated_trades': 5,
        'zone_penetration_pct': 0.55,
        'momentum_threshold': 0.8,
        'friday_tp_pct': 0.60,
    },
}

DEFAULT_STRATEGY_PRESET = 'optimized'

STRATEGY_PRESET_DESCRIPTIONS = {
    'source': 'Source-like 1:1 profile with tighter correlation cap',
    'balanced': 'Conservative profile with default filters',
    'aggressive': 'Higher-return profile with looser entry filters',
    'optimized': 'Best risk-adjusted profile: 46 trades, +398% return, 10.6% DD at 7.5% risk',
}
