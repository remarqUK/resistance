"""Single source of truth for all backtest and strategy configuration.

To try different scenarios:
  1. Edit an existing profile or add a new one below
  2. Run:  python run.py backtest --profile <name> --days 365 --balance 1000 --risk-pct 5

All tunable parameters are defined here. No need to edit any other file.
"""

# ============================================================================
#  TRADEABLE PAIRS
# ============================================================================
# `ticker` is the cache/data-source key (IBKR / Yahoo).
# `pip` sets the minimum price increment.
# `decimals` controls display precision.

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


# ============================================================================
#  BLOCKED PAIR + DIRECTION COMBOS
# ============================================================================
# Pair/direction combos with historically poor win rates.
# Remove entries here to unblock them for all profiles, or override
# per-profile with use_pair_direction_filter = False.

BLOCKED_PAIR_DIRECTIONS = {
    # 0% WR — never win
    ('USDCAD', 'LONG'),    # 0% WR
    ('NZDUSD', 'LONG'),    # 0% WR
    ('NZDUSD', 'SHORT'),   # 0% WR
    ('GBPJPY', 'LONG'),    # 0% WR
    ('GBPJPY', 'SHORT'),   # 0% WR
    ('AUDJPY', 'LONG'),    # 0% WR
    ('AUDJPY', 'SHORT'),   # 0% WR
    ('EURCHF', 'LONG'),    # 0% WR
    ('EURCHF', 'SHORT'),   # 0% WR
    # <15% WR
    ('GBPUSD', 'LONG'),    # 13.3% WR
}


# ============================================================================
#  CORRELATION GROUPS
# ============================================================================
# Pairs sharing a currency tend to move together.
# Used by the correlation filter to cap simultaneous exposure.

CORRELATION_GROUPS = {
    'USD': ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCHF', 'USDCAD', 'USDJPY'],
    'JPY': ['USDJPY', 'EURJPY', 'GBPJPY', 'AUDJPY', 'CADJPY', 'CHFJPY', 'NZDJPY'],
    'EUR': ['EURUSD', 'EURGBP', 'EURJPY', 'EURAUD', 'EURCAD', 'EURCHF'],
    'GBP': ['GBPUSD', 'EURGBP', 'GBPJPY', 'GBPAUD', 'GBPCAD', 'GBPCHF'],
    'AUD': ['AUDUSD', 'AUDJPY', 'EURAUD', 'GBPAUD', 'AUDNZD', 'AUDCAD'],
    'CAD': ['USDCAD', 'CADJPY', 'EURCAD', 'GBPCAD', 'AUDCAD'],
    'CHF': ['USDCHF', 'CHFJPY', 'EURCHF', 'GBPCHF'],
    'NZD': ['NZDUSD', 'NZDJPY', 'AUDNZD'],
}


# ============================================================================
#  STRATEGY PROFILES
# ============================================================================
# Each profile is a COMPLETE set of every tunable parameter.
# To create a new scenario, copy an existing profile and change values.
#
# Parameter reference:
#
#   Zone detection (daily chart)
#   ----------------------------
#   pivot_window             Bars left/right for pivot high/low detection
#   cluster_tolerance        % tolerance for clustering nearby pivots into zones
#   major_touches            Minimum bounces for a zone to be classified "major"
#   max_zone_width_pct       Maximum zone width as % of price (filters wide zones)
#   zone_history_days        Days of daily data used to detect zones
#
#   Entry rules (1H chart)
#   ----------------------
#   min_zone_touches         Minimum zone touches required before taking a trade
#   zone_penetration_pct     How far price must enter the zone (0.5 = halfway)
#   min_entry_candle_body_pct  Minimum body/range ratio on entry candle (filters dojis)
#   momentum_lookback        Bars to check for strong momentum approaching zone
#   momentum_threshold       Body/range ratio threshold for "strong momentum" candle
#   max_linger_bars          Skip entry if price overlapped zone this many bars recently (0=off)
#   linger_lookback          How many bars back to check for lingering (default 8)
#   zone_exhaustion_threshold  Skip if zone had this many distinct visits recently (0=off)
#   zone_exhaustion_lookback   1H bars to look back for recent zone visits (default 50)
#   blocked_hours            UTC hours where entries are blocked (poor historical WR)
#   blocked_days             Weekdays where entries are blocked (0=Mon, 4=Fri)
#
#   Exit rules
#   ----------
#   rr_ratio                 Risk-to-reward target (1.3 = TP at 1.3x the risk distance)
#   sl_buffer_pct            % buffer beyond zone edge for stop loss placement
#   early_exit_r             Close losers when adverse R-multiple reaches this threshold
#   max_hold_bars            Max bars to hold without TP (72 = ~3 days)
#   sideways_bars            Bars of no progress before sideways exit (15 = ~15 hours)
#   sideways_threshold       Must move this fraction toward TP or it's "sideways"
#   friday_tp_pct            Close winners on Friday if progress >= this % toward TP
#
#   Position management
#   -------------------
#   cooldown_bars            Minimum bars between consecutive entries on same pair
#   max_correlated_trades    Cap on simultaneous trades across correlated pairs
#   use_correlation_filter   Enable/disable correlation-based position cap
#   correlation_prefer_quality  When corr cap is hit, swap in higher-quality trade (default False)
#
#   Execution model
#   ---------------
#   spread_pips              Assumed bid/ask spread in pips (applied to midpoint bars)
#   stop_slippage_pips       Extra adverse slippage on stop loss fills
#
#   Filters
#   -------
#   use_time_filters         Block entries during blocked_hours / blocked_days
#   use_pair_direction_filter  Block entries for pair+direction combos in BLOCKED_PAIR_DIRECTIONS
#
#   Quality-based sizing
#   --------------------
#   quality_sizing           Enable quality-based risk scaling (default False)
#   quality_risk_min         Risk multiplier for lowest quality signal (default 0.5x)
#   quality_risk_max         Risk multiplier for highest quality signal (default 1.5x)
#
#   Backtest settings
#   -----------------
#   hourly_days              Days of 1H data for walk-forward execution
#   starting_balance         Starting account balance for compounding P&L
#   risk_pct                 Risk per trade as % of current balance

PROFILES = {
    'optimized': {
        'description': 'Best risk-adjusted: 99 trades, +337% return, 17.8% max DD at 5% risk',

        # Zone detection
        'pivot_window': 5,
        'cluster_tolerance': 0.08,
        'major_touches': 3,
        'max_zone_width_pct': 0.35,
        'zone_history_days': 180,

        # Entry rules
        'min_zone_touches': 3,
        'zone_penetration_pct': 0.55,
        'min_entry_candle_body_pct': 0.15,
        'momentum_lookback': 2,
        'momentum_threshold': 0.8,
        'blocked_hours': [2, 3],
        'blocked_days': [],

        # Exit rules
        'rr_ratio': 1.3,
        'sl_buffer_pct': 0.15,
        'early_exit_r': 0.35,
        'max_hold_bars': 72,
        'sideways_bars': 15,
        'sideways_threshold': 0.3,
        'friday_tp_pct': 0.60,

        # Position management
        'cooldown_bars': 1,
        'max_correlated_trades': 5,
        'use_correlation_filter': True,

        # Execution model
        'spread_pips': 0.6,
        'stop_slippage_pips': 0.2,

        # Filters
        'use_time_filters': True,
        'use_pair_direction_filter': True,

        # Backtest settings
        'hourly_days': 365,
        'starting_balance': 1000.0,
        'risk_pct': 5.0,
    },

    'source': {
        'description': 'Source-like 1:1 profile with tighter correlation cap',

        # Zone detection
        'pivot_window': 5,
        'cluster_tolerance': 0.08,
        'major_touches': 3,
        'max_zone_width_pct': 0.35,
        'zone_history_days': 180,

        # Entry rules
        'min_zone_touches': 3,
        'zone_penetration_pct': 0.50,
        'min_entry_candle_body_pct': 0.15,
        'momentum_lookback': 2,
        'momentum_threshold': 0.7,
        'blocked_hours': [2, 3],
        'blocked_days': [],

        # Exit rules
        'rr_ratio': 1.0,
        'sl_buffer_pct': 0.15,
        'early_exit_r': 0.4,
        'max_hold_bars': 72,
        'sideways_bars': 15,
        'sideways_threshold': 0.3,
        'friday_tp_pct': 0.70,

        # Position management
        'cooldown_bars': 2,
        'max_correlated_trades': 3,
        'use_correlation_filter': True,

        # Execution model
        'spread_pips': 0.6,
        'stop_slippage_pips': 0.2,

        # Filters
        'use_time_filters': True,
        'use_pair_direction_filter': True,

        # Backtest settings
        'hourly_days': 365,
        'starting_balance': 1000.0,
        'risk_pct': 5.0,
    },

    'balanced': {
        'description': 'Conservative profile with default filters',

        # Zone detection
        'pivot_window': 5,
        'cluster_tolerance': 0.08,
        'major_touches': 3,
        'max_zone_width_pct': 0.35,
        'zone_history_days': 180,

        # Entry rules
        'min_zone_touches': 3,
        'zone_penetration_pct': 0.50,
        'min_entry_candle_body_pct': 0.15,
        'momentum_lookback': 2,
        'momentum_threshold': 0.7,
        'blocked_hours': [2, 3],
        'blocked_days': [],

        # Exit rules
        'rr_ratio': 1.2,
        'sl_buffer_pct': 0.15,
        'early_exit_r': 0.5,
        'max_hold_bars': 72,
        'sideways_bars': 15,
        'sideways_threshold': 0.3,
        'friday_tp_pct': 0.70,

        # Position management
        'cooldown_bars': 2,
        'max_correlated_trades': 4,
        'use_correlation_filter': True,

        # Execution model
        'spread_pips': 0.6,
        'stop_slippage_pips': 0.2,

        # Filters
        'use_time_filters': True,
        'use_pair_direction_filter': True,

        # Backtest settings
        'hourly_days': 365,
        'starting_balance': 1000.0,
        'risk_pct': 5.0,
    },

    'high_volume': {
        'description': '937 trades, +5460142% return, 20.0% DD, max streak 8, 46% WR',

        # Zone detection
        'pivot_window': 5,
        'cluster_tolerance': 0.08,
        'major_touches': 3,
        'max_zone_width_pct': 0.35,
        'zone_history_days': 180,

        # Entry rules — relaxed for higher trade volume
        'min_zone_touches': 3,
        'zone_penetration_pct': 0.38,           # was 0.42 — more zone entries
        'min_entry_candle_body_pct': 0.05,       # accepts weaker reversal candles
        'momentum_lookback': 2,
        'momentum_threshold': 0.6,              # fewer momentum rejections
        'blocked_hours': [2, 3],
        'blocked_days': [],

        # Exit rules — Nick-aligned: 1.1R target, cut losers at 0.4R
        'rr_ratio': 1.1,                       # closer to Nick's 1:1 approach
        'sl_buffer_pct': 0.15,
        'early_exit_r': 0.40,                  # let trades breathe (Nick cuts at 1/3-1/2)
        'max_hold_bars': 72,
        'sideways_bars': 15,                    # was 20 — exit sooner on sideways
        'sideways_threshold': 0.3,
        'friday_tp_pct': 0.60,

        # Position management
        'cooldown_bars': 1,
        'max_correlated_trades': 5,
        'use_correlation_filter': True,

        # Execution model
        'spread_pips': 0.6,
        'stop_slippage_pips': 0.2,

        # Filters
        'use_time_filters': True,
        'use_pair_direction_filter': True,

        # Backtest settings
        'hourly_days': 365,
        'starting_balance': 1000.0,
        'risk_pct': 8.0,

        # Dynamic risk sizing: scale risk down during drawdowns
        'dynamic_risk': True,
        'dd_risk_start': 5.0,            # start reducing risk at 5% DD
        'dd_risk_full': 18.0,            # risk hits floor at 18% DD
        'dd_risk_floor': 0.5,            # minimum 0.5% risk during deep drawdown
    },

    'aggressive': {
        'description': 'Higher-return profile with looser entry filters',

        # Zone detection
        'pivot_window': 5,
        'cluster_tolerance': 0.08,
        'major_touches': 3,
        'max_zone_width_pct': 0.35,
        'zone_history_days': 180,

        # Entry rules
        'min_zone_touches': 3,
        'zone_penetration_pct': 0.50,
        'min_entry_candle_body_pct': 0.10,
        'momentum_lookback': 2,
        'momentum_threshold': 0.7,
        'blocked_hours': [2, 3],
        'blocked_days': [],

        # Exit rules
        'rr_ratio': 1.2,
        'sl_buffer_pct': 0.10,
        'early_exit_r': 0.5,
        'max_hold_bars': 72,
        'sideways_bars': 15,
        'sideways_threshold': 0.3,
        'friday_tp_pct': 0.70,

        # Position management
        'cooldown_bars': 2,
        'max_correlated_trades': 4,
        'use_correlation_filter': True,

        # Execution model
        'spread_pips': 0.6,
        'stop_slippage_pips': 0.2,

        # Filters
        'use_time_filters': True,
        'use_pair_direction_filter': True,

        # Backtest settings
        'hourly_days': 365,
        'starting_balance': 1000.0,
        'risk_pct': 5.0,
    },
}

DEFAULT_PROFILE = 'high_volume'


# ============================================================================
#  HELPERS  (used by the rest of the codebase — not meant for editing)
# ============================================================================

def get_profile(name: str | None = None) -> dict:
    """Return a complete profile dict by name. Raises KeyError if unknown."""
    name = name or DEFAULT_PROFILE
    return PROFILES[name]


def list_profiles() -> dict[str, str]:
    """Return {name: description} for all available profiles."""
    return {name: p['description'] for name, p in PROFILES.items()}
