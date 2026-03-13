"""Configuration and shared defaults.

All master values now live in fx_sr/profiles.py.
This module re-exports them for backward compatibility so existing imports
like ``from .config import PAIRS`` keep working.
"""

from .profiles import PAIRS, PROFILES, DEFAULT_PROFILE, get_profile

# ---------------------------------------------------------------------------
# Zone detection defaults (re-derived from the default profile)
# ---------------------------------------------------------------------------
_DEFAULT = get_profile()

DEFAULT_PIVOT_WINDOW = _DEFAULT['pivot_window']
DEFAULT_CLUSTER_TOL = _DEFAULT['cluster_tolerance']
DEFAULT_MAJOR_TOUCHES = _DEFAULT['major_touches']
DEFAULT_ZONE_HISTORY_DAYS = _DEFAULT['zone_history_days']
DEFAULT_MAX_ZONE_WIDTH_PCT = _DEFAULT['max_zone_width_pct']

# ---------------------------------------------------------------------------
# Strategy defaults (re-derived from the default profile)
# ---------------------------------------------------------------------------
DEFAULT_RR_RATIO = _DEFAULT['rr_ratio']
DEFAULT_SL_BUFFER_PCT = _DEFAULT['sl_buffer_pct']
DEFAULT_EARLY_EXIT_R = _DEFAULT['early_exit_r']
DEFAULT_COOLDOWN_BARS = _DEFAULT['cooldown_bars']
DEFAULT_MIN_ENTRY_CANDLE_BODY_PCT = _DEFAULT['min_entry_candle_body_pct']
DEFAULT_MOMENTUM_LOOKBACK = _DEFAULT['momentum_lookback']
DEFAULT_MAX_CORRELATED_TRADES = _DEFAULT['max_correlated_trades']
DEFAULT_EXECUTION_SPREAD_PIPS = _DEFAULT['spread_pips']
DEFAULT_STOP_SLIPPAGE_PIPS = _DEFAULT['stop_slippage_pips']

# ---------------------------------------------------------------------------
# Presets (built from profiles for backward compat with run.py / param_sweep)
# ---------------------------------------------------------------------------
STRATEGY_PRESETS = {
    name: {
        'rr_ratio': p['rr_ratio'],
        'sl_buffer_pct': p['sl_buffer_pct'],
        'early_exit_r': p['early_exit_r'],
        'cooldown_bars': p['cooldown_bars'],
        'min_entry_candle_body_pct': p['min_entry_candle_body_pct'],
        'momentum_lookback': p['momentum_lookback'],
        'max_correlated_trades': p['max_correlated_trades'],
        'zone_penetration_pct': p.get('zone_penetration_pct', 0.50),
        'momentum_threshold': p.get('momentum_threshold', 0.7),
        'friday_tp_pct': p.get('friday_tp_pct', 0.70),
    }
    for name, p in PROFILES.items()
}

DEFAULT_STRATEGY_PRESET = DEFAULT_PROFILE

STRATEGY_PRESET_DESCRIPTIONS = {
    name: p['description'] for name, p in PROFILES.items()
}
