"""Average True Range (ATR) computation for volatility-adapted stop losses."""

import numpy as np
import pandas as pd


def compute_atr(hourly_df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Compute Average True Range from hourly OHLC data.

    Uses the standard Wilder smoothing (exponential moving average with
    alpha = 1/period) over True Range values.

    Returns a Series indexed same as hourly_df.  The first ``period`` rows
    will be NaN because there is not enough history yet.
    """
    high = hourly_df['High'].astype(float)
    low = hourly_df['Low'].astype(float)
    prev_close = hourly_df['Close'].astype(float).shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Wilder smoothing: EMA with alpha = 1/period
    atr = tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    return atr
