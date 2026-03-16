"""Tests for the HourlyBarAccumulator."""

import unittest
from types import SimpleNamespace

import pandas as pd

from fx_sr.bar_accumulator import HourlyBarAccumulator, _hour_start


def _bar(time, open_, high, low, close, volume=0):
    return SimpleNamespace(
        time=time, open_=open_, high=high, low=low, close=close, volume=volume,
    )


class HourStartTests(unittest.TestCase):

    def test_rounds_down_to_hour(self):
        ts = pd.Timestamp('2026-03-10 14:23:45', tz='UTC')
        self.assertEqual(_hour_start(ts), pd.Timestamp('2026-03-10 14:00:00', tz='UTC'))

    def test_exact_hour_unchanged(self):
        ts = pd.Timestamp('2026-03-10 14:00:00', tz='UTC')
        self.assertEqual(_hour_start(ts), ts)


class AccumulatorTests(unittest.TestCase):

    def test_seed_initializes_pair(self):
        acc = HourlyBarAccumulator()
        df = pd.DataFrame(
            {'Open': [1.1], 'High': [1.11], 'Low': [1.09], 'Close': [1.105], 'Volume': [100]},
            index=pd.DatetimeIndex([pd.Timestamp('2026-03-10 13:00', tz='UTC')]),
        )
        acc.seed('EURUSD', df)
        self.assertIn('EURUSD', acc.seeded_pairs)
        result = acc.get_hourly_df('EURUSD')
        self.assertEqual(len(result), 1)

    def test_realtime_bars_accumulate_into_hourly(self):
        acc = HourlyBarAccumulator()
        acc.seed('EURUSD', pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume']))

        # Two bars in the same hour
        acc.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 14:00:05', tz='UTC'), 1.10, 1.11, 1.09, 1.105,
        ))
        acc.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 14:00:10', tz='UTC'), 1.105, 1.115, 1.095, 1.11,
        ))

        df = acc.get_hourly_df('EURUSD')
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.iloc[0]['Open'], 1.10)
        self.assertAlmostEqual(df.iloc[0]['High'], 1.115)
        self.assertAlmostEqual(df.iloc[0]['Low'], 1.09)
        self.assertAlmostEqual(df.iloc[0]['Close'], 1.11)

    def test_hour_boundary_finalizes_bar_and_fires_callback(self):
        acc = HourlyBarAccumulator()
        acc.seed('EURUSD', pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume']))

        completed = []
        acc.on_bar_complete(lambda pair, ts: completed.append((pair, ts)))

        # Bar in hour 14
        acc.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 14:59:55', tz='UTC'), 1.10, 1.11, 1.09, 1.105,
        ))
        # Bar in hour 15 — triggers hour-14 finalization
        acc.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 15:00:05', tz='UTC'), 1.105, 1.12, 1.10, 1.115,
        ))

        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0][0], 'EURUSD')
        self.assertEqual(completed[0][1], pd.Timestamp('2026-03-10 14:00', tz='UTC'))

        df = acc.get_hourly_df('EURUSD')
        # 1 completed + 1 in-progress
        self.assertEqual(len(df), 2)

    def test_callback_failures_are_logged(self):
        acc = HourlyBarAccumulator()
        acc.seed('EURUSD', pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume']))
        acc.on_bar_complete(lambda *_args: (_ for _ in ()).throw(RuntimeError('boom')))

        with self.assertLogs('fx_sr.bar_accumulator', level='ERROR') as logs:
            acc.on_realtime_bar('EURUSD', _bar(
                pd.Timestamp('2026-03-10 14:59:55', tz='UTC'), 1.10, 1.11, 1.09, 1.105,
            ))
            acc.on_realtime_bar('EURUSD', _bar(
                pd.Timestamp('2026-03-10 15:00:05', tz='UTC'), 1.105, 1.12, 1.10, 1.115,
            ))

        self.assertTrue(any('Hourly bar completion callback failed for EURUSD' in line for line in logs.output))

    def test_get_completed_df_excludes_in_progress_bar(self):
        acc = HourlyBarAccumulator()
        acc.seed('EURUSD', pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume']))

        acc.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 14:59:55', tz='UTC'), 1.10, 1.11, 1.09, 1.105,
        ))
        acc.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 15:00:05', tz='UTC'), 1.20, 1.21, 1.19, 1.205,
        ))

        completed = acc.get_completed_df('EURUSD')
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed.index[0], pd.Timestamp('2026-03-10 14:00', tz='UTC'))
        self.assertAlmostEqual(completed.iloc[0]['Close'], 1.105)

        full = acc.get_hourly_df('EURUSD')
        self.assertEqual(len(full), 2)
        self.assertEqual(full.index[-1], pd.Timestamp('2026-03-10 15:00', tz='UTC'))

    def test_get_hourly_df_includes_seeded_and_current(self):
        acc = HourlyBarAccumulator()
        seeded = pd.DataFrame(
            {'Open': [1.1], 'High': [1.11], 'Low': [1.09], 'Close': [1.105], 'Volume': [100]},
            index=pd.DatetimeIndex([pd.Timestamp('2026-03-10 13:00', tz='UTC')]),
        )
        acc.seed('EURUSD', seeded)
        acc.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 14:00:05', tz='UTC'), 1.10, 1.11, 1.09, 1.105,
        ))

        df = acc.get_hourly_df('EURUSD')
        self.assertEqual(len(df), 2)
        # Seeded bar at 13:00, in-progress at 14:00
        self.assertEqual(df.index[0], pd.Timestamp('2026-03-10 13:00', tz='UTC'))
        self.assertEqual(df.index[1], pd.Timestamp('2026-03-10 14:00', tz='UTC'))

    def test_get_latest_price(self):
        acc = HourlyBarAccumulator()
        acc.seed('EURUSD', pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume']))
        self.assertIsNone(acc.get_latest_price('EURUSD'))

        acc.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 14:00:05', tz='UTC'), 1.10, 1.11, 1.09, 1.1234,
        ))
        self.assertAlmostEqual(acc.get_latest_price('EURUSD'), 1.1234)

    def test_price_tick_creates_bar(self):
        acc = HourlyBarAccumulator()
        acc.seed('EURUSD', pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume']))
        acc.on_price_tick('EURUSD', 1.1050)
        acc.on_price_tick('EURUSD', 1.1070)
        acc.on_price_tick('EURUSD', 1.1030)

        df = acc.get_hourly_df('EURUSD')
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.iloc[0]['Open'], 1.1050)
        self.assertAlmostEqual(df.iloc[0]['High'], 1.1070)
        self.assertAlmostEqual(df.iloc[0]['Low'], 1.1030)
        self.assertAlmostEqual(df.iloc[0]['Close'], 1.1030)

    def test_tail_n_limits_output(self):
        acc = HourlyBarAccumulator()
        rows = []
        for h in range(100):
            rows.append({
                'Open': 1.1, 'High': 1.11, 'Low': 1.09, 'Close': 1.105, 'Volume': 10,
            })
        index = pd.DatetimeIndex(
            [pd.Timestamp(f'2026-03-{(h // 24) + 1:02d} {h % 24:02d}:00', tz='UTC') for h in range(100)],
        )
        acc.seed('EURUSD', pd.DataFrame(rows, index=index))
        df = acc.get_hourly_df('EURUSD', tail_n=10)
        self.assertEqual(len(df), 10)


if __name__ == '__main__':
    unittest.main()
