"""Tests for neutralization position tracking."""

import logging
import os
import pytest
import psycopg

from fx_sr.positions import (
    load_neutralization_positions,
    record_neutralization_position,
    remove_neutralization_position,
)

TEST_DB_URL = os.environ.get(
    'RESISTANCE_DATABASE_URL',
    'postgresql://postgres:Harrison12_!@localhost:5432/resistance',
)


@pytest.fixture(autouse=True)
def _clean_neutralization_table():
    """Wipe neutralization_position rows before each test."""
    conn = psycopg.connect(TEST_DB_URL, autocommit=True)
    try:
        conn.execute("DELETE FROM neutralization_position")
    except Exception:
        pass  # table may not exist yet on first run
    finally:
        conn.close()
    yield
    conn = psycopg.connect(TEST_DB_URL, autocommit=True)
    try:
        conn.execute("DELETE FROM neutralization_position")
    except Exception:
        pass
    finally:
        conn.close()


def test_record_and_load():
    record_neutralization_position('GBPJPY', 'LONG', order_id=10175, exchange='IDEALPRO')
    positions = load_neutralization_positions()
    assert ('GBPJPY', 'LONG') in positions


def test_record_is_idempotent():
    record_neutralization_position('GBPJPY', 'LONG', order_id=100)
    record_neutralization_position('GBPJPY', 'LONG', order_id=200)
    positions = load_neutralization_positions()
    assert ('GBPJPY', 'LONG') in positions
    assert len([k for k in positions if k == ('GBPJPY', 'LONG')]) == 1


def test_remove():
    record_neutralization_position('GBPJPY', 'LONG')
    remove_neutralization_position('GBPJPY', 'LONG')
    positions = load_neutralization_positions()
    assert ('GBPJPY', 'LONG') not in positions


def test_remove_nonexistent_is_safe():
    remove_neutralization_position('NZDUSD', 'SHORT')  # should not raise


def test_load_empty():
    positions = load_neutralization_positions()
    assert isinstance(positions, set)
    assert len(positions) == 0


from fx_sr.ibkr import _neutralization_pair_direction


def test_neutralization_pair_direction_buy_jpy():
    """BUY JPY against GBP -> sells GBP, buys JPY -> GBPJPY SHORT (selling GBP)."""
    pair, direction = _neutralization_pair_direction('JPY', 'GBP', 'SELL', 'GBPJPY')
    assert pair == 'GBPJPY'
    assert direction == 'SHORT'


def test_neutralization_pair_direction_sell_eur():
    """SELL EUR against GBP -> we sell EUR, buy GBP -> EURGBP SHORT."""
    pair, direction = _neutralization_pair_direction('EUR', 'GBP', 'SELL', 'EURGBP')
    assert pair == 'EURGBP'
    assert direction == 'SHORT'


def test_neutralization_pair_direction_buy_usd():
    """BUY USD against GBP via GBPUSD contract with BUY action -> GBPUSD LONG."""
    pair, direction = _neutralization_pair_direction('USD', 'GBP', 'BUY', 'GBPUSD')
    assert pair == 'GBPUSD'
    assert direction == 'LONG'


def test_neutralization_pair_direction_unknown_pair():
    """Unknown pair returns None."""
    result = _neutralization_pair_direction('XYZ', 'GBP', 'BUY', 'XYZGBP')
    assert result is None


def test_fxconv_qualification_filter_suppresses_expected_probe_noise():
    from fx_sr.ibkr import _SuppressFxconvQualificationFilter

    filter_ = _SuppressFxconvQualificationFilter()

    def record(message):
        return logging.LogRecord('ib_async.wrapper', logging.ERROR, '', 0, message, (), None)

    assert not filter_.filter(record(
        "Error 200, reqId 12: The destination or exchange selected is Invalid. "
        "contract: Contract(secType='CASH', symbol='EUR', exchange='FXCONV', currency='GBP')"
    ))
    assert not filter_.filter(record(
        "Unknown contract: Contract(secType='CASH', symbol='EUR', exchange='FXCONV', currency='GBP')"
    ))
    assert filter_.filter(record(
        "Error 200, reqId 99: unrelated contract: Contract(secType='CASH', exchange='IDEALPRO')"
    ))


import types
from unittest.mock import patch, MagicMock
from fx_sr.positions import sync_positions
from fx_sr.strategy import StrategyParams


def _make_ibkr_position(pair, size, avg_cost):
    return {'pair': pair, 'size': size, 'avg_cost': avg_cost}


@patch('fx_sr.positions.set_setting')
@patch('fx_sr.positions._load_trades', return_value={})
@patch('fx_sr.positions.ibkr')
@patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[])
@patch('fx_sr.positions.reconcile_broker_ledger')
def test_sync_skips_neutralization_positions(mock_reconcile, mock_broker_positions, mock_ibkr, mock_load, mock_setting):
    """sync_positions should not create open_trades for neutralization positions."""
    mock_ibkr.fetch_positions.return_value = [
        _make_ibkr_position('GBPJPY', 30000, 213.76),
    ]
    mock_ibkr.fetch_open_order_counts.return_value = {}

    record_neutralization_position('GBPJPY', 'LONG')

    params = StrategyParams()
    result = sync_positions(params=params)

    assert 'GBPJPY:LONG' not in result
    # Must not attempt bracket resubmission for skipped positions.
    mock_ibkr.submit_bracket_for_existing_position.assert_not_called()


@patch('fx_sr.positions.set_setting')
@patch('fx_sr.positions._load_trades', return_value={})
@patch('fx_sr.positions.ibkr')
@patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[])
@patch('fx_sr.positions.reconcile_broker_ledger')
def test_sync_skips_blocked_pair_directions(mock_reconcile, mock_broker_positions, mock_ibkr, mock_load, mock_setting):
    """sync_positions should not adopt positions for blocked pair+direction combos."""
    mock_ibkr.fetch_positions.return_value = [
        _make_ibkr_position('GBPJPY', 30000, 213.76),  # GBPJPY LONG is blocked
    ]
    mock_ibkr.fetch_open_order_counts.return_value = {}

    params = StrategyParams(use_pair_direction_filter=True)
    with patch('fx_sr.positions.BLOCKED_PAIR_DIRECTIONS', {('GBPJPY', 'LONG')}):
        result = sync_positions(params=params)

    assert 'GBPJPY:LONG' not in result
    # Must not attempt bracket resubmission for blocked positions.
    mock_ibkr.submit_bracket_for_existing_position.assert_not_called()


# ---------------------------------------------------------------------------
# neutralize_currency_balance fill-status gating
# ---------------------------------------------------------------------------

class _FakeContract:
    """Minimal contract stand-in that holds keyword attrs like the real class."""
    def __init__(self, **kwargs):
        self.conId = 0
        for k, v in kwargs.items():
            setattr(self, k, v)


def _fake_forex(pair):
    return _FakeContract(symbol=pair[:3], currency=pair[3:], exchange='IDEALPRO', secType='CASH')


def _mock_ib_for_neutralize(fill_status):
    """Build a mock IB connection whose neutralization order reports *fill_status*.

    FXCONV contracts always fail qualification (conId stays 0).
    IDEALPRO contracts succeed only for pairs in the PAIRS dict.
    """
    from fx_sr.profiles import PAIRS

    ib = MagicMock()
    ib.qualify_calls = []

    def qualify(c):
        ib.qualify_calls.append((
            getattr(c, 'exchange', ''),
            getattr(c, 'symbol', '') + getattr(c, 'currency', ''),
        ))
        if getattr(c, 'exchange', '') == 'FXCONV':
            c.conId = 0
            return [c]
        pair = getattr(c, 'symbol', '') + getattr(c, 'currency', '')
        c.conId = 123 if pair in PAIRS else 0
        return [c]

    ib.qualifyContracts = qualify

    order_status = MagicMock()
    order_status.status = fill_status
    order_status.avgFillPrice = 0.87
    trade = MagicMock()
    trade.order.orderId = 999
    trade.orderStatus = order_status
    ib.placeOrder.return_value = trade
    return ib


def _run_neutralize(fill_status, currency='EUR', amount=17000.0, account_currency='GBP'):
    """Run neutralize_currency_balance with a fully-mocked IBKR connection."""
    from fx_sr import ibkr as ibkr_module

    ibkr_module._FXCONV_QUALIFICATION_CACHE.clear()
    ib = _mock_ib_for_neutralize(fill_status)

    fake_ib_async = types.ModuleType('ib_async')
    fake_ib_async.Contract = _FakeContract
    fake_ib_async.MarketOrder = MagicMock(return_value=MagicMock())
    fake_ib_async.Forex = _fake_forex

    import sys
    with patch('fx_sr.ibkr._get_connection', return_value=(ib, True)), \
         patch('fx_sr.ibkr.log_order_event'), \
         patch.dict(sys.modules, {'ib_async': fake_ib_async}):
        from fx_sr.ibkr import neutralize_currency_balance
        neutralize_currency_balance(currency, amount, account_currency)
    return ib


def test_neutralize_records_on_filled():
    """neutralize_currency_balance records a neutralization position when Filled."""
    _run_neutralize('Filled')
    positions = load_neutralization_positions()
    assert ('EURGBP', 'SHORT') in positions


def test_neutralize_caches_failed_fxconv_qualification():
    """Repeated neutralization should not re-probe known-invalid FXCONV pairs."""
    from fx_sr import ibkr as ibkr_module

    ibkr_module._FXCONV_QUALIFICATION_CACHE.clear()
    ib = _mock_ib_for_neutralize('Filled')

    fake_ib_async = types.ModuleType('ib_async')
    fake_ib_async.Contract = _FakeContract
    fake_ib_async.MarketOrder = MagicMock(return_value=MagicMock())
    fake_ib_async.Forex = _fake_forex

    import sys
    with patch('fx_sr.ibkr._get_connection', return_value=(ib, True)), \
         patch('fx_sr.ibkr.log_order_event'), \
         patch.dict(sys.modules, {'ib_async': fake_ib_async}):
        from fx_sr.ibkr import neutralize_currency_balance
        neutralize_currency_balance('EUR', 17000.0, 'GBP')
        neutralize_currency_balance('EUR', 17000.0, 'GBP')

    fxconv_calls = [call for call in ib.qualify_calls if call[0] == 'FXCONV']
    idealpro_calls = [call for call in ib.qualify_calls if call[0] == 'IDEALPRO']
    assert fxconv_calls == [('FXCONV', 'EURGBP'), ('FXCONV', 'GBPEUR')]
    assert idealpro_calls == [('IDEALPRO', 'EURGBP'), ('IDEALPRO', 'EURGBP')]


def test_neutralize_does_not_record_on_submitted():
    """neutralize_currency_balance must NOT record when order is only Submitted."""
    _run_neutralize('Submitted')
    positions = load_neutralization_positions()
    assert ('EURGBP', 'SHORT') not in positions


def test_neutralize_does_not_record_on_presubmitted():
    """neutralize_currency_balance must NOT record when order is PreSubmitted."""
    _run_neutralize('PreSubmitted')
    positions = load_neutralization_positions()
    assert ('EURGBP', 'SHORT') not in positions
