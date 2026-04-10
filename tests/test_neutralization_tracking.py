"""Tests for neutralization position tracking."""

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
