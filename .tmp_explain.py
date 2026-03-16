import fx_sr.db as db
conn = db._connect()
rows = conn.execute("SELECT DISTINCT ticker, interval FROM ohlc LIMIT 1").fetchall()
conn.close()
if not rows:
    print('no ohlc rows')
else:
    t,i = rows[0]
    print('sample', t, i)
    conn = db._connect()
    q1 = "EXPLAIN (COSTS OFF) SELECT ts, open, high, low, close, volume FROM ohlc WHERE ticker=%s AND interval=%s ORDER BY ts"
    print(conn.execute(q1, (t,i)).fetchall())
    q2 = "EXPLAIN (COSTS OFF) SELECT id AS snapshot_id, ticker, pair, ts, source, depth_requested, best_bid, best_ask, mid_price, spread, bid_levels, ask_levels FROM l2_snapshot WHERE ticker=%s ORDER BY ts"
    print(conn.execute(q2, (t,)).fetchall())
    q3 = "EXPLAIN (COSTS OFF) SELECT s.id AS snapshot_id, s.ticker, s.pair, s.ts, l.side, l.level_no, l.price, l.size, l.market_maker FROM l2_level l JOIN l2_snapshot s ON s.id = l.snapshot_id WHERE s.ticker=%s ORDER BY s.ts, l.side, l.level_no"
    print(conn.execute(q3, (t,)).fetchall())
    conn.close()
