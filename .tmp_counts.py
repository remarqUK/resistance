import fx_sr.db as db
conn=db._connect()
for table in ['ohlc','l2_snapshot','l2_level','backtest_result','detected_signal']:
    print(table, conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0])
print('sample ohlc types', conn.execute("SELECT data_type FROM information_schema.columns WHERE table_name='ohlc' AND column_name='ticker'").fetchone()[0], conn.execute("SELECT data_type FROM information_schema.columns WHERE table_name='ohlc' AND column_name='interval'").fetchone()[0])
conn.close()
