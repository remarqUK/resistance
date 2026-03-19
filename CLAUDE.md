# CLAUDE.md

## Database

This project uses **PostgreSQL only**. There is no SQLite anywhere in the stack. Do not reference SQLite, `fx_data.db`, or `sqlite3` in code, docs, or suggestions. A legacy migration script exists at `scripts/migrate_sqlite_to_postgres.py` but is historical only.
