# CLAUDE.md

## Database

This project uses **PostgreSQL only**. There is no SQLite anywhere in the stack. Do not reference SQLite, `fx_data.db`, or `sqlite3` in code, docs, or suggestions. A legacy migration script exists at `scripts/migrate_sqlite_to_postgres.py` but is historical only.

## Frontend

The dashboard is a **React SPA** (`frontend/live-dashboard/src/`), built with Vite and served as static files from `fx_sr/web_live/react/`. All frontend page changes must go in React components (`.tsx` files in `frontend/live-dashboard/src/pages/`), **not** in vanilla HTML files in `fx_sr/web_live/`.

- Vanilla JS files in `fx_sr/web_live/` (e.g. `replay.js`, `live_diary.js`, `diary_shared.js`) are loaded by React components via `<script>` tags — JS logic changes still go there.
- HTML structure changes must go in the `.tsx` components, not `.html` files.
- After React changes, run `npm run build` from the repo root to rebuild the SPA.
- Routes are defined in `frontend/live-dashboard/src/App.tsx` and served via `_index` (the SPA shell) in `fx_sr/live_web.py`.
