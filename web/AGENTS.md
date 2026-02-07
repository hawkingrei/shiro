# Shiro Report Frontend - Agent Notes

## Summary
- Next.js app (static export) reads `reports.json` with fallback to `report.json` from `web/public/`.
- Filter by oracle, TiDB commit, and errors; text search covers SQL and details.
- Designed for GitHub Pages / Vercel; update JSON to refresh data.

## Data flow
- `cmd/shiro-report` generates both `reports.json` and `report.json`.
- Frontend fetches `./reports.json` first, then falls back to `./report.json`.
- Commit field is derived from `tidb_version()` or plan replayer meta.
- Worker integration is optional via `NEXT_PUBLIC_WORKER_BASE_URL` for download/similar-bug API links.

## Deployment notes
- Static export is under `web/out/`.
- For GitHub Pages subpaths, set `NEXT_PUBLIC_BASE_PATH` before `npm run build`.
