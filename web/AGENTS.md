# Shiro Report Frontend - Agent Notes

## Summary
- Next.js app (static export) reads `report.json` from `web/public/`.
- Filter by oracle, TiDB commit, and errors; text search covers SQL and details.
- Designed for GitHub Pages / Vercel; update JSON to refresh data.

## Data flow
- `cmd/shiro-report` generates `report.json`.
- Frontend fetches `./report.json` on load and renders cases.
- Commit field is derived from `tidb_version()` or plan replayer meta.

## Deployment notes
- Static export is under `web/out/`.
- For GitHub Pages subpaths, set `NEXT_PUBLIC_BASE_PATH` before `npm run build`.
