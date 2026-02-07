# Shiro Report Frontend

This is a static Next.js frontend that reads `reports.json` (fallback to `report.json`) at runtime and renders the fuzz case index.

## Local preview
```bash
npm install
npm run dev
```

Open `http://localhost:3000` and ensure `web/public/reports.json` (or `web/public/report.json`) exists.

## Static export
```bash
npm run build
```

The static site is emitted to `web/out/`. Deploy that directory to GitHub Pages or Vercel.

## Data refresh
Update `web/public/reports.json` and redeploy. No backend required.

If you deploy Cloudflare Worker metadata APIs, set `NEXT_PUBLIC_WORKER_BASE_URL` before build.
The UI will show per-case `Find similar` links to `/api/v1/cases/:case_id/similar`.
Case-level archive/report action links are rendered only for HTTP(S) URLs; `s3://` URLs are hidden and should be exposed through Worker download endpoints or a public artifact base URL.

Cloudflare worker sources are colocated under `web/cloudflare-worker/`.

## Notes
- Commit filters rely on `tidb_commit` populated by `cmd/shiro-report` from `tidb_version()` or plan replayer metadata.
- For GitHub Pages subpaths, set `NEXT_PUBLIC_BASE_PATH=/your-repo` before `npm run build`.
