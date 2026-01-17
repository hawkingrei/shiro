# Shiro Report Frontend

This is a static Next.js frontend that reads `report.json` at runtime and renders the fuzz case index.

## Local preview
```bash
npm install
npm run dev
```

Open `http://localhost:3000` and ensure `web/public/report.json` exists.

## Static export
```bash
npm run build
```

The static site is emitted to `web/out/`. Deploy that directory to GitHub Pages or Vercel.

## Data refresh
Update `web/public/report.json` and redeploy. No backend required.

## Notes
- Commit filters rely on `tidb_commit` populated by `cmd/shiro-report` from `tidb_version()` or plan replayer metadata.
- For GitHub Pages subpaths, set `NEXT_PUBLIC_BASE_PATH=/your-repo` before `npm run build`.
