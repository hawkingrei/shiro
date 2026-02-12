# Shiro Cloudflare Worker Metadata Plane

This worker provides a minimal metadata plane for fuzz cases while case artifacts remain in object storage.

## What it stores in D1
- `case_id` (UUIDv7, primary key)
- triage metadata: `labels`, `linked_issue`

Schema is in `schema.sql`.

## Endpoints
- `GET /api/v1/health`
- `POST /api/v1/cases/sync`
- `GET /api/v1/cases`
- `GET /api/v1/cases/:case_id`
- `GET /api/v1/cases/:case_id/similar`
- `PATCH /api/v1/cases/:case_id`
- `POST /api/v1/cases/search`

`/api/v1/cases/sync`, `GET /api/v1/cases/:case_id`, and `PATCH /api/v1/cases/:case_id` require `Authorization: Bearer <API_TOKEN>` when `API_TOKEN` is set.
If you really need local insecure mode, set `ALLOW_INSECURE_WRITES=1`.

## Similar-bug search
`GET /api/v1/cases/:case_id/similar?limit=20&ai=1`
- Uses one bug (`case_id`) as anchor and returns ranked similar bugs.
- Ranking combines deterministic similarity (`labels`, `linked_issue`, token overlap).
- `ai=1` adds AI explanation/rerank summary over top candidates.

## Sync payload
Only `case_id` is used for metadata registration; additional fields are ignored.
```json
{
  "manifest_url": "https://<r2-public-domain>/<prefix>/reports.json",
  "generated_at": "2026-02-06T16:32:00Z",
  "source": "s3://<bucket>/<prefix>/",
  "cases": [
    {
      "case_id": "0194d4f8-b6ce-7d4e-b13d-3be7446954d4"
    }
  ]
}
```

## Quick start
1. Create D1 database and apply schema:
```bash
wrangler d1 create shiro_cases
wrangler d1 execute shiro_cases --file ./schema.sql
```
2. Copy `wrangler.toml.example` to `wrangler.toml` and fill database id / token vars.
3. Run locally:
```bash
wrangler dev
```
4. Deploy:
```bash
wrangler deploy
```

## Assets
The Git-integrated deploy expects an assets directory. This repo provides an empty `assets/` by default.
If you want to serve a static UI, build it and copy the output into `assets/` before deploying.
Cloudflare's Git integration runs `wrangler versions upload` from the repo root, so the root
`wrangler.jsonc` mirrors the worker entrypoint and assets directory.

## Serve the report UI from the Worker
To deploy a single Worker that serves the UI and APIs from the same domain:
```bash
cd web
npm run build:worker
```
This exports the Next.js UI to `web/out` and copies it into `web/cloudflare-worker/assets`.
Deploy the worker normally after the assets are in place.

## Integration with `cmd/shiro-report`
Use these flags to publish report manifests to R2/S3-compatible storage and sync D1 metadata:
```bash
go run ./cmd/shiro-report \
  -input s3://<artifact-bucket>/<cases-prefix>/ \
  -output web/public \
  -artifact-public-base-url https://<artifact-public-domain> \
  -publish-endpoint https://<accountid>.r2.cloudflarestorage.com \
  -publish-region auto \
  -publish-bucket <r2-bucket> \
  -publish-prefix shiro/manifests/latest \
  -publish-access-key-id <r2-access-key> \
  -publish-secret-access-key <r2-secret-key> \
  -publish-public-base-url https://<public-r2-domain> \
  -worker-sync-endpoint https://<worker-domain>/api/v1/cases/sync \
  -worker-sync-token <api-token>
```

If publish/sync flags are omitted, behavior stays unchanged.

## Security and limits
- Sync and patch request bodies are size-limited (`2 MiB` for sync, `64 KiB` for search/patch).
- Sync rejects oversized `cases[]` payloads (limit `2000`).
- CORS responses include `Vary: Origin`.
