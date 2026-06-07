# Bruno API collection — kb-llamaindex

[Bruno](https://www.usebruno.com/) collection for the kb-llamaindex
HTTP API. Stored as plain `.bru` text files so it diffs cleanly in
git.

## Open in Bruno

1. Install Bruno: `brew install bruno` or download from
   <https://www.usebruno.com/downloads>.
2. **File → Open Collection** → pick `docs/bruno/`.
3. Top-right environment selector → choose `local` (default points at
   `http://localhost:8000`).
4. Replace the `apiKey` secret in the environment with one of the
   keys configured via the `API_KEYS` env var on the server.

## Layout

```
docs/bruno/
├── bruno.json                       # collection metadata
├── environments/
│   ├── local.bru                    # baseUrl + apiKey for dev
│   └── docker.bru                   # same default but separate slot
│                                    # for any container-side overrides
├── Health/
│   └── Health Check.bru             # GET /health (no auth)
├── Ingestion/
│   ├── Upload Document.bru          # POST /api/v1/ingest (multipart)
│   └── Get Job Status.bru           # GET /api/v1/ingest/{job_id}
├── Search/
│   ├── Local Search.bru             # POST /api/v1/search/local
│   ├── Global Search.bru            # POST /api/v1/search/global
│   ├── Drift Search.bru             # POST /api/v1/search/drift
│   └── Auto Search.bru              # POST /api/v1/search/auto
├── Documents/
│   └── Download Source.bru          # GET /api/v1/documents/{doc_id}
├── Admin/
│   ├── Rebuild Communities.bru      # POST /api/v1/admin/communities/rebuild
│   └── Wiki Rebuild.bru             # POST /admin/wiki/rebuild (NO /api/v1, no auth)
└── README.md
```

> The legacy `/api/v1/search`, `/agent`, `/selfrag`, `/legacy/agent`
> endpoints were removed in the R7b cutover. The sole search surface is
> now `/api/v1/search/{local,global,drift,auto}` — usage + tuning memo:
> `docs/runbook/search-usage.md`.

## Environment variables

| Var      | Default                 | Where set                                       |
|----------|-------------------------|-------------------------------------------------|
| baseUrl  | `http://localhost:8000` | environment file                                |
| apiKey   | `sk-litellm-stub`       | environment file (marked secret — git-ignored when committed) |

The default `apiKey` matches `ApiSettings.api_key` from `src/config.py`
for early bring-up. In production, replace it with one of the values
listed in `API_KEYS`.

## Auth

Most endpoints require the header

```
X-API-Key: {{apiKey}}
```

Bruno renders this automatically because the requests pull the value
from `vars` in the active environment.

Exceptions (no `X-API-Key` in code):
- `GET /health` — public liveness probe.
- `POST /admin/wiki/rebuild` — has no `require_api_key` dependency
  (and is mounted without the `/api/v1` prefix). Treat it as an
  internal/operator endpoint.

## Typical flow

1. **Health** → confirm the API is up.
2. **Upload Document** → POST a small file, copy `job_id` from the
   response.
3. **Get Job Status** → set the `jobId` request var to the value
   above; poll until status is `completed` (or `vector_only`).
4. **Search → Local** → query the corpus (default mode). Use **Auto**
   to let the router pick the mode.
5. **Documents → Download Source** → set the `docId` request var to a
   `doc_id` from a search response's `sources[]` / `documents[]` and
   pull the original file (Bruno: *Response → Save Response*).
6. *(optional, for Global/Drift)* **Admin → Rebuild Communities** once,
   then use **Global** / **Drift** for corpus-level questions.
7. *(optional)* **Admin → Wiki Rebuild** to (re)generate per-entity
   MediaWiki articles (`?all=true` rebuilds all; needs `WIKI_ENABLED`).

## Notes

- `Upload Document` uses Bruno's `@file(...)` helper. Drop a sample
  file at `docs/bruno/samples/sample.txt` (or change the path
  inline). Anything LlamaIndex's `SimpleDirectoryReader` accepts
  works — PDF, DOCX, PPTX, TXT, MD, EML.
- The synthesizer always forces a Russian-language answer (corpus
  is normalised to Russian); English questions are fine but the
  reply will be Russian.
- **Global** and **Drift** map-reduce over community summaries — run
  **Admin → Rebuild Communities** first (needs Neo4j + GDS; the build is
  offline and runs for minutes).
- All four search modes share the `SearchRequest` shape; only `query`,
  `top_k` and `history` (multi-turn — `[{role, content}]`) are consumed
  — the mode is chosen by the endpoint, not a body field. Other fields
  (`mode`, `department`, filters, …) are accepted but ignored.
- The `SearchResponse` carries `documents[]` (`{doc_id, url}`) alongside
  `sources[]`; each `url` is the relative `/api/v1/documents/{doc_id}`
  download link served by **Documents → Download Source**.

## Updating the collection

`.bru` is line-based and git-friendly. When you change an endpoint:

1. Tweak the route in `src/api/routes/...`.
2. Update the matching `.bru` (request body, docs section).
3. Run `uv run pytest tests/test_api -v` to verify the contract.
4. Commit both `src/` and `docs/bruno/` together.
