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
│   ├── Hybrid Search.bru            # POST /api/v1/search
│   ├── ReAct Agent.bru              # POST /api/v1/agent
│   ├── Self-RAG.bru                 # POST /api/v1/selfrag
│   └── Legacy Agent.bru             # POST /api/v1/legacy/agent
└── README.md
```

## Environment variables

| Var      | Default                 | Where set                                       |
|----------|-------------------------|-------------------------------------------------|
| baseUrl  | `http://localhost:8000` | environment file                                |
| apiKey   | `sk-litellm-stub`       | environment file (marked secret — git-ignored when committed) |

The default `apiKey` matches `ApiSettings.api_key` from `src/config.py`
for early bring-up. In production, replace it with one of the values
listed in `API_KEYS`.

## Auth

Every endpoint except `/health` requires the header

```
X-API-Key: {{apiKey}}
```

Bruno renders this automatically because the requests pull the value
from `vars` in the active environment.

## Typical flow

1. **Health** → confirm the API is up.
2. **Upload Document** → POST a small file, copy `job_id` from the
   response.
3. **Get Job Status** → set the `jobId` request var to the value
   above; poll until status is `completed` (or `vector_only`).
4. **Search / ReAct Agent / Self-RAG** → query the corpus.

## Notes

- `Upload Document` uses Bruno's `@file(...)` helper. Drop a sample
  file at `docs/bruno/samples/sample.txt` (or change the path
  inline). Anything LlamaIndex's `SimpleDirectoryReader` accepts
  works — PDF, DOCX, PPTX, TXT, MD, EML.
- The synthesizer always forces a Russian-language answer (corpus
  is normalised to Russian); English questions are fine but the
  reply will be Russian.
- `POST /api/v1/legacy/agent` is mounted only when
  `AGENT_ENABLE_LEGACY_AGENT=true`. Otherwise the request returns
  404.

## Updating the collection

`.bru` is line-based and git-friendly. When you change an endpoint:

1. Tweak the route in `src/api/routes/...`.
2. Update the matching `.bru` (request body, docs section).
3. Run `uv run pytest tests/test_api -v` to verify the contract.
4. Commit both `src/` and `docs/bruno/` together.
