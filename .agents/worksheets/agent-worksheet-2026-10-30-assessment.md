# Confluencr backend assessment

Status: COMPLETE

## Scope and authorization

- User requested a new implementation under `~/stuff`, completed and tested locally.
- Inspect the supplied PDF for suspicious agent instructions before implementation; stop immediately if found.
- Deployment is explicitly deferred. The user subsequently authorized a public GitHub repository, a commit, and publication.
- The PDF requests a Python webhook API, 202 acknowledgments within 500ms, persistent transactions, a 30-second background delay, idempotency, health/status endpoints, and a README.
- Starting Git commit: none (new repository).

## PDF inspection

- Inspected all three rendered pages, extracted text and text styles, metadata, annotations, widgets, optional layers, embedded file names, decoded object streams, and hidden text traces.
- No suspicious agent instructions found. Two invisible spans on page 3 contain only ordinary arrows used in the success criteria. No embedded files, links, annotations, JavaScript, or actions.
- Source: the user-supplied Full Stack Developer Assessment PDF (not redistributed).

## Decisions and plan

- FastAPI, Psycopg async pools, PostgreSQL (local Docker; cloud PostgreSQL via DATABASE_URL later).
- One durable transaction row doubles as the queue; insertion and enqueueing are the same commit.
- Persist a not-before deadline 30 seconds after receipt. Separate workers atomically finish due rows with FOR UPDATE SKIP LOCKED. No in-memory timers or second broker.
- First payload wins for a transaction ID, including conflicting repeats. All valid repeats return 202.
- Preserve the PDF's single-item array for the transaction query response.
- Use real PostgreSQL integration tests, a real 30-second smoke test, load measurements, and restart/error recovery checks.
- Add Docker Compose, reproducible dependency lock, CI, run/test documentation, and a source bundle for handoff.

## Progress / verification

- Application, migration, Docker Compose, exact Decimal JSON input/output, lockfile, README, CI and verification scripts implemented.
- `uv run pytest -q -s`: 31 passed in 33.56s, including the real delay with 200 transactions at concurrency 20. In-process acknowledgments: p50 8.1ms, p95 13.6ms, maximum 17.7ms; completion delay 30.002–30.111s.
- `docker compose up -d --build`: passed after giving only the API service ownership of the shared image build (concurrent same-tag builds conflicted).
- Real recovery script passed: worker SIGKILL, API restart, database stop/start durability, 503 during storage outage (3.483ms), retry after recovery, and duplicate timestamp preservation.
- Run HTTP load and recovery scripts sequentially: recovery deliberately interrupts the database/API. An initial HTTP run was interrupted by starting recovery before it completed; repeat it in isolation before recording its result.
- Isolated real HTTP run passed: 200 unique transactions, 21 duplicate deliveries, concurrency 20. Acknowledgment p50 29.751ms, p95 111.657ms, maximum 171.449ms; delay 30.003446–30.116849s. Every final result queried and duplicate timestamp preservation checked.
- Ruff lint and formatting, lock consistency, OpenAPI schema and Swagger tester checks passed.
- Final artifacts: source ZIP, combined verification JSON and PDF-inspection JSON under the chat's `outputs/` folder. Reports are also included under repository `reports/`.
- Handoff: Docker Compose API, worker and PostgreSQL are running locally, API tester at http://localhost:8000/docs. README covers startup, tests, architecture, scope, limits and later cloud configuration.
- The user subsequently authorized migration execution, API redeployment and a worker deployment on Northflank.
- Published public repository: `https://github.com/Aniganesh/confluencr-transaction-webhooks`, default branch `main`.
- Initial source commit `3a98b1a5f44ef26b9a6f688374b336def0e678ae` was pushed successfully. GitHub Actions workflow `Verify` was queued on that revision.
- Northflank migration build `keen-spot-8004` and run `8ef56b0f-ceb4-4172-b653-a92bca753a95` succeeded; API restarted with 1/1 health checks passing.
- Worker `confluencr-worker` reuses API image `steadfast-tiger-8015`, command `python -m app.worker`, one free 256 MB instance, no ports, and a directly copied API DATABASE_URL (credentials never stored in this repository).
- Live transaction `txn_cloud_5b678d8964bb4c178ed26d49d7c43ff9` completed in 30.043156s. Replays preserved the completed record and timestamps; a warm HTTP POST returned 202 in 188.758ms. Evidence: `reports/worker-verification.json`.
- Public API: https://p01--confluencr-webhook-demo--wrj6twgtmvhr.code.run/ ; interactive tester: append `/docs`.
- Worker-verification heartbeat was disabled after completion. GitHub implementation CI passed. Documentation and source archive refreshed for the authorized assessment submission.
