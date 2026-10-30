# Confluencr backend assessment

Status: ACTIVE

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
- Cloud deployment remains deferred; user will choose a hosting target after testing.
- Next: stage reviewed source, commit on `main`, create the user's public GitHub repository, push, verify the remote repository and CI run, then update this worksheet to COMPLETE.
