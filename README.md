# Code Queue

An async code submission queue built with **FastAPI**, **Redis**, and a **Worker** process — all containerized with Docker Compose.

Submit a code snippet and language, get a job ID back, and poll for the result. The worker picks up jobs from the Redis queue and simulates execution asynchronously.

---

## Architecture

```
Client
  │
  ▼
POST /jobs  ──►  Redis List (job_queue)  ──►  Worker (BRPOP)
                 Redis Hash (job:{id})  ◄──────────────┘
  ▲                     │                       │ (on failure)
  └──  GET /jobs/{id}  ─┘               re-queue w/ backoff
                                                │ (exhausted)
                                        Redis List (dead_letter_queue)
                                                │
                                        GET /jobs/dead-letter
                                        POST /jobs/{id}/retry
```

- **API** (`api/`) — FastAPI service. Accepts submissions, stores job state in Redis, exposes status endpoints.
- **Worker** (`worker/`) — Plain Python process. Blocking-pops job IDs from the Redis List, simulates execution (random delay + fake output), writes result back.
- **Redis** — Queue (List) + job state store (Hashes).

---

## Quick Start

**Prerequisites:** Docker and Docker Compose v2.

```bash
# Clone / enter project
cd code_queue

# Build and start all 3 services
docker compose up --build
```

API is available at `http://localhost:8000`.  
Interactive docs (Swagger UI) at `http://localhost:8000/docs`.

---

## API Reference

### Submit a job

```bash
curl -s -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"code": "print(\"Hello, World!\")", "language": "python"}' | jq
```

Response:
```json
{
  "job_id": "3f2a1b4c-...",
  "status": "pending",
  "language": "python",
  "code": "print(\"Hello, World!\")",
  "result": null,
  "created_at": "2026-07-17T13:51:00+00:00",
  "updated_at": null,
  "attempt": 0,
  "max_attempts": 3,
  "last_error": null
}
```

### Check job status

```bash
curl -s http://localhost:8000/jobs/<job_id> | jq
```

Possible `status` values:

| Status | Meaning |
|---|---|
| `pending` | Queued, not yet picked up |
| `processing` | Worker is currently executing |
| `done` | Finished — check `result` field |
| `failed` | Exhausted all retry attempts — check `last_error` field |

### List all jobs

```bash
curl -s http://localhost:8000/jobs | jq
```

> Uses Redis `KEYS` scan — fine for dev/debug, not for production at scale.

### Health check

```bash
curl -s http://localhost:8000/health
# {"status":"ok","redis":"ok"}
```

---

## Retries & Dead-Letter Queue

When a job fails, the worker automatically retries it with **exponential backoff** before giving up.

**Backoff schedule** (default `MAX_ATTEMPTS=3`):

| Attempt | Delay before retry |
|---|---|
| 1 → 2 | 2 seconds |
| 2 → 3 | 4 seconds |
| 3 (final) | — moved to dead-letter queue |

After exhausting all attempts, the job is moved to the **dead-letter queue** and its status is set to `failed`. The `last_error` field contains the error from the final attempt.

`MAX_ATTEMPTS` is configurable via an environment variable in `docker-compose.yml`.

### List dead-lettered jobs

```bash
curl -s http://localhost:8000/jobs/dead-letter | jq
```

### Manually retry a dead-lettered job

Resets the attempt counter and re-queues the job for a fresh run.

```bash
curl -s -X POST http://localhost:8000/jobs/<job_id>/retry | jq
```

Returns `409` if the job is not in `failed` state.

---

## Supported Languages

`python`, `javascript`, `java`, `c`, `cpp`, `go`, `ruby`, `rust`

---

## Scaling the Worker

Run multiple worker instances to process jobs in parallel:

```bash
docker compose up --build --scale worker=3
```

---

## Stopping

```bash
docker compose down
```

---

## Future Scope

- **Real execution** — Replace the simulated worker with a sandboxed runner (e.g., subprocess with resource limits, or ephemeral containers via Docker-in-Docker).
- **Persistence** — Add a Redis volume in `docker-compose.yml` so jobs survive restarts.
- **Authentication** — Add API key or JWT auth to the FastAPI layer.
- **Redis Streams** — Upgrade from a List to Redis Streams for consumer groups, message acknowledgment, and replay.

---

## Project Structure

```
code_queue/
├── api/
│   ├── main.py          # FastAPI app
│   ├── requirements.txt
│   └── Dockerfile
├── worker/
│   ├── worker.py        # Queue consumer
│   ├── requirements.txt
│   └── Dockerfile
├── docker-compose.yml
└── README.md
```
