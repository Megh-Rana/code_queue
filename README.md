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
  ▲                     │
  └──  GET /jobs/{id}  ─┘
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
  "updated_at": null
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
| `error` | Worker hit an unexpected error |

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
- **Retries & dead-letter queue** — Re-queue failed jobs with backoff; move permanently failed jobs to a dead-letter list.

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
