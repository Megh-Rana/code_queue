import uuid
import json
from datetime import datetime, timezone

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator

# ---------------------------------------------------------------------------
# App & Redis setup
# ---------------------------------------------------------------------------

app = FastAPI(title="Code Queue API", version="1.0.0")

# Connection is created once at startup and reused across requests.
redis_client: aioredis.Redis | None = None

QUEUE_KEY = "job_queue"
JOB_KEY_PREFIX = "job:"


@app.on_event("startup")
async def startup() -> None:
    global redis_client
    redis_client = aioredis.from_url(
        "redis://redis:6379",
        encoding="utf-8",
        decode_responses=True,
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    if redis_client:
        await redis_client.aclose()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

SUPPORTED_LANGUAGES = {"python", "javascript", "java", "c", "cpp", "go", "ruby", "rust"}


class JobSubmission(BaseModel):
    code: str
    language: str

    @field_validator("language")
    @classmethod
    def validate_language(cls, v: str) -> str:
        lang = v.strip().lower()
        if lang not in SUPPORTED_LANGUAGES:
            raise ValueError(
                f"Unsupported language '{v}'. Supported: {sorted(SUPPORTED_LANGUAGES)}"
            )
        return lang

    @field_validator("code")
    @classmethod
    def validate_code(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("code must not be empty")
        return v


class JobResponse(BaseModel):
    job_id: str
    status: str
    language: str
    code: str
    result: str | None
    created_at: str
    updated_at: str | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def get_job_or_404(job_id: str) -> dict:
    data = await redis_client.hgetall(f"{JOB_KEY_PREFIX}{job_id}")
    if not data:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return data


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/jobs", response_model=JobResponse, status_code=201)
async def submit_job(submission: JobSubmission):
    """Submit a code snippet for async processing."""
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    # Store empty strings in Redis (hashes don't support null values)
    job_data = {
        "job_id": job_id,
        "status": "pending",
        "language": submission.language,
        "code": submission.code,
        "result": "",
        "created_at": now,
        "updated_at": "",
    }

    # Persist job state and enqueue atomically via pipeline
    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.hset(f"{JOB_KEY_PREFIX}{job_id}", mapping=job_data)
        pipe.lpush(QUEUE_KEY, job_id)
        await pipe.execute()

    return JobResponse(
        job_id=job_id,
        status="pending",
        language=submission.language,
        code=submission.code,
        result=None,
        created_at=now,
        updated_at=None,
    )


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    """Get the current status and result of a job."""
    data = await get_job_or_404(job_id)
    return JobResponse(
        job_id=data["job_id"],
        status=data["status"],
        language=data["language"],
        code=data["code"],
        result=data["result"] or None,
        created_at=data["created_at"],
        updated_at=data["updated_at"] or None,
    )


@app.get("/jobs", response_model=list[JobResponse])
async def list_jobs():
    """List all jobs (scans Redis — for dev/debug use)."""
    keys = await redis_client.keys(f"{JOB_KEY_PREFIX}*")
    if not keys:
        return []

    jobs = []
    for key in keys:
        data = await redis_client.hgetall(key)
        if data:
            jobs.append(
                JobResponse(
                    job_id=data["job_id"],
                    status=data["status"],
                    language=data["language"],
                    code=data["code"],
                    result=data["result"] or None,
                    created_at=data["created_at"],
                    updated_at=data["updated_at"] or None,
                )
            )

    # Sort newest first
    jobs.sort(key=lambda j: j.created_at, reverse=True)
    return jobs


@app.get("/health")
async def health():
    """Health check — also pings Redis."""
    await redis_client.ping()
    return {"status": "ok", "redis": "ok"}
