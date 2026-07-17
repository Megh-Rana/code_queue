import os
import secrets
import uuid
from datetime import datetime, timezone

import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, field_validator

# ---------------------------------------------------------------------------
# App & Redis setup
# ---------------------------------------------------------------------------

app = FastAPI(title="Code Queue API", version="1.2.0")

redis_client: aioredis.Redis | None = None

QUEUE_KEY = "job_queue"
DEAD_LETTER_KEY = "dead_letter_queue"
JOB_KEY_PREFIX = "job:"
API_KEYS_SET = "api_keys"
DEFAULT_MAX_ATTEMPTS = 3

# Seed key loaded from environment — added to Redis on startup so the first
# caller is never locked out. If unset, a random key is generated and logged.
SEED_API_KEY = os.getenv("SEED_API_KEY")


@app.on_event("startup")
async def startup() -> None:
    global redis_client
    redis_client = aioredis.from_url(
        "redis://redis:6379",
        encoding="utf-8",
        decode_responses=True,
    )

    # Bootstrap the seed key
    key = SEED_API_KEY or secrets.token_urlsafe(32)
    await redis_client.sadd(API_KEYS_SET, key)

    if not SEED_API_KEY:
        # Log it so the operator can retrieve it from container logs
        print(f"\n{'='*60}")
        print(f"  No SEED_API_KEY set. Generated key for this session:")
        print(f"  {key}")
        print(f"{'='*60}\n", flush=True)


@app.on_event("shutdown")
async def shutdown() -> None:
    if redis_client:
        await redis_client.aclose()


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------

_bearer_scheme = HTTPBearer(auto_error=False)


async def require_api_key(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
) -> str:
    """Validate Bearer token against the Redis api_keys set."""
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header. Expected: Bearer <api-key>",
            headers={"WWW-Authenticate": "Bearer"},
        )

    key = credentials.credentials
    is_valid = await redis_client.sismember(API_KEYS_SET, key)
    if not is_valid:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return key


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
    attempt: int
    max_attempts: int
    last_error: str | None


class ApiKeyResponse(BaseModel):
    key: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def get_job_or_404(job_id: str) -> dict:
    data = await redis_client.hgetall(f"{JOB_KEY_PREFIX}{job_id}")
    if not data:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return data


def _to_job_response(data: dict) -> JobResponse:
    return JobResponse(
        job_id=data["job_id"],
        status=data["status"],
        language=data["language"],
        code=data["code"],
        result=data.get("result") or None,
        created_at=data["created_at"],
        updated_at=data.get("updated_at") or None,
        attempt=int(data.get("attempt", 0)),
        max_attempts=int(data.get("max_attempts", DEFAULT_MAX_ATTEMPTS)),
        last_error=data.get("last_error") or None,
    )


# ---------------------------------------------------------------------------
# Auth management routes  (/auth/keys)
# ---------------------------------------------------------------------------

@app.post("/auth/keys", response_model=ApiKeyResponse, status_code=201)
async def create_api_key(_key: str = Depends(require_api_key)):
    """Generate a new API key and add it to the active set."""
    new_key = secrets.token_urlsafe(32)
    await redis_client.sadd(API_KEYS_SET, new_key)
    return ApiKeyResponse(key=new_key)


@app.get("/auth/keys", response_model=list[str])
async def list_api_keys(_key: str = Depends(require_api_key)):
    """List all active API keys."""
    keys = await redis_client.smembers(API_KEYS_SET)
    return sorted(keys)


@app.delete("/auth/keys/{target_key}", status_code=204)
async def revoke_api_key(target_key: str, current_key: str = Depends(require_api_key)):
    """
    Revoke an API key. Prevents revoking the key currently in use to avoid
    accidental self-lockout.
    """
    if target_key == current_key:
        raise HTTPException(
            status_code=400,
            detail="Cannot revoke the API key you are currently using.",
        )
    removed = await redis_client.srem(API_KEYS_SET, target_key)
    if not removed:
        raise HTTPException(status_code=404, detail="API key not found.")


# ---------------------------------------------------------------------------
# Job routes  (all protected)
# ---------------------------------------------------------------------------

@app.post("/jobs", response_model=JobResponse, status_code=201)
async def submit_job(
    submission: JobSubmission,
    _key: str = Depends(require_api_key),
):
    """Submit a code snippet for async processing."""
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    job_data = {
        "job_id": job_id,
        "status": "pending",
        "language": submission.language,
        "code": submission.code,
        "result": "",
        "created_at": now,
        "updated_at": "",
        "attempt": "0",
        "max_attempts": str(DEFAULT_MAX_ATTEMPTS),
        "last_error": "",
    }

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
        attempt=0,
        max_attempts=DEFAULT_MAX_ATTEMPTS,
        last_error=None,
    )


@app.get("/jobs/dead-letter", response_model=list[JobResponse])
async def list_dead_letter_jobs(_key: str = Depends(require_api_key)):
    """List all jobs in the dead-letter queue (permanently failed)."""
    job_ids = await redis_client.lrange(DEAD_LETTER_KEY, 0, -1)
    if not job_ids:
        return []

    jobs = []
    for job_id in job_ids:
        data = await redis_client.hgetall(f"{JOB_KEY_PREFIX}{job_id}")
        if data:
            jobs.append(_to_job_response(data))
    return jobs


@app.post("/jobs/{job_id}/retry", response_model=JobResponse)
async def retry_dead_letter_job(
    job_id: str,
    _key: str = Depends(require_api_key),
):
    """Re-queue a dead-lettered job. Resets attempt counter."""
    data = await get_job_or_404(job_id)

    if data.get("status") != "failed":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job '{job_id}' is not in failed state "
                f"(current status: {data.get('status')}). "
                "Only failed jobs can be manually retried."
            ),
        )

    now = datetime.now(timezone.utc).isoformat()

    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.hset(
            f"{JOB_KEY_PREFIX}{job_id}",
            mapping={"status": "pending", "attempt": "0", "last_error": "", "updated_at": now},
        )
        pipe.lrem(DEAD_LETTER_KEY, 0, job_id)
        pipe.lpush(QUEUE_KEY, job_id)
        await pipe.execute()

    updated = await redis_client.hgetall(f"{JOB_KEY_PREFIX}{job_id}")
    return _to_job_response(updated)


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, _key: str = Depends(require_api_key)):
    """Get the current status and result of a job."""
    data = await get_job_or_404(job_id)
    return _to_job_response(data)


@app.get("/jobs", response_model=list[JobResponse])
async def list_jobs(_key: str = Depends(require_api_key)):
    """List all jobs (scans Redis — for dev/debug use)."""
    keys = await redis_client.keys(f"{JOB_KEY_PREFIX}*")
    if not keys:
        return []

    jobs = []
    for key in keys:
        data = await redis_client.hgetall(key)
        if data:
            jobs.append(_to_job_response(data))

    jobs.sort(key=lambda j: j.created_at, reverse=True)
    return jobs


@app.get("/health")
async def health():
    """Health check — no auth required."""
    await redis_client.ping()
    return {"status": "ok", "redis": "ok"}
