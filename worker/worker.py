"""
Worker process — pulls jobs from the Redis queue and simulates execution.

Queue strategy: Redis List with BRPOP (blocking right-pop).
  - API pushes job IDs to the left  (LPUSH job_queue <id>)
  - Worker pops from the right       (BRPOP job_queue)
  → FIFO order

Retry strategy: exponential backoff.
  - On failure, re-queue the job after 2^attempt seconds (2s, 4s, 8s, ...)
  - After MAX_ATTEMPTS failures, move the job to the dead-letter list and
    mark status = "failed". No further processing.

Future scope: replace _simulate_execution() with a real sandboxed runner
(e.g., subprocess + resource limits, or a container-per-job approach).
"""

import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import redis

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("worker")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
QUEUE_KEY = "job_queue"
DEAD_LETTER_KEY = "dead_letter_queue"
JOB_KEY_PREFIX = "job:"
BRPOP_TIMEOUT = 5       # seconds — unblocks periodically so shutdown signal is checked
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

# ---------------------------------------------------------------------------
# Simulated execution
# ---------------------------------------------------------------------------

# Fake outputs per language — gives the simulation a bit of personality
FAKE_OUTPUTS: dict[str, list[str]] = {
    "python":     ["Hello, World!", "42", "[1, 2, 3]", "True"],
    "javascript": ["Hello, World!", "undefined", "Promise { <pending> }", "NaN"],
    "java":       ["Hello, World!", "Exception in thread \"main\"... (just kidding)", "0"],
    "c":          ["Segmentation fault (core dumped) — just kidding!", "Hello, World!", "0"],
    "cpp":        ["Hello, World!", "0", "std::bad_alloc — just kidding!"],
    "go":         ["Hello, World!", "goroutine leak detected — just kidding!", "0"],
    "ruby":       ["Hello, World!", "nil", "=> 42"],
    "rust":       ["Hello, World!", "error[E0502] — just kidding, it compiled!", "()"],
}

DEFAULT_OUTPUTS = ["Hello, World!", "Done.", "0"]


def _simulate_execution(language: str, code: str) -> str:
    """Simulate code execution with a random delay and fake output."""
    delay = random.uniform(2, 5)
    log.info("  Simulating execution (%.1fs delay)...", delay)
    time.sleep(delay)

    outputs = FAKE_OUTPUTS.get(language, DEFAULT_OUTPUTS)
    fake_output = random.choice(outputs)

    lines = code.strip().splitlines()
    loc = len(lines)

    return (
        f"[Simulated] Compiled and ran {loc} line(s) of {language}.\n"
        f"stdout: {fake_output}\n"
        f"exit code: 0"
    )


# ---------------------------------------------------------------------------
# Retry / dead-letter helpers
# ---------------------------------------------------------------------------

def _backoff_seconds(attempt: int) -> float:
    """Exponential backoff: 2^attempt seconds (2, 4, 8, ...)."""
    return float(2 ** attempt)


def _requeue_with_backoff(r: redis.Redis, job_id: str, attempt: int) -> None:
    """Sleep the backoff duration, then push the job back onto the main queue."""
    delay = _backoff_seconds(attempt)
    log.info(
        "Job %s failed on attempt %d — retrying in %.0fs (backoff)...",
        job_id, attempt, delay,
    )
    time.sleep(delay)
    r.lpush(QUEUE_KEY, job_id)
    log.info("Job %s re-queued for attempt %d.", job_id, attempt + 1)


def _move_to_dead_letter(r: redis.Redis, job_id: str, error: str) -> None:
    """Mark job as permanently failed and add to dead-letter list."""
    key = f"{JOB_KEY_PREFIX}{job_id}"
    now = datetime.now(timezone.utc).isoformat()
    r.hset(
        key,
        mapping={
            "status": "failed",
            "last_error": error,
            "updated_at": now,
        },
    )
    r.lpush(DEAD_LETTER_KEY, job_id)
    log.warning("Job %s exhausted all retries — moved to dead-letter queue.", job_id)


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_job(r: redis.Redis, job_id: str) -> None:
    key = f"{JOB_KEY_PREFIX}{job_id}"
    now = datetime.now(timezone.utc).isoformat()

    # Fetch job data
    job = r.hgetall(key)
    if not job:
        log.warning("Job %s not found in Redis — skipping", job_id)
        return

    language = job.get("language", "unknown")
    code = job.get("code", "")
    attempt = int(job.get("attempt", "0")) + 1   # increment on each dequeue
    max_attempts = int(job.get("max_attempts", str(MAX_ATTEMPTS)))

    log.info(
        "Processing job %s [language=%s, attempt=%d/%d]",
        job_id, language, attempt, max_attempts,
    )

    # Record attempt count and mark as processing
    r.hset(key, mapping={"status": "processing", "attempt": attempt, "updated_at": now})

    try:
        result = _simulate_execution(language, code)
    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        log.exception("Job %s failed on attempt %d/%d", job_id, attempt, max_attempts)

        # Update last_error regardless of whether we retry or dead-letter
        r.hset(key, mapping={"last_error": error_msg, "updated_at": datetime.now(timezone.utc).isoformat()})

        if attempt < max_attempts:
            r.hset(key, mapping={"status": "pending"})
            _requeue_with_backoff(r, job_id, attempt)
        else:
            _move_to_dead_letter(r, job_id, error_msg)
        return

    # Success
    r.hset(
        key,
        mapping={
            "status": "done",
            "result": result,
            "last_error": "",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    log.info("Job %s completed successfully on attempt %d.", job_id, attempt)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(r: redis.Redis) -> None:
    log.info("Worker started. Listening on queue '%s'...", QUEUE_KEY)

    while True:
        # BRPOP returns (key, value) or None on timeout
        item = r.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT)
        if item is None:
            continue

        _, job_id = item
        log.info("Dequeued job: %s", job_id)

        try:
            process_job(r, job_id)
        except Exception:
            log.exception("Unhandled error for job %s — continuing", job_id)


# ---------------------------------------------------------------------------
# Startup & graceful shutdown
# ---------------------------------------------------------------------------

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Received signal %s — shutting down after current job...", signum)
    _shutdown = True


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info("Connecting to Redis at %s", REDIS_URL)
    r = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

    # Wait for Redis to be ready (Docker startup race)
    for attempt in range(1, 11):
        try:
            r.ping()
            log.info("Redis connection established.")
            break
        except redis.exceptions.ConnectionError:
            log.warning("Redis not ready (attempt %d/10) — retrying in 2s...", attempt)
            time.sleep(2)
    else:
        log.error("Could not connect to Redis after 10 attempts. Exiting.")
        sys.exit(1)

    run(r)

    log.info("Worker shut down cleanly.")


if __name__ == "__main__":
    main()
