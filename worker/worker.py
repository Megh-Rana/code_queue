"""
Worker process — pulls jobs from the Redis queue and simulates execution.

Queue strategy: Redis List with BRPOP (blocking right-pop).
  - API pushes job IDs to the left  (LPUSH job_queue <id>)
  - Worker pops from the right       (BRPOP job_queue)
  → FIFO order

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
JOB_KEY_PREFIX = "job:"
BRPOP_TIMEOUT = 5  # seconds — unblocks periodically so shutdown signal is checked

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
# Core processing loop
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

    log.info("Processing job %s [language=%s]", job_id, language)

    # Mark as processing
    r.hset(key, mapping={"status": "processing", "updated_at": now})

    try:
        result = _simulate_execution(language, code)
        status = "done"
    except Exception as exc:
        log.exception("Unexpected error processing job %s", job_id)
        result = f"Internal worker error: {exc}"
        status = "error"

    # Write result
    r.hset(
        key,
        mapping={
            "status": status,
            "result": result,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    log.info("Job %s finished with status=%s", job_id, status)


def run(r: redis.Redis) -> None:
    log.info("Worker started. Listening on queue '%s'...", QUEUE_KEY)

    while True:
        # BRPOP returns (key, value) or None on timeout
        item = r.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT)
        if item is None:
            # Timeout — loop back so we can check for shutdown signals
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
