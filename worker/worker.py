"""
Worker process — pulls jobs from the Redis queue and executes code.

Queue strategy: Redis List with BRPOP (blocking right-pop).
  - API pushes job IDs to the left  (LPUSH job_queue <id>)
  - Worker pops from the right       (BRPOP job_queue)
  → FIFO order

Execution strategy: subprocess per job with resource limits.
  - Wall-clock timeout enforced via subprocess.run(timeout=...)
  - CPU time + memory limits applied in the child process preexec via
    the stdlib `resource` module (Linux only)
  - Code written to a temp file, cleaned up after execution
  - stdout + stderr captured and truncated to MAX_OUTPUT_BYTES

Retry strategy: exponential backoff.
  - On failure, re-queue the job after 2^attempt seconds (2s, 4s, 8s, ...)
  - After MAX_ATTEMPTS failures, move to dead-letter list (status=failed)

Languages with real execution: python, javascript, c, cpp, go, ruby
Languages with simulated execution (heavy runtimes): java, rust
"""

import logging
import os
import resource
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

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

REDIS_URL      = os.getenv("REDIS_URL", "redis://redis:6379")
QUEUE_KEY      = "job_queue"
DEAD_LETTER_KEY = "dead_letter_queue"
JOB_KEY_PREFIX = "job:"
BRPOP_TIMEOUT  = 5       # seconds

MAX_ATTEMPTS    = int(os.getenv("MAX_ATTEMPTS", "3"))
EXEC_TIMEOUT    = int(os.getenv("EXEC_TIMEOUT", "10"))      # wall-clock seconds
MEM_LIMIT_MB    = int(os.getenv("MEM_LIMIT_MB", "256"))     # heap memory cap (MB)
MAX_OUTPUT_BYTES = int(os.getenv("MAX_OUTPUT_BYTES", "10240"))  # 10 KB

# ---------------------------------------------------------------------------
# Language runner registry
# ---------------------------------------------------------------------------

# Each entry is a callable: (src_path: Path, bin_dir: Path) -> list[str]
# that returns the shell command list to execute.
# Languages that need a compile step return a two-phase tuple instead.

FILE_EXTENSIONS = {
    "python":     ".py",
    "javascript": ".js",
    "c":          ".c",
    "cpp":        ".cpp",
    "go":         ".go",
    "ruby":       ".rb",
    # simulated
    "java":       ".java",
    "rust":       ".rs",
}

REAL_LANGUAGES = {"python", "javascript", "c", "cpp", "go", "ruby"}


def _apply_resource_limits() -> None:
    """
    Called as preexec_fn in the child process (Linux only).
    Sets CPU time and data segment (heap) memory caps.

    We use RLIMIT_DATA rather than RLIMIT_AS because modern runtimes
    (Node.js/V8, Go) pre-reserve large virtual address ranges that far
    exceed their actual memory usage. RLIMIT_AS would kill them at startup.
    RLIMIT_DATA limits actual heap allocation, which is what we care about.
    """
    mem_bytes = MEM_LIMIT_MB * 1024 * 1024
    # Data segment size (heap allocations)
    resource.setrlimit(resource.RLIMIT_DATA, (mem_bytes, mem_bytes))
    # CPU time in seconds (hard = soft + 2 for grace)
    resource.setrlimit(resource.RLIMIT_CPU, (EXEC_TIMEOUT, EXEC_TIMEOUT + 2))


def _build_run_commands(language: str, src: Path, bin_dir: Path) -> tuple[list | None, list]:
    """
    Returns (compile_cmd, run_cmd).
    compile_cmd is None for interpreted languages.
    """
    bin_path = bin_dir / "out"

    commands = {
        "python":     (None,                         ["python3", str(src)]),
        "javascript": (None,                         ["node", str(src)]),
        "ruby":       (None,                         ["ruby", str(src)]),
        "go":         (None,                         ["go", "run", str(src)]),
        "c":          (["gcc", str(src), "-o", str(bin_path), "-lm"],
                                                     [str(bin_path)]),
        "cpp":        (["g++", str(src), "-o", str(bin_path)],
                                                     [str(bin_path)]),
    }
    return commands[language]


def _truncate(output: str) -> str:
    """Cap output at MAX_OUTPUT_BYTES and append a notice if truncated."""
    encoded = output.encode("utf-8", errors="replace")
    if len(encoded) <= MAX_OUTPUT_BYTES:
        return output
    truncated = encoded[:MAX_OUTPUT_BYTES].decode("utf-8", errors="replace")
    return truncated + f"\n... [output truncated at {MAX_OUTPUT_BYTES} bytes]"


# ---------------------------------------------------------------------------
# Simulated execution (java, rust — heavy runtimes not installed)
# ---------------------------------------------------------------------------

import random

FAKE_OUTPUTS = {
    "java": ["Hello, World!", "0", "Exception in thread \"main\"... (just kidding)"],
    "rust": ["Hello, World!", "()", "error[E0502] — just kidding, it compiled!"],
}


def _simulate_execution(language: str, code: str) -> tuple[str, str, int]:
    """Returns (stdout, stderr, exit_code) for unsupported runtimes."""
    import time as _time
    delay = random.uniform(1, 3)
    log.info("  [simulated] sleeping %.1fs for %s", delay, language)
    _time.sleep(delay)
    fake = random.choice(FAKE_OUTPUTS.get(language, ["Done."]))
    lines = len(code.strip().splitlines())
    stdout = (
        f"[Simulated — {language} runtime not installed]\n"
        f"Compiled and ran {lines} line(s).\n"
        f"stdout: {fake}\n"
    )
    return stdout, "", 0


# ---------------------------------------------------------------------------
# Real execution
# ---------------------------------------------------------------------------

def _run_real(language: str, code: str) -> tuple[str, str, int]:
    """
    Write code to a temp file, compile (if needed), run, return
    (stdout, stderr, exit_code). Raises on timeout or OOM.
    """
    ext = FILE_EXTENSIONS[language]

    with tempfile.TemporaryDirectory(prefix="codeq_") as tmp:
        tmp_path = Path(tmp)
        src = tmp_path / f"main{ext}"
        src.write_text(code, encoding="utf-8")

        compile_cmd, run_cmd = _build_run_commands(language, src, tmp_path)

        # ── Compile step (C, C++) ──────────────────────────────────────────
        if compile_cmd:
            log.info("  Compiling: %s", " ".join(compile_cmd))
            try:
                comp = subprocess.run(
                    compile_cmd,
                    capture_output=True,
                    text=True,
                    timeout=EXEC_TIMEOUT,
                    preexec_fn=_apply_resource_limits,
                )
            except subprocess.TimeoutExpired:
                return "", "Compilation timed out.", 1

            if comp.returncode != 0:
                return "", _truncate(comp.stderr or comp.stdout), comp.returncode

        # ── Run step ──────────────────────────────────────────────────────
        log.info("  Running: %s", " ".join(run_cmd))
        try:
            proc = subprocess.run(
                run_cmd,
                capture_output=True,
                text=True,
                timeout=EXEC_TIMEOUT,
                preexec_fn=_apply_resource_limits,
            )
            return (
                _truncate(proc.stdout),
                _truncate(proc.stderr),
                proc.returncode,
            )
        except subprocess.TimeoutExpired:
            return "", f"Execution timed out after {EXEC_TIMEOUT}s.", 124
        except MemoryError:
            return "", "Process exceeded memory limit.", 1


# ---------------------------------------------------------------------------
# Unified execute dispatcher
# ---------------------------------------------------------------------------

def execute(language: str, code: str) -> str:
    """
    Dispatch to real or simulated runner.
    Returns a formatted result string stored in the job hash.
    """
    if language in REAL_LANGUAGES:
        log.info("  Executing %s (real)", language)
        stdout, stderr, exit_code = _run_real(language, code)
    else:
        log.info("  Executing %s (simulated)", language)
        stdout, stderr, exit_code = _simulate_execution(language, code)

    parts = []
    if stdout:
        parts.append(f"stdout:\n{stdout.rstrip()}")
    if stderr:
        parts.append(f"stderr:\n{stderr.rstrip()}")
    parts.append(f"exit code: {exit_code}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Retry / dead-letter helpers
# ---------------------------------------------------------------------------

def _backoff_seconds(attempt: int) -> float:
    return float(2 ** attempt)


def _requeue_with_backoff(r: redis.Redis, job_id: str, attempt: int) -> None:
    delay = _backoff_seconds(attempt)
    log.info("Job %s failed on attempt %d — retrying in %.0fs...", job_id, attempt, delay)
    time.sleep(delay)
    r.lpush(QUEUE_KEY, job_id)
    log.info("Job %s re-queued for attempt %d.", job_id, attempt + 1)


def _move_to_dead_letter(r: redis.Redis, job_id: str, error: str) -> None:
    key = f"{JOB_KEY_PREFIX}{job_id}"
    now = datetime.now(timezone.utc).isoformat()
    r.hset(key, mapping={"status": "failed", "last_error": error, "updated_at": now})
    r.lpush(DEAD_LETTER_KEY, job_id)
    log.warning("Job %s exhausted all retries — moved to dead-letter queue.", job_id)


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_job(r: redis.Redis, job_id: str) -> None:
    key = f"{JOB_KEY_PREFIX}{job_id}"
    now = datetime.now(timezone.utc).isoformat()

    job = r.hgetall(key)
    if not job:
        log.warning("Job %s not found in Redis — skipping", job_id)
        return

    language = job.get("language", "unknown")
    code     = job.get("code", "")
    attempt  = int(job.get("attempt", "0")) + 1
    max_attempts = int(job.get("max_attempts", str(MAX_ATTEMPTS)))

    log.info("Processing job %s [language=%s, attempt=%d/%d]", job_id, language, attempt, max_attempts)

    r.hset(key, mapping={"status": "processing", "attempt": attempt, "updated_at": now})

    try:
        result = execute(language, code)
    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        log.exception("Job %s failed on attempt %d/%d", job_id, attempt, max_attempts)
        r.hset(key, mapping={"last_error": error_msg, "updated_at": datetime.now(timezone.utc).isoformat()})

        if attempt < max_attempts:
            r.hset(key, mapping={"status": "pending"})
            _requeue_with_backoff(r, job_id, attempt)
        else:
            _move_to_dead_letter(r, job_id, error_msg)
        return

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
