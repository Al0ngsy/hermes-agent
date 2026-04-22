"""
Cron job storage and management.

Jobs are stored in ~/.hermes/cron/jobs.json
Output is saved to ~/.hermes/cron/output/{job_id}/{timestamp}.md
"""

import copy
import json
import logging
import tempfile
import threading
import os
import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from hermes_constants import get_hermes_home
from typing import TYPE_CHECKING, Optional, Dict, List, Any

if TYPE_CHECKING:
    from storage.backends import (
        StructuredStateBackend,
        ArtifactStorageBackend,
        DistributedLockBackend,
    )

logger = logging.getLogger(__name__)

from hermes_time import now as _hermes_now

try:
    from croniter import croniter
    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False

# =============================================================================
# Configuration
# =============================================================================

HERMES_DIR = get_hermes_home().resolve()
CRON_DIR = HERMES_DIR / "cron"
JOBS_FILE = CRON_DIR / "jobs.json"

# In-process lock protecting load_jobs→modify→save_jobs cycles for file-based path.
_jobs_file_lock = threading.Lock()
OUTPUT_DIR = CRON_DIR / "output"
ONESHOT_GRACE_SECONDS = 120

# ---------------------------------------------------------------------------
# Storage-backend globals — None means use legacy file-based fallback.
# Call init_storage() early in Hermes startup to enable backend-backed cron.
# ---------------------------------------------------------------------------
_structured: Optional["StructuredStateBackend"] = None
_artifacts: Optional["ArtifactStorageBackend"] = None
_locks: Optional["DistributedLockBackend"] = None


def init_storage(
    structured: "StructuredStateBackend",
    artifacts: "ArtifactStorageBackend",
    locks: "DistributedLockBackend",
) -> None:
    """Wire up storage backends.  Call once at Hermes startup."""
    global _structured, _artifacts, _locks
    _structured, _artifacts, _locks = structured, artifacts, locks


@contextmanager
def _write_lock(job_id: str = "global"):
    """Context manager that provides safe write access to job storage.

    When a DistributedLockBackend is available, acquires a distributed lock
    so concurrent processes don't clobber each other.  Falls back to the
    in-process threading lock for the file-based path.
    """
    if _locks is not None:
        with _locks.lock_context(
            f"cron:write:{job_id}", duration_seconds=30, wait_seconds=10
        ):
            yield
    else:
        with _jobs_file_lock:
            yield


def _normalize_skill_list(skill: Optional[str] = None, skills: Optional[Any] = None) -> List[str]:
    """Normalize legacy/single-skill and multi-skill inputs into a unique ordered list."""
    if skills is None:
        raw_items = [skill] if skill else []
    elif isinstance(skills, str):
        raw_items = [skills]
    else:
        raw_items = list(skills)

    normalized: List[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _apply_skill_fields(job: Dict[str, Any]) -> Dict[str, Any]:
    """Return a job dict with canonical `skills` and legacy `skill` fields aligned."""
    normalized = dict(job)
    skills = _normalize_skill_list(normalized.get("skill"), normalized.get("skills"))
    normalized["skills"] = skills
    normalized["skill"] = skills[0] if skills else None
    return normalized


def _secure_dir(path: Path):
    """Set directory to owner-only access (0700). No-op on Windows."""
    try:
        os.chmod(path, 0o700)
    except (OSError, NotImplementedError):
        pass  # Windows or other platforms where chmod is not supported


def _secure_file(path: Path):
    """Set file to owner-only read/write (0600). No-op on Windows."""
    try:
        if path.exists():
            os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass


def ensure_dirs():
    """Ensure cron directories exist with secure permissions."""
    CRON_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _secure_dir(CRON_DIR)
    _secure_dir(OUTPUT_DIR)


# =============================================================================
# Schedule Parsing
# =============================================================================

def parse_duration(s: str) -> int:
    """
    Parse duration string into minutes.
    
    Examples:
        "30m" → 30
        "2h" → 120
        "1d" → 1440
    """
    s = s.strip().lower()
    match = re.match(r'^(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$', s)
    if not match:
        raise ValueError(f"Invalid duration: '{s}'. Use format like '30m', '2h', or '1d'")
    
    value = int(match.group(1))
    unit = match.group(2)[0]  # First char: m, h, or d
    
    multipliers = {'m': 1, 'h': 60, 'd': 1440}
    return value * multipliers[unit]


def parse_schedule(schedule: str) -> Dict[str, Any]:
    """
    Parse schedule string into structured format.
    
    Returns dict with:
        - kind: "once" | "interval" | "cron"
        - For "once": "run_at" (ISO timestamp)
        - For "interval": "minutes" (int)
        - For "cron": "expr" (cron expression)
    
    Examples:
        "30m"              → once in 30 minutes
        "2h"               → once in 2 hours
        "every 30m"        → recurring every 30 minutes
        "every 2h"         → recurring every 2 hours
        "0 9 * * *"        → cron expression
        "2026-02-03T14:00" → once at timestamp
    """
    schedule = schedule.strip()
    original = schedule
    schedule_lower = schedule.lower()
    
    # "every X" pattern → recurring interval
    if schedule_lower.startswith("every "):
        duration_str = schedule[6:].strip()
        minutes = parse_duration(duration_str)
        return {
            "kind": "interval",
            "minutes": minutes,
            "display": f"every {minutes}m"
        }
    
    # Check for cron expression (5 or 6 space-separated fields)
    # Cron fields: minute hour day month weekday [year]
    parts = schedule.split()
    if len(parts) >= 5 and all(
        re.match(r'^[\d\*\-,/]+$', p) for p in parts[:5]
    ):
        if not HAS_CRONITER:
            raise ValueError("Cron expressions require 'croniter' package. Install with: pip install croniter")
        # Validate cron expression
        try:
            croniter(schedule)
        except Exception as e:
            raise ValueError(f"Invalid cron expression '{schedule}': {e}")
        return {
            "kind": "cron",
            "expr": schedule,
            "display": schedule
        }
    
    # ISO timestamp (contains T or looks like date)
    if 'T' in schedule or re.match(r'^\d{4}-\d{2}-\d{2}', schedule):
        try:
            # Parse and validate
            dt = datetime.fromisoformat(schedule.replace('Z', '+00:00'))
            # Make naive timestamps timezone-aware at parse time so the stored
            # value doesn't depend on the system timezone matching at check time.
            if dt.tzinfo is None:
                dt = dt.astimezone()  # Interpret as local timezone
            return {
                "kind": "once",
                "run_at": dt.isoformat(),
                "display": f"once at {dt.strftime('%Y-%m-%d %H:%M')}"
            }
        except ValueError as e:
            raise ValueError(f"Invalid timestamp '{schedule}': {e}")
    
    # Duration like "30m", "2h", "1d" → one-shot from now
    try:
        minutes = parse_duration(schedule)
        run_at = _hermes_now() + timedelta(minutes=minutes)
        return {
            "kind": "once",
            "run_at": run_at.isoformat(),
            "display": f"once in {original}"
        }
    except ValueError:
        pass
    
    raise ValueError(
        f"Invalid schedule '{original}'. Use:\n"
        f"  - Duration: '30m', '2h', '1d' (one-shot)\n"
        f"  - Interval: 'every 30m', 'every 2h' (recurring)\n"
        f"  - Cron: '0 9 * * *' (cron expression)\n"
        f"  - Timestamp: '2026-02-03T14:00:00' (one-shot at time)"
    )


def _ensure_aware(dt: datetime) -> datetime:
    """Return a timezone-aware datetime in Hermes configured timezone.

    Backward compatibility:
    - Older stored timestamps may be naive.
    - Naive values are interpreted as *system-local wall time* (the timezone
      `datetime.now()` used when they were created), then converted to the
      configured Hermes timezone.

    This preserves relative ordering for legacy naive timestamps across
    timezone changes and avoids false not-due results.
    """
    target_tz = _hermes_now().tzinfo
    if dt.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo
        return dt.replace(tzinfo=local_tz).astimezone(target_tz)
    return dt.astimezone(target_tz)


def _recoverable_oneshot_run_at(
    schedule: Dict[str, Any],
    now: datetime,
    *,
    last_run_at: Optional[str] = None,
) -> Optional[str]:
    """Return a one-shot run time if it is still eligible to fire.

    One-shot jobs get a small grace window so jobs created a few seconds after
    their requested minute still run on the next tick. Once a one-shot has
    already run, it is never eligible again.
    """
    if schedule.get("kind") != "once":
        return None
    if last_run_at:
        return None

    run_at = schedule.get("run_at")
    if not run_at:
        return None

    run_at_dt = _ensure_aware(datetime.fromisoformat(run_at))
    if run_at_dt >= now - timedelta(seconds=ONESHOT_GRACE_SECONDS):
        return run_at
    return None


def _compute_grace_seconds(schedule: dict) -> int:
    """Compute how late a job can be and still catch up instead of fast-forwarding.

    Uses half the schedule period, clamped between 120 seconds and 2 hours.
    This ensures daily jobs can catch up if missed by up to 2 hours,
    while frequent jobs (every 5-10 min) still fast-forward quickly.
    """
    MIN_GRACE = 120
    MAX_GRACE = 7200  # 2 hours

    kind = schedule.get("kind")

    if kind == "interval":
        period_seconds = schedule.get("minutes", 1) * 60
        grace = period_seconds // 2
        return max(MIN_GRACE, min(grace, MAX_GRACE))

    if kind == "cron" and HAS_CRONITER:
        try:
            now = _hermes_now()
            cron = croniter(schedule["expr"], now)
            first = cron.get_next(datetime)
            second = cron.get_next(datetime)
            period_seconds = int((second - first).total_seconds())
            grace = period_seconds // 2
            return max(MIN_GRACE, min(grace, MAX_GRACE))
        except Exception:
            pass

    return MIN_GRACE


def compute_next_run(schedule: Dict[str, Any], last_run_at: Optional[str] = None) -> Optional[str]:
    """
    Compute the next run time for a schedule.

    Returns ISO timestamp string, or None if no more runs.
    """
    now = _hermes_now()

    if schedule["kind"] == "once":
        return _recoverable_oneshot_run_at(schedule, now, last_run_at=last_run_at)

    elif schedule["kind"] == "interval":
        minutes = schedule["minutes"]
        if last_run_at:
            # Next run is last_run + interval
            last = _ensure_aware(datetime.fromisoformat(last_run_at))
            next_run = last + timedelta(minutes=minutes)
        else:
            # First run is now + interval
            next_run = now + timedelta(minutes=minutes)
        return next_run.isoformat()

    elif schedule["kind"] == "cron":
        if not HAS_CRONITER:
            return None
        cron = croniter(schedule["expr"], now)
        next_run = cron.get_next(datetime)
        return next_run.isoformat()

    return None


# =============================================================================
# Job CRUD Operations
# =============================================================================

def load_jobs() -> Dict[str, Any]:
    """Load all jobs from storage.

    Returns a dict keyed by job_id.  Uses the configured storage backend when
    available; falls back to the legacy ``jobs.json`` file otherwise.
    """
    if _structured is not None:
        return _load_jobs_from_backend()
    return _load_jobs_from_file()


def _load_jobs_from_backend() -> Dict[str, Any]:
    index = _structured.get_json("cron:index") or {"ids": []}
    job_ids = index.get("ids", [])
    result: Dict[str, Any] = {}
    for job_id in job_ids:
        job = _structured.get_json(f"cron:jobs:{job_id}")
        if job is not None:
            result[job_id] = job
    return result


def _load_jobs_from_file() -> Dict[str, Any]:
    """Load legacy jobs.json and return as a dict keyed by job_id."""
    ensure_dirs()
    if not JOBS_FILE.exists():
        return {}

    try:
        with open(JOBS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            jobs_list = data.get("jobs", [])
            return {j["id"]: j for j in jobs_list if "id" in j}
    except json.JSONDecodeError:
        try:
            with open(JOBS_FILE, 'r', encoding='utf-8') as f:
                data = json.loads(f.read(), strict=False)
                jobs_list = data.get("jobs", [])
                jobs_dict = {j["id"]: j for j in jobs_list if "id" in j}
                if jobs_dict:
                    save_jobs(jobs_dict)
                    logger.warning("Auto-repaired jobs.json (had invalid control characters)")
                return jobs_dict
        except Exception as e:
            logger.error("Failed to auto-repair jobs.json: %s", e)
            raise RuntimeError(f"Cron database corrupted and unrepairable: {e}") from e
    except IOError as e:
        logger.error("IOError reading jobs.json: %s", e)
        raise RuntimeError(f"Failed to read cron database: {e}") from e


def save_jobs(jobs: Dict[str, Any]) -> None:
    """Persist all jobs.

    Accepts a dict keyed by job_id.  Writes to the storage backend when
    available; falls back to the legacy ``jobs.json`` file otherwise.
    """
    if _structured is not None:
        _save_jobs_to_backend(jobs)
    else:
        _save_jobs_to_file(jobs)


def _save_jobs_to_backend(jobs: Dict[str, Any]) -> None:
    new_ids = list(jobs.keys())
    _structured.set_json(
        "cron:index",
        {"ids": new_ids, "updated_at": _hermes_now().isoformat()},
    )
    # Remove jobs that no longer exist.
    existing_keys = set(_structured.list_keys("cron:jobs:"))
    new_keys = {f"cron:jobs:{jid}" for jid in new_ids}
    for key in existing_keys - new_keys:
        _structured.delete_json(key)
    for job_id, job in jobs.items():
        _structured.set_json(f"cron:jobs:{job_id}", job)


def _save_jobs_to_file(jobs: Dict[str, Any]) -> None:
    """Serialize dict to the legacy list format and atomically write jobs.json."""
    ensure_dirs()
    jobs_list = list(jobs.values())
    fd, tmp_path = tempfile.mkstemp(dir=str(JOBS_FILE.parent), suffix='.tmp', prefix='.jobs_')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump({"jobs": jobs_list, "updated_at": _hermes_now().isoformat()}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, JOBS_FILE)
        _secure_file(JOBS_FILE)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def create_job(
    prompt: str,
    schedule: str,
    name: Optional[str] = None,
    repeat: Optional[int] = None,
    deliver: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
    skill: Optional[str] = None,
    skills: Optional[List[str]] = None,
    model: Optional[str] = None,
    provider: Optional[str] = None,
    base_url: Optional[str] = None,
    script: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new cron job.

    Args:
        prompt: The prompt to run (must be self-contained, or a task instruction when skill is set)
        schedule: Schedule string (see parse_schedule)
        name: Optional friendly name
        repeat: How many times to run (None = forever, 1 = once)
        deliver: Where to deliver output ("origin", "local", "telegram", etc.)
        origin: Source info where job was created (for "origin" delivery)
        skill: Optional legacy single skill name to load before running the prompt
        skills: Optional ordered list of skills to load before running the prompt
        model: Optional per-job model override
        provider: Optional per-job provider override
        base_url: Optional per-job base URL override
        script: Optional path to a Python script whose stdout is injected into the
                prompt each run.  The script runs before the agent turn, and its output
                is prepended as context.  Useful for data collection / change detection.

    Returns:
        The created job dict
    """
    parsed_schedule = parse_schedule(schedule)

    # Normalize repeat: treat 0 or negative values as None (infinite)
    if repeat is not None and repeat <= 0:
        repeat = None

    # Auto-set repeat=1 for one-shot schedules if not specified
    if parsed_schedule["kind"] == "once" and repeat is None:
        repeat = 1

    # Default delivery to origin if available, otherwise local
    if deliver is None:
        deliver = "origin" if origin else "local"

    job_id = uuid.uuid4().hex[:12]
    now = _hermes_now().isoformat()

    normalized_skills = _normalize_skill_list(skill, skills)
    normalized_model = str(model).strip() if isinstance(model, str) else None
    normalized_provider = str(provider).strip() if isinstance(provider, str) else None
    normalized_base_url = str(base_url).strip().rstrip("/") if isinstance(base_url, str) else None
    normalized_model = normalized_model or None
    normalized_provider = normalized_provider or None
    normalized_base_url = normalized_base_url or None
    normalized_script = str(script).strip() if isinstance(script, str) else None
    normalized_script = normalized_script or None

    label_source = (prompt or (normalized_skills[0] if normalized_skills else None)) or "cron job"
    job = {
        "id": job_id,
        "name": name or label_source[:50].strip(),
        "prompt": prompt,
        "skills": normalized_skills,
        "skill": normalized_skills[0] if normalized_skills else None,
        "model": normalized_model,
        "provider": normalized_provider,
        "base_url": normalized_base_url,
        "script": normalized_script,
        "schedule": parsed_schedule,
        "schedule_display": parsed_schedule.get("display", schedule),
        "repeat": {
            "times": repeat,  # None = forever
            "completed": 0
        },
        "enabled": True,
        "state": "scheduled",
        "paused_at": None,
        "paused_reason": None,
        "created_at": now,
        "next_run_at": compute_next_run(parsed_schedule),
        "last_run_at": None,
        "last_status": None,
        "last_error": None,
        "last_delivery_error": None,
        # Delivery configuration
        "deliver": deliver,
        "origin": origin,  # Tracks where job was created for "origin" delivery
    }

    jobs = load_jobs()
    jobs[job_id] = job
    save_jobs(jobs)

    return job


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get a job by ID."""
    job = load_jobs().get(job_id)
    return _apply_skill_fields(job) if job is not None else None


def list_jobs(include_disabled: bool = False) -> List[Dict[str, Any]]:
    """List all jobs, optionally including disabled ones."""
    jobs = [_apply_skill_fields(j) for j in load_jobs().values()]
    if not include_disabled:
        jobs = [j for j in jobs if j.get("enabled", True)]
    return jobs


def update_job(job_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Update a job by ID, refreshing derived schedule fields when needed."""
    jobs = load_jobs()
    job = jobs.get(job_id)
    if job is None:
        return None

    updated = _apply_skill_fields({**job, **updates})
    schedule_changed = "schedule" in updates

    if "skills" in updates or "skill" in updates:
        normalized_skills = _normalize_skill_list(updated.get("skill"), updated.get("skills"))
        updated["skills"] = normalized_skills
        updated["skill"] = normalized_skills[0] if normalized_skills else None

    if schedule_changed:
        updated_schedule = updated["schedule"]
        if isinstance(updated_schedule, str):
            updated_schedule = parse_schedule(updated_schedule)
            updated["schedule"] = updated_schedule
        updated["schedule_display"] = updates.get(
            "schedule_display",
            updated_schedule.get("display", updated.get("schedule_display")),
        )
        if updated.get("state") != "paused":
            updated["next_run_at"] = compute_next_run(updated_schedule)

    if updated.get("enabled", True) and updated.get("state") != "paused" and not updated.get("next_run_at"):
        updated["next_run_at"] = compute_next_run(updated["schedule"])

    jobs[job_id] = updated
    save_jobs(jobs)
    return _apply_skill_fields(jobs[job_id])


def pause_job(job_id: str, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Pause a job without deleting it."""
    return update_job(
        job_id,
        {
            "enabled": False,
            "state": "paused",
            "paused_at": _hermes_now().isoformat(),
            "paused_reason": reason,
        },
    )


def resume_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Resume a paused job and compute the next future run from now."""
    job = get_job(job_id)
    if not job:
        return None

    next_run_at = compute_next_run(job["schedule"])
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": next_run_at,
        },
    )


def trigger_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Schedule a job to run on the next scheduler tick."""
    job = get_job(job_id)
    if not job:
        return None
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": _hermes_now().isoformat(),
        },
    )


def remove_job(job_id: str) -> bool:
    """Remove a job by ID."""
    jobs = load_jobs()
    if job_id not in jobs:
        return False
    del jobs[job_id]
    save_jobs(jobs)
    return True


def mark_job_run(job_id: str, success: bool, error: Optional[str] = None,
                 delivery_error: Optional[str] = None):
    """
    Mark a job as having been run.
    
    Updates last_run_at, last_status, increments completed count,
    computes next_run_at, and auto-deletes if repeat limit reached.

    ``delivery_error`` is tracked separately from the agent error — a job
    can succeed (agent produced output) but fail delivery (platform down).
    """
    with _write_lock(job_id):
        jobs = load_jobs()
        job = jobs.get(job_id)
        if job is None:
            logger.warning("mark_job_run: job_id %s not found, skipping save", job_id)
            return

        now = _hermes_now().isoformat()
        job["last_run_at"] = now
        job["last_status"] = "ok" if success else "error"
        job["last_error"] = error if not success else None
        # Track delivery failures separately — cleared on successful delivery
        job["last_delivery_error"] = delivery_error
        
        # Increment completed count
        if job.get("repeat"):
            job["repeat"]["completed"] = job["repeat"].get("completed", 0) + 1
            
            # Check if we've hit the repeat limit
            times = job["repeat"].get("times")
            completed = job["repeat"]["completed"]
            if times is not None and times > 0 and completed >= times:
                del jobs[job_id]
                save_jobs(jobs)
                return
        
        # Compute next run
        job["next_run_at"] = compute_next_run(job["schedule"], now)

        # If no next run (one-shot completed), disable
        if job["next_run_at"] is None:
            job["enabled"] = False
            job["state"] = "completed"
        elif job.get("state") != "paused":
            job["state"] = "scheduled"

        jobs[job_id] = job
        save_jobs(jobs)


def advance_next_run(job_id: str) -> bool:
    """Preemptively advance next_run_at for a recurring job before execution.

    Call this BEFORE run_job() so that if the process crashes mid-execution,
    the job won't re-fire on the next gateway restart.  This converts the
    scheduler from at-least-once to at-most-once for recurring jobs — missing
    one run is far better than firing dozens of times in a crash loop.

    One-shot jobs are left unchanged so they can still retry on restart.

    When a DistributedLockBackend is configured, also acquires
    ``cron:claim:{job_id}`` so only one instance claims each run.

    Returns True if next_run_at was advanced, False otherwise.
    """
    with _write_lock(job_id):
        # Try to claim this run via distributed lock (no-wait).
        claim_handle = None
        if _locks is not None:
            claim_handle = _locks.acquire_lock(
                f"cron:claim:{job_id}", duration_seconds=3600, wait_seconds=0
            )
            if claim_handle is None:
                logger.debug("advance_next_run: job %s already claimed by another instance", job_id)
                return False

        jobs = load_jobs()
        job = jobs.get(job_id)
        if job is None:
            if claim_handle is not None and _locks is not None:
                _locks.release_lock(claim_handle.lock_id)
            return False

        kind = job.get("schedule", {}).get("kind")
        if kind not in ("cron", "interval"):
            # One-shot — don't advance; release claim so it isn't held forever.
            if claim_handle is not None and _locks is not None:
                _locks.release_lock(claim_handle.lock_id)
            return False

        now = _hermes_now().isoformat()
        new_next = compute_next_run(job["schedule"], now)
        if new_next and new_next != job.get("next_run_at"):
            job["next_run_at"] = new_next
            jobs[job_id] = job
            save_jobs(jobs)
            return True
        return False


def get_due_jobs() -> List[Dict[str, Any]]:
    """Get all jobs that are due to run now.

    For recurring jobs (cron/interval), if the scheduled time is stale
    (more than one period in the past, e.g. because the gateway was down),
    the job is fast-forwarded to the next future run instead of firing
    immediately.  This prevents a burst of missed jobs on gateway restart.
    """
    now = _hermes_now()
    raw_jobs = load_jobs()  # Dict[job_id, job_dict]
    # Deep-copy so _apply_skill_fields doesn't mutate raw_jobs (needed for save).
    jobs = [_apply_skill_fields(j) for j in copy.deepcopy(raw_jobs).values()]
    due = []
    needs_save = False

    for job in jobs:
        if not job.get("enabled", True):
            continue

        job_id = job["id"]
        next_run = job.get("next_run_at")
        if not next_run:
            recovered_next = _recoverable_oneshot_run_at(
                job.get("schedule", {}),
                now,
                last_run_at=job.get("last_run_at"),
            )
            if not recovered_next:
                continue

            job["next_run_at"] = recovered_next
            next_run = recovered_next
            logger.info(
                "Job '%s' had no next_run_at; recovering one-shot run at %s",
                job.get("name", job_id),
                recovered_next,
            )
            if job_id in raw_jobs:
                raw_jobs[job_id]["next_run_at"] = recovered_next
                needs_save = True

        next_run_dt = _ensure_aware(datetime.fromisoformat(next_run))
        if next_run_dt <= now:
            schedule = job.get("schedule", {})
            kind = schedule.get("kind")

            # For recurring jobs, check if the scheduled time is stale
            # (gateway was down and missed the window). Fast-forward to
            # the next future occurrence instead of firing a stale run.
            grace = _compute_grace_seconds(schedule)
            if kind in ("cron", "interval") and (now - next_run_dt).total_seconds() > grace:
                new_next = compute_next_run(schedule, now.isoformat())
                if new_next:
                    logger.info(
                        "Job '%s' missed its scheduled time (%s, grace=%ds). "
                        "Fast-forwarding to next run: %s",
                        job.get("name", job_id),
                        next_run,
                        grace,
                        new_next,
                    )
                    if job_id in raw_jobs:
                        raw_jobs[job_id]["next_run_at"] = new_next
                        needs_save = True
                    continue  # Skip this run

            due.append(job)

    if needs_save:
        save_jobs(raw_jobs)

    return due


def save_job_output(job_id: str, output: str):
    """Save job output.

    When an ArtifactStorageBackend is configured, stores the output as an
    artifact and returns the artifact ID string.  Otherwise writes to the
    legacy filesystem path and returns the ``Path`` object.
    """
    if _artifacts is not None:
        timestamp = _hermes_now().strftime("%Y-%m-%d_%H-%M-%S")
        artifact_id = f"cron:output:{job_id}:{timestamp}"
        _artifacts.write_artifact(artifact_id, output.encode("utf-8"))
        return artifact_id

    # Legacy file-based path
    ensure_dirs()
    job_output_dir = OUTPUT_DIR / job_id
    job_output_dir.mkdir(parents=True, exist_ok=True)
    _secure_dir(job_output_dir)
    
    timestamp = _hermes_now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = job_output_dir / f"{timestamp}.md"
    
    fd, tmp_path = tempfile.mkstemp(dir=str(job_output_dir), suffix='.tmp', prefix='.output_')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(output)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, output_file)
        _secure_file(output_file)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    
    return output_file


def get_job_output(job_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Retrieve recent output entries for a job.

    Returns a list of dicts with ``timestamp`` and ``content`` keys (plus
    ``artifact_id`` when using the backend), sorted newest-first.
    """
    if _artifacts is not None:
        prefix = f"cron:output:{job_id}:"
        artifact_ids = sorted(_artifacts.list_artifacts(prefix), reverse=True)[:limit]
        results = []
        for artifact_id in artifact_ids:
            data = _artifacts.read_artifact(artifact_id)
            if data is not None:
                timestamp = artifact_id[len(prefix):]
                results.append({
                    "artifact_id": artifact_id,
                    "timestamp": timestamp,
                    "content": data.decode("utf-8", errors="replace"),
                })
        return results

    # Legacy file-based path
    job_output_dir = OUTPUT_DIR / job_id
    if not job_output_dir.exists():
        return []
    files = sorted(job_output_dir.glob("*.md"), reverse=True)[:limit]
    results = []
    for f in files:
        try:
            content = f.read_text(encoding="utf-8")
        except OSError:
            continue
        results.append({"timestamp": f.stem, "content": content})
    return results


def add_job(job_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add a pre-built job dict directly to storage.

    Unlike ``create_job()``, this accepts a complete job dict (must include
    an ``id`` field) and stores it as-is.  Useful for migrations and tests.
    """
    if "id" not in job_dict:
        raise ValueError("job_dict must contain an 'id' field")
    jobs = load_jobs()
    jobs[job_dict["id"]] = job_dict
    save_jobs(jobs)
    return job_dict
