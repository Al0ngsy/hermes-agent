"""Skill file synchronisation between HERMES_HOME/skills/ and the artifact backend.

Design
------
Skill files (SKILL.md, supporting scripts, references, assets) are written to
HERMES_HOME/skills/ by skill_manager_tool.py.  In a stateless container without
a PVC those files are lost on pod restart.

This module provides:

  save_skill_file(artifacts, base_skills_dir, file_path)
      Store a single skill file into the artifact backend.
      Called by skill_manager_tool._atomic_write_text at write time.

  save_all_skills(artifacts, skills_dirs)
      Full reconciliation pass — store every file from all skill dirs.
      Used for initial bulk sync and the periodic reconciliation cron job.

  restore_skills_from_backend(artifacts, target_skills_dir)
      Restore all stored skill files from the backend to the filesystem.
      Called at startup so file-scanning skill readers work unchanged.

  register_reconciliation_cron_job(interval_minutes)
      Register a Hermes cron job that runs save_all_skills periodically.
      Idempotent — skips registration if a job with the same ID already exists.

Artifact key scheme
-------------------
  skills/files/{skill_name}/{relative_path_within_skill_dir}

  Example: skills/files/my-workflow/SKILL.md
           skills/files/my-workflow/references/api.md
           skills/files/tools/query_db/query_db.py

Only files under a skill *directory* (i.e. next to or beneath a SKILL.md) are
stored.  Loose SKILL.md files at the root of the skills dir are stored as-is.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from storage.backends import ArtifactStorageBackend

logger = logging.getLogger(__name__)

_ARTIFACT_PREFIX = "skills/files/"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _skill_key(skill_name: str, rel_path: str) -> str:
    """Build the artifact key for one skill file."""
    return f"{_ARTIFACT_PREFIX}{skill_name}/{rel_path}"


def _parse_skill_key(artifact_id: str) -> Optional[Tuple[str, str]]:
    """Return (skill_name, rel_path) from an artifact key, or None if not a skill key."""
    if not artifact_id.startswith(_ARTIFACT_PREFIX):
        return None
    rest = artifact_id[len(_ARTIFACT_PREFIX):]
    parts = rest.split("/", 1)
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def _iter_skill_files(skills_dir: Path):
    """Yield (skill_name, rel_from_skill_dir, absolute_path) for every skill file."""
    if not skills_dir.exists():
        return
    for skill_md in skills_dir.rglob("SKILL.md"):
        skill_dir = skill_md.parent
        rel_to_skills = skill_dir.relative_to(skills_dir)
        skill_name = str(rel_to_skills).replace(os.sep, "/")
        for file_path in skill_dir.rglob("*"):
            if file_path.is_file():
                rel = file_path.relative_to(skill_dir)
                yield skill_name, str(rel).replace(os.sep, "/"), file_path


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def save_skill_file(
    artifact_backend: "ArtifactStorageBackend",
    base_skills_dir: Path,
    file_path: Path,
) -> bool:
    """Store a single skill file into the artifact backend.

    Returns True on success, False on failure (logged as warning).
    No-op if file_path is not under base_skills_dir or has no SKILL.md parent.
    """
    try:
        file_path = Path(file_path)
        base_skills_dir = Path(base_skills_dir)

        if not file_path.is_relative_to(base_skills_dir):
            return False

        rel_to_skills = file_path.relative_to(base_skills_dir)
        parts = rel_to_skills.parts  # e.g. ("my-skill", "SKILL.md")

        # Find the skill directory (the one containing SKILL.md)
        skill_dir = file_path.parent
        while skill_dir != base_skills_dir:
            if (skill_dir / "SKILL.md").exists():
                break
            skill_dir = skill_dir.parent
        else:
            return False  # not inside a skill dir

        skill_name = str(skill_dir.relative_to(base_skills_dir)).replace(os.sep, "/")
        rel = str(file_path.relative_to(skill_dir)).replace(os.sep, "/")
        artifact_id = _skill_key(skill_name, rel)

        data = file_path.read_bytes()
        artifact_backend.write_artifact(artifact_id, data)
        logger.debug("skill_sync: stored %s → %s (%d bytes)", file_path.name, artifact_id, len(data))
        return True
    except Exception as exc:
        logger.warning("skill_sync: failed to store %s: %s", file_path, exc)
        return False


def save_all_skills(
    artifact_backend: "ArtifactStorageBackend",
    skills_dirs: List[Path],
) -> Dict[str, int]:
    """Full reconciliation: store every skill file from all skill dirs to backend.

    Returns {"skills": N, "files": N, "errors": N}.
    """
    skills_seen: set = set()
    files_stored = 0
    errors = 0

    for skills_dir in skills_dirs:
        for skill_name, rel, abs_path in _iter_skill_files(skills_dir):
            artifact_id = _skill_key(skill_name, rel)
            try:
                data = abs_path.read_bytes()
                artifact_backend.write_artifact(artifact_id, data)
                skills_seen.add(skill_name)
                files_stored += 1
            except Exception as exc:
                logger.warning("skill_sync: save_all error %s: %s", artifact_id, exc)
                errors += 1

    logger.info(
        "skill_sync: save_all complete — %d skills, %d files, %d errors",
        len(skills_seen), files_stored, errors,
    )
    return {"skills": len(skills_seen), "files": files_stored, "errors": errors}


def restore_skills_from_backend(
    artifact_backend: "ArtifactStorageBackend",
    target_skills_dir: Path,
) -> Dict[str, int]:
    """Restore all skill files from the backend to the filesystem.

    Existing files are overwritten only if the backend copy differs in size.
    Returns {"skills": N, "files": N, "skipped": N, "errors": N}.
    """
    target_skills_dir = Path(target_skills_dir)
    artifact_ids = artifact_backend.list_artifacts(prefix=_ARTIFACT_PREFIX)

    skills_seen: set = set()
    files_written = 0
    files_skipped = 0
    errors = 0

    for artifact_id in artifact_ids:
        parsed = _parse_skill_key(artifact_id)
        if parsed is None:
            continue
        skill_name, rel = parsed
        target = target_skills_dir / skill_name.replace("/", os.sep) / rel.replace("/", os.sep)

        try:
            data = artifact_backend.read_artifact(artifact_id)
            if data is None:
                continue

            # Skip if file already exists with the same size (avoid unnecessary writes)
            if target.exists() and target.stat().st_size == len(data):
                files_skipped += 1
                skills_seen.add(skill_name)
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            skills_seen.add(skill_name)
            files_written += 1
        except Exception as exc:
            logger.warning("skill_sync: restore error %s → %s: %s", artifact_id, target, exc)
            errors += 1

    logger.info(
        "skill_sync: restore complete — %d skills, %d written, %d skipped, %d errors",
        len(skills_seen), files_written, files_skipped, errors,
    )
    return {
        "skills": len(skills_seen),
        "files": files_written,
        "skipped": files_skipped,
        "errors": errors,
    }


def register_reconciliation_cron_job(interval_minutes: int = 15) -> Optional[str]:
    """Register a Hermes cron job that reconciles skill files to the backend.

    Uses the `script` field to run the sync as a Python one-liner, so no agent
    prompt or LLM call is needed for the reconciliation itself.

    Idempotent — if a job with id ``skill-sync-reconcile`` already exists it
    is updated with the new interval but not duplicated.

    Returns the job_id on success, None on failure.
    """
    _SYNC_JOB_ID = "skill-sync-reconcile"
    _SYNC_SCRIPT = (
        "from storage.skill_sync import save_all_skills; "
        "from agent.skill_utils import get_all_skills_dirs; "
        "from storage.migration import startup_init_backends; "
        "b = startup_init_backends(); "
        "r = save_all_skills(b.artifacts, get_all_skills_dirs()) if b else {'skills':0,'files':0,'errors':0}; "
        "print(f\"Synced {r['skills']} skills ({r['files']} files, {r['errors']} errors) to PostgreSQL\")"
    )

    try:
        from cron import jobs as cron_jobs

        existing = cron_jobs.load_jobs()
        if _SYNC_JOB_ID in existing:
            logger.debug("skill_sync: reconciliation cron job already registered")
            return _SYNC_JOB_ID

        job = cron_jobs.create_job(
            prompt="Skill sync reconciliation complete. Output: {output}",
            schedule=f"every {interval_minutes} minutes",
            name="Skill Sync — PostgreSQL reconciliation",
            deliver="local",
            script=f"python3 -c '{_SYNC_SCRIPT}'",
        )
        # Override the auto-generated id with our stable sentinel
        from cron.jobs import save_jobs
        jobs_dict = cron_jobs.load_jobs()
        job_with_stable_id = {**job, "id": _SYNC_JOB_ID}
        # Remove the original auto-id entry and insert with stable id
        jobs_dict.pop(job["id"], None)
        jobs_dict[_SYNC_JOB_ID] = job_with_stable_id
        save_jobs(jobs_dict)

        logger.info(
            "skill_sync: registered reconciliation cron job every %d minutes", interval_minutes
        )
        return _SYNC_JOB_ID
    except Exception as exc:
        logger.warning("skill_sync: failed to register cron job: %s", exc)
        return None
