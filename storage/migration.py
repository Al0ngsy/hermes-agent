"""Migration and validation tooling for Hermes stateless operation.

Provides:
- HermesMigration: imports HERMES_HOME directory into storage backends
- MigrationResult: dataclass for per-section migration outcome
- validate_parity: compare file state vs backend state
- migrate_hermes_home: top-level CLI/startup migration function
- startup_init_backends: wire all Hermes modules to storage backends at startup
"""

import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .backends import StorageBackendSet

logger = logging.getLogger(__name__)

# Guard against double-wiring in startup_init_backends
_backends_initialized: bool = False


# =============================================================================
# MigrationResult
# =============================================================================


@dataclass
class MigrationResult:
    """Outcome of a single migration section."""

    section: str
    success: bool
    items_imported: int = 0
    items_skipped: int = 0
    errors: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        status = "✓" if self.success else "✗"
        return (
            f"{status} {self.section}: {self.items_imported} imported, "
            f"{self.items_skipped} skipped, {len(self.errors)} errors"
        )


# =============================================================================
# HermesMigration
# =============================================================================


class HermesMigration:
    """
    Imports an existing HERMES_HOME directory into the storage backends.

    Supports:
    - Full import (initial migration)
    - Validation (compare file state vs backend state)
    - Dual-write mode (write to both file and backend during rollout)
    """

    def __init__(self, hermes_home: Path, backends: StorageBackendSet):
        self.hermes_home = hermes_home
        self.backends = backends
        self.logger = logging.getLogger(__name__)

    # -------------------------------------------------------------------------
    # Sessions
    # -------------------------------------------------------------------------

    def import_sessions(self) -> MigrationResult:
        """Read HERMES_HOME/state.db (SQLite) and import all sessions + messages
        into StructuredStateBackend.

        Key scheme:
          sessions:meta:{id}          — full session metadata dict
          sessions:index:{id}         — lightweight index entry
          sessions:msgs:{id}:{seq:010d} — individual message records
        """
        section = "sessions"
        db_path = self.hermes_home / "state.db"

        if not db_path.exists():
            return MigrationResult(
                section=section, success=True,
                items_skipped=1,
            )

        imported = 0
        skipped = 0
        errors: List[str] = []

        try:
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")

            try:
                sessions = conn.execute(
                    "SELECT * FROM sessions ORDER BY started_at"
                ).fetchall()
            except sqlite3.OperationalError as e:
                return MigrationResult(
                    section=section, success=False,
                    errors=[f"Cannot read sessions table: {e}"],
                )

            structured = self.backends.structured

            for row in sessions:
                session_id = row["id"]
                meta_key = f"sessions:meta:{session_id}"

                try:
                    # Build meta dict from all columns
                    meta: Dict[str, Any] = dict(row)
                    # Deserialize model_config if stored as JSON string
                    if meta.get("model_config") and isinstance(meta["model_config"], str):
                        try:
                            meta["model_config"] = json.loads(meta["model_config"])
                        except (json.JSONDecodeError, TypeError):
                            pass

                    # Skip if already present in backend
                    if structured.get_json(meta_key) is not None:
                        skipped += 1
                        continue

                    structured.set_json(meta_key, meta)
                    structured.set_json(
                        f"sessions:index:{session_id}",
                        {
                            "id": session_id,
                            "source": meta.get("source"),
                            "started_at": meta.get("started_at"),
                            "title": meta.get("title"),
                            "parent_session_id": meta.get("parent_session_id"),
                        },
                    )

                    # Import messages for this session
                    try:
                        msgs = conn.execute(
                            "SELECT * FROM messages WHERE session_id = ? ORDER BY timestamp, id",
                            (session_id,),
                        ).fetchall()
                        for seq, msg_row in enumerate(msgs):
                            msg_dict: Dict[str, Any] = dict(msg_row)
                            msg_key = f"sessions:msgs:{session_id}:{seq:010d}"
                            structured.set_json(msg_key, msg_dict)
                    except sqlite3.OperationalError as me:
                        errors.append(f"Session {session_id} messages: {me}")

                    imported += 1
                    self.logger.debug("Imported session %s", session_id)

                except Exception as e:
                    errors.append(f"Session {session_id}: {e}")

        except Exception as e:
            return MigrationResult(
                section=section, success=False,
                errors=[f"Failed to open state.db: {e}"],
            )
        finally:
            try:
                conn.close()
            except Exception:
                pass

        self.logger.info("Sessions: imported=%d skipped=%d errors=%d", imported, skipped, len(errors))
        return MigrationResult(
            section=section,
            success=len(errors) == 0,
            items_imported=imported,
            items_skipped=skipped,
            errors=errors,
        )

    # -------------------------------------------------------------------------
    # Cron Jobs
    # -------------------------------------------------------------------------

    def import_cron_jobs(self) -> MigrationResult:
        """Import cron jobs from HERMES_HOME/cron/jobs.json and output artifacts."""
        section = "cron"
        jobs_file = self.hermes_home / "cron" / "jobs.json"

        imported = 0
        skipped = 0
        errors: List[str] = []

        if jobs_file.exists():
            try:
                raw = jobs_file.read_text(encoding="utf-8")
                raw_data = json.loads(raw)
                # jobs.json format: {"jobs": [...], "updated_at": ...}
                if isinstance(raw_data, dict):
                    jobs_list: List[Dict[str, Any]] = raw_data.get("jobs", [])
                elif isinstance(raw_data, list):
                    jobs_list = raw_data
                else:
                    jobs_list = []

                structured = self.backends.structured

                for job in jobs_list:
                    job_id = job.get("id") or job.get("job_id")
                    if not job_id:
                        errors.append(f"Job missing id: {job!r}")
                        continue

                    key = f"cron:jobs:{job_id}"
                    if structured.get_json(key) is not None:
                        skipped += 1
                        continue

                    try:
                        structured.set_json(key, job)
                        imported += 1
                        self.logger.debug("Imported cron job %s", job_id)
                    except Exception as e:
                        errors.append(f"Job {job_id}: {e}")

            except (json.JSONDecodeError, OSError) as e:
                errors.append(f"Failed to read jobs.json: {e}")

        # Import cron output files as artifacts
        output_dir = self.hermes_home / "cron" / "output"
        artifacts_imported = 0
        if output_dir.exists():
            for job_dir in output_dir.iterdir():
                if not job_dir.is_dir():
                    continue
                job_id = job_dir.name
                for output_file in sorted(job_dir.glob("*.md")):
                    artifact_id = f"cron:output:{job_id}:{output_file.stem}"
                    try:
                        existing = self.backends.artifacts.read_artifact(artifact_id)
                        if existing is not None:
                            skipped += 1
                            continue
                        data = output_file.read_bytes()
                        self.backends.artifacts.write_artifact(artifact_id, data)
                        artifacts_imported += 1
                    except Exception as e:
                        errors.append(f"Artifact {artifact_id}: {e}")

        total_imported = imported + artifacts_imported
        self.logger.info(
            "Cron: jobs=%d artifacts=%d skipped=%d errors=%d",
            imported, artifacts_imported, skipped, len(errors),
        )
        return MigrationResult(
            section=section,
            success=len(errors) == 0,
            items_imported=total_imported,
            items_skipped=skipped,
            errors=errors,
        )

    # -------------------------------------------------------------------------
    # Config
    # -------------------------------------------------------------------------

    def import_config(self) -> MigrationResult:
        """Import config.yaml and SOUL.md into the structured backend."""
        section = "config"
        imported = 0
        skipped = 0
        errors: List[str] = []

        structured = self.backends.structured

        # config.yaml
        config_file = self.hermes_home / "config.yaml"
        if config_file.exists():
            if structured.get_json("config:main") is not None:
                skipped += 1
            else:
                try:
                    content = config_file.read_text(encoding="utf-8")
                    structured.set_json("config:main", {"content": content, "format": "yaml"})
                    imported += 1
                    self.logger.debug("Imported config.yaml")
                except Exception as e:
                    errors.append(f"config.yaml: {e}")

        # SOUL.md
        soul_file = self.hermes_home / "SOUL.md"
        if soul_file.exists():
            if structured.get_json("config:soul") is not None:
                skipped += 1
            else:
                try:
                    content = soul_file.read_text(encoding="utf-8")
                    structured.set_json("config:soul", {"content": content, "format": "markdown"})
                    imported += 1
                    self.logger.debug("Imported SOUL.md")
                except Exception as e:
                    errors.append(f"SOUL.md: {e}")

        self.logger.info("Config: imported=%d skipped=%d errors=%d", imported, skipped, len(errors))
        return MigrationResult(
            section=section,
            success=len(errors) == 0,
            items_imported=imported,
            items_skipped=skipped,
            errors=errors,
        )

    # -------------------------------------------------------------------------
    # Memory
    # -------------------------------------------------------------------------

    def import_memory(self) -> MigrationResult:
        """Import memory files into the structured backend under memory:index."""
        section = "memory"
        imported = 0
        skipped = 0
        errors: List[str] = []

        structured = self.backends.structured

        if structured.get_json("memory:index") is not None:
            skipped += 1
            self.logger.info("Memory: already present in backend (skipped)")
            return MigrationResult(
                section=section, success=True,
                items_imported=0, items_skipped=1,
            )

        memory_entries: List[str] = []
        user_entries: List[str] = []

        # Try HERMES_HOME/memory/ directory layout first
        mem_dir = self.hermes_home / "memory"
        if mem_dir.is_dir():
            memory_file = mem_dir / "MEMORY.md"
            user_file = mem_dir / "USER.md"
        else:
            # Legacy flat layout
            memory_file = self.hermes_home / "MEMORY.md"
            user_file = self.hermes_home / "USER.md"

        try:
            if memory_file.exists():
                raw = memory_file.read_text(encoding="utf-8")
                memory_entries = [e.strip() for e in raw.split("\n§\n") if e.strip()]
        except Exception as e:
            errors.append(f"MEMORY.md: {e}")

        try:
            if user_file.exists():
                raw = user_file.read_text(encoding="utf-8")
                user_entries = [e.strip() for e in raw.split("\n§\n") if e.strip()]
        except Exception as e:
            errors.append(f"USER.md: {e}")

        if memory_entries or user_entries:
            try:
                data = {
                    "memory": memory_entries,
                    "user": user_entries,
                    "updated_at": os.path.getmtime(str(memory_file))
                    if memory_file.exists()
                    else 0.0,
                }
                structured.set_json("memory:index", data)
                imported = len(memory_entries) + len(user_entries)
                self.logger.info("Memory: imported %d entries", imported)
            except Exception as e:
                errors.append(f"Writing memory:index: {e}")

        return MigrationResult(
            section=section,
            success=len(errors) == 0,
            items_imported=imported,
            items_skipped=skipped,
            errors=errors,
        )

    # -------------------------------------------------------------------------
    # Auth
    # -------------------------------------------------------------------------

    def import_auth(self) -> MigrationResult:
        """Import auth.json into the encrypted blob backend under auth:store."""
        section = "auth"
        auth_file = self.hermes_home / "auth.json"

        if not auth_file.exists():
            return MigrationResult(section=section, success=True, items_skipped=1)

        blobs = self.backends.encrypted_blobs

        if blobs.get_blob("auth:store") is not None:
            return MigrationResult(
                section=section, success=True, items_imported=0, items_skipped=1
            )

        try:
            data = auth_file.read_bytes()
            blobs.set_blob("auth:store", data)
            self.logger.info("Auth: imported auth.json (%d bytes)", len(data))
            return MigrationResult(section=section, success=True, items_imported=1)
        except Exception as e:
            return MigrationResult(
                section=section, success=False, errors=[f"auth.json: {e}"]
            )

    # -------------------------------------------------------------------------
    # import_all
    # -------------------------------------------------------------------------

    def import_all(self) -> Dict[str, MigrationResult]:
        """Run all import methods. Returns dict of section name → MigrationResult."""
        results: Dict[str, MigrationResult] = {}

        steps = [
            ("sessions", self.import_sessions),
            ("cron", self.import_cron_jobs),
            ("config", self.import_config),
            ("memory", self.import_memory),
            ("auth", self.import_auth),
        ]

        for name, fn in steps:
            self.logger.info("Migration: starting section '%s'", name)
            try:
                result = fn()
            except Exception as e:
                result = MigrationResult(
                    section=name, success=False, errors=[f"Unexpected error: {e}"]
                )
            results[name] = result
            self.logger.info("Migration: %s", result)

        return results


# =============================================================================
# validate_parity
# =============================================================================


def validate_parity(hermes_home: Path, backends: StorageBackendSet) -> Dict[str, bool]:
    """Compare file-based state vs backend state.

    Returns dict of section → parity_ok.
    """
    parity: Dict[str, bool] = {}

    # --- Sessions ---
    db_path = hermes_home / "state.db"
    try:
        if db_path.exists():
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
            conn.execute("PRAGMA query_only=ON")
            try:
                file_count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            finally:
                conn.close()
            backend_count = len(backends.structured.list_keys("sessions:meta:"))
            parity["sessions"] = file_count == backend_count
            if file_count != backend_count:
                logger.warning(
                    "Session parity mismatch: file=%d backend=%d",
                    file_count, backend_count,
                )
        else:
            backend_count = len(backends.structured.list_keys("sessions:meta:"))
            parity["sessions"] = backend_count == 0
    except Exception as e:
        logger.warning("Session parity check failed: %s", e)
        parity["sessions"] = False

    # --- Cron jobs ---
    jobs_file = hermes_home / "cron" / "jobs.json"
    try:
        if jobs_file.exists():
            raw = jobs_file.read_text(encoding="utf-8")
            raw_data = json.loads(raw)
            if isinstance(raw_data, dict):
                jobs_list = raw_data.get("jobs", [])
            elif isinstance(raw_data, list):
                jobs_list = raw_data
            else:
                jobs_list = []
            file_job_count = len(jobs_list)
            backend_job_count = len(backends.structured.list_keys("cron:jobs:"))
            parity["cron"] = file_job_count == backend_job_count
            if file_job_count != backend_job_count:
                logger.warning(
                    "Cron parity mismatch: file=%d backend=%d",
                    file_job_count, backend_job_count,
                )
        else:
            backend_job_count = len(backends.structured.list_keys("cron:jobs:"))
            parity["cron"] = backend_job_count == 0
    except Exception as e:
        logger.warning("Cron parity check failed: %s", e)
        parity["cron"] = False

    # --- Config ---
    config_file = hermes_home / "config.yaml"
    try:
        if config_file.exists():
            file_content = config_file.read_text(encoding="utf-8")
            backend_val = backends.structured.get_json("config:main")
            if backend_val is None:
                parity["config"] = False
            else:
                backend_content = backend_val.get("content", "")
                parity["config"] = file_content.strip() == backend_content.strip()
        else:
            parity["config"] = backends.structured.get_json("config:main") is None
    except Exception as e:
        logger.warning("Config parity check failed: %s", e)
        parity["config"] = False

    # --- Auth ---
    auth_file = hermes_home / "auth.json"
    try:
        file_has_auth = auth_file.exists()
        backend_has_auth = backends.encrypted_blobs.get_blob("auth:store") is not None
        parity["auth"] = file_has_auth == backend_has_auth
        if file_has_auth != backend_has_auth:
            logger.warning(
                "Auth parity mismatch: file_exists=%s backend_exists=%s",
                file_has_auth, backend_has_auth,
            )
    except Exception as e:
        logger.warning("Auth parity check failed: %s", e)
        parity["auth"] = False

    return parity


# =============================================================================
# migrate_hermes_home
# =============================================================================


def migrate_hermes_home(
    hermes_home: Optional[Path] = None,
    backend_type: str = "postgres",
    postgres_url: Optional[str] = None,
    validate_after: bool = True,
    dry_run: bool = False,
) -> bool:
    """Top-level migration function. Run from CLI or startup script.

    Returns True if migration succeeded (and parity validated if requested).
    """
    from hermes_constants import get_hermes_home
    from .factory import create_storage_backends

    if hermes_home is None:
        hermes_home = get_hermes_home()

    logger.info("migrate_hermes_home: home=%s backend=%s dry_run=%s", hermes_home, backend_type, dry_run)

    if dry_run:
        logger.info("Dry run — no data will be written")
        migration = HermesMigration(hermes_home, _noop_backends())
        results = migration.import_all()
        for r in results.values():
            logger.info("DRY RUN %s", r)
        return all(r.success for r in results.values())

    try:
        backends = create_storage_backends(backend_type, postgres_url)
    except Exception as e:
        logger.error("Failed to create storage backends: %s", e)
        return False

    try:
        migration = HermesMigration(hermes_home, backends)
        results = migration.import_all()

        success = all(r.success for r in results.values())
        for r in results.values():
            level = logging.INFO if r.success else logging.WARNING
            logger.log(level, "  %s", r)

        if validate_after:
            logger.info("Validating parity after migration…")
            parity = validate_parity(hermes_home, backends)
            for section, ok in parity.items():
                if ok:
                    logger.info("  ✓ %s parity OK", section)
                else:
                    logger.warning("  ✗ %s parity MISMATCH", section)
            success = success and all(parity.values())

        return success
    finally:
        try:
            backends.close()
        except Exception:
            pass


# =============================================================================
# startup_init_backends
# =============================================================================


def startup_init_backends(
    backend_type: Optional[str] = None,
    postgres_url: Optional[str] = None,
) -> Optional[StorageBackendSet]:
    """Initialize storage backends and wire them into all Hermes modules.

    Called once at startup. Returns None if using local/file-based defaults (no-op).
    Returns StorageBackendSet if backends were configured and wired.

    Idempotent: safe to call multiple times (subsequent calls are no-ops).
    """
    global _backends_initialized

    backend_type = backend_type or os.environ.get("HERMES_STORAGE_BACKEND", "local")
    backend_type = backend_type.strip().lower()

    if backend_type == "local":
        # No wiring needed — file-based defaults work as-is
        try:
            from hermes_logging import configure_stateless_logging, is_stateless_mode
            if is_stateless_mode():
                configure_stateless_logging()
        except ImportError:
            pass
        return None

    if _backends_initialized:
        logger.debug("startup_init_backends: already initialized, skipping")
        return None

    from .factory import create_storage_backends

    try:
        backends = create_storage_backends(backend_type, postgres_url)
    except Exception as e:
        logger.error("startup_init_backends: failed to create backends: %s", e)
        return None

    _wire_backends(backends)
    _backends_initialized = True
    logger.info("startup_init_backends: backends wired (%s)", backend_type)
    return backends


def _wire_backends(backends: StorageBackendSet) -> None:
    """Wire initialized backends into all Hermes modules.

    Each import is guarded by try/except ImportError so partial installs work.
    """
    # cron.jobs — takes structured, artifacts, locks separately
    try:
        from cron import jobs as cron_jobs
        cron_jobs.init_storage(
            backends.structured,
            backends.artifacts,
            backends.locks,
        )
        logger.debug("Wired: cron.jobs")
    except ImportError:
        pass

    # hermes_cli.config
    try:
        from hermes_cli import config as hermes_config
        hermes_config.init_storage(backends.structured)
        logger.debug("Wired: hermes_cli.config")
    except ImportError:
        pass

    # hermes_cli.auth
    try:
        from hermes_cli import auth as hermes_auth
        hermes_auth.init_storage(backends.encrypted_blobs)
        logger.debug("Wired: hermes_cli.auth")
    except ImportError:
        pass

    # agent.prompt_builder (SOUL)
    try:
        from agent import prompt_builder
        prompt_builder.init_storage(backends.structured)
        logger.debug("Wired: agent.prompt_builder")
    except ImportError:
        pass

    # tools.memory_tool
    try:
        from tools import memory_tool
        memory_tool.init_storage(backends.structured)
        logger.debug("Wired: tools.memory_tool")
    except ImportError:
        pass

    # tools.skills_tool
    try:
        from tools import skills_tool
        skills_tool.init_storage(backends.structured)
        logger.debug("Wired: tools.skills_tool")
    except ImportError:
        pass

    # tools.skill_manager_tool
    try:
        from tools import skill_manager_tool
        skill_manager_tool.init_storage(backends.structured)
        skill_manager_tool.init_artifact_storage(backends.artifacts)
        logger.debug("Wired: tools.skill_manager_tool")
    except ImportError:
        pass

    # Restore skill files from backend → HERMES_HOME/skills/ so file-scanning
    # skill readers work on a fresh container without a PVC.
    try:
        from storage.skill_sync import restore_skills_from_backend as _restore_skills
        from agent.skill_utils import get_skills_dir as _get_skills_dir
        _skills_dir = _get_skills_dir()
        _result = _restore_skills(backends.artifacts, _skills_dir)
        if _result["files"] > 0:
            logger.info(
                "skill_sync: restored %d skills (%d files) from backend",
                _result["skills"], _result["files"],
            )
    except Exception as _exc:
        logger.debug("skill_sync: restore skipped: %s", _exc)

    # hermes_logging — configure stateless if requested
    try:
        from hermes_logging import configure_stateless_logging, is_stateless_mode
        if is_stateless_mode():
            configure_stateless_logging()
            logger.debug("Wired: hermes_logging (stateless)")
    except ImportError:
        pass


# =============================================================================
# Internal helpers
# =============================================================================


def _noop_backends() -> StorageBackendSet:
    """Return a no-op StorageBackendSet for dry-run mode."""
    from .backends import (
        ArtifactStorageBackend,
        DistributedLockBackend,
        EncryptedBlobBackend,
        StructuredStateBackend,
    )
    from typing import Tuple

    class _NoopStructured(StructuredStateBackend):
        def get_json(self, key, default=None): return default
        def set_json(self, key, value): pass
        def delete_json(self, key): return False
        def list_keys(self, prefix=""): return []
        def search(self, query, prefix="", limit=100): return []
        def upsert_json(self, key, value): pass
        def batch_get(self, keys): return {}
        def batch_set(self, items): pass

    class _NoopBlobs(EncryptedBlobBackend):
        def get_blob(self, key, default=None): return default
        def set_blob(self, key, value): pass
        def delete_blob(self, key): return False
        def list_keys(self, prefix=""): return []
        def batch_get(self, keys): return {k: None for k in keys}
        def batch_delete(self, keys): return 0

    class _NoopArtifacts(ArtifactStorageBackend):
        def write_artifact(self, artifact_id, data): return artifact_id
        def read_artifact(self, artifact_id): return None
        def delete_artifact(self, artifact_id): return False
        def list_artifacts(self, prefix=""): return []
        def get_artifact_size(self, artifact_id): return None

    class _NoopLocks(DistributedLockBackend):
        def acquire_lock(self, lock_id, duration_seconds, wait_seconds=0): return None
        def release_lock(self, lock_id): return False
        def extend_lock(self, lock_id, duration_seconds): return False
        def is_locked(self, lock_id): return False

    return StorageBackendSet(
        structured=_NoopStructured(),
        encrypted_blobs=_NoopBlobs(),
        artifacts=_NoopArtifacts(),
        locks=_NoopLocks(),
    )
