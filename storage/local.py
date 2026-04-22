"""
Local storage backend implementation using SQLite and filesystem.

This backend wraps existing Hermes behavior (hermes_state.py, HERMES_HOME files)
behind the storage abstraction layer. Used for backward compatibility and local
development.

Architecture:
- StructuredState: SQLite with JSON columns and FTS5 search
- EncryptedBlobs: File-based with os-level encryption (future: encrypted columns)
- Artifacts: Direct filesystem writes under HERMES_HOME/artifacts/
- Locks: SQLite advisory locks via pragma
"""

import json
import logging
import sqlite3
import threading
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import contextmanager

from hermes_constants import get_hermes_home
from .backends import (
    StructuredStateBackend,
    EncryptedBlobBackend,
    ArtifactStorageBackend,
    DistributedLockBackend,
    LockHandle,
    StorageBackendSet,
)

logger = logging.getLogger(__name__)


class LocalStructuredStateBackend(StructuredStateBackend):
    """SQLite-backed structured state with FTS5 search."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            timeout=5.0,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.Lock()

        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize SQLite schema for structured state."""
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS structured_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_structured_created ON structured_state(created_at);
                CREATE INDEX IF NOT EXISTS idx_structured_updated ON structured_state(updated_at);

                CREATE VIRTUAL TABLE IF NOT EXISTS structured_state_fts USING fts5(
                    content,
                    content=structured_state,
                    content_rowid=rowid
                );

                CREATE TRIGGER IF NOT EXISTS structured_state_ai AFTER INSERT ON structured_state BEGIN
                    INSERT INTO structured_state_fts(rowid, content) VALUES (NEW.rowid, NEW.value);
                END;

                CREATE TRIGGER IF NOT EXISTS structured_state_ad AFTER DELETE ON structured_state BEGIN
                    INSERT INTO structured_state_fts(structured_state_fts, rowid, content)
                    VALUES('delete', OLD.rowid, OLD.value);
                END;

                CREATE TRIGGER IF NOT EXISTS structured_state_au AFTER UPDATE ON structured_state BEGIN
                    INSERT INTO structured_state_fts(structured_state_fts, rowid, content)
                    VALUES('delete', OLD.rowid, OLD.value);
                    INSERT INTO structured_state_fts(rowid, content) VALUES (NEW.rowid, NEW.value);
                END;
            """)

    def get_json(
        self, key: str, default: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM structured_state WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except (json.JSONDecodeError, TypeError):
            return default

    def set_json(self, key: str, value: Dict[str, Any]) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO structured_state (key, value, created_at, updated_at)
                   VALUES (?, ?, ?, ?)""",
                (key, json.dumps(value), now, now),
            )
            self._conn.commit()

    def delete_json(self, key: str) -> bool:
        with self._lock:
            cursor = self._conn.execute("DELETE FROM structured_state WHERE key = ?", (key,))
            self._conn.commit()
        return cursor.rowcount > 0

    def list_keys(self, prefix: str = "") -> List[str]:
        with self._lock:
            if prefix:
                rows = self._conn.execute(
                    "SELECT key FROM structured_state WHERE key LIKE ? ORDER BY key",
                    (prefix + "%",),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT key FROM structured_state ORDER BY key"
                ).fetchall()
        return [row["key"] for row in rows]

    def search(
        self, query: str, prefix: str = "", limit: int = 100
    ) -> List[Tuple[str, Dict[str, Any]]]:
        with self._lock:
            if prefix:
                rows = self._conn.execute(
                    """SELECT key, value FROM structured_state
                       WHERE key IN (
                           SELECT key FROM structured_state
                           WHERE rowid IN (SELECT rowid FROM structured_state_fts WHERE content MATCH ?)
                       ) AND key LIKE ?
                       LIMIT ?""",
                    (query, prefix + "%", limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """SELECT key, value FROM structured_state
                       WHERE rowid IN (SELECT rowid FROM structured_state_fts WHERE content MATCH ?)
                       LIMIT ?""",
                    (query, limit),
                ).fetchall()

        result = []
        for row in rows:
            try:
                value = json.loads(row["value"])
                result.append((row["key"], value))
            except (json.JSONDecodeError, TypeError):
                pass
        return result

    def upsert_json(self, key: str, value: Dict[str, Any]) -> None:
        self.set_json(key, value)

    def batch_get(self, keys: List[str]) -> Dict[str, Dict[str, Any]]:
        result = {}
        with self._lock:
            placeholders = ",".join("?" * len(keys))
            rows = self._conn.execute(
                f"SELECT key, value FROM structured_state WHERE key IN ({placeholders})",
                keys,
            ).fetchall()
        for row in rows:
            try:
                result[row["key"]] = json.loads(row["value"])
            except (json.JSONDecodeError, TypeError):
                pass
        return result

    def batch_set(self, items: Dict[str, Dict[str, Any]]) -> None:
        now = time.time()
        with self._lock:
            for key, value in items.items():
                self._conn.execute(
                    """INSERT OR REPLACE INTO structured_state (key, value, created_at, updated_at)
                       VALUES (?, ?, ?, ?)""",
                    (key, json.dumps(value), now, now),
                )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            if self._conn:
                try:
                    self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except Exception:
                    pass
                self._conn.close()


class LocalEncryptedBlobBackend(EncryptedBlobBackend):
    """File-based encrypted blob storage under HERMES_HOME/blobs/."""

    def __init__(self, blob_dir: Path):
        self.blob_dir = blob_dir
        self.blob_dir.mkdir(parents=True, exist_ok=True)
        # Secure permissions: owner-only access
        try:
            os.chmod(self.blob_dir, 0o700)
        except (OSError, NotImplementedError):
            pass

    def _blob_path(self, key: str) -> Path:
        """Get path for a blob key, sanitizing for filesystem safety."""
        # Simple path traversal protection: reject keys with /
        if "/" in key or key.startswith("."):
            raise ValueError(f"Invalid blob key: {key}")
        return self.blob_dir / key

    def get_blob(self, key: str, default: Optional[bytes] = None) -> Optional[bytes]:
        path = self._blob_path(key)
        if not path.exists():
            return default
        try:
            return path.read_bytes()
        except (OSError, IOError):
            return default

    def set_blob(self, key: str, value: bytes) -> None:
        path = self._blob_path(key)
        path.write_bytes(value)
        # Secure: owner-only read/write
        try:
            os.chmod(path, 0o600)
        except (OSError, NotImplementedError):
            pass

    def delete_blob(self, key: str) -> bool:
        path = self._blob_path(key)
        if not path.exists():
            return False
        try:
            path.unlink()
            return True
        except (OSError, IOError):
            return False

    def list_keys(self, prefix: str = "") -> List[str]:
        keys = []
        for path in self.blob_dir.iterdir():
            if path.is_file():
                key = path.name
                if not prefix or key.startswith(prefix):
                    keys.append(key)
        return sorted(keys)

    def batch_get(self, keys: List[str]) -> Dict[str, Optional[bytes]]:
        result = {}
        for key in keys:
            result[key] = self.get_blob(key)
        return result

    def batch_delete(self, keys: List[str]) -> int:
        count = 0
        for key in keys:
            if self.delete_blob(key):
                count += 1
        return count


class LocalArtifactStorageBackend(ArtifactStorageBackend):
    """File-based artifact storage under HERMES_HOME/artifacts/."""

    def __init__(self, artifact_dir: Path):
        self.artifact_dir = artifact_dir
        self.artifact_dir.mkdir(parents=True, exist_ok=True)

    def _artifact_path(self, artifact_id: str) -> Path:
        """Get path for artifact, sanitizing for filesystem safety."""
        if "/" in artifact_id or artifact_id.startswith("."):
            raise ValueError(f"Invalid artifact ID: {artifact_id}")
        return self.artifact_dir / artifact_id

    def write_artifact(self, artifact_id: str, data: bytes) -> str:
        path = self._artifact_path(artifact_id)
        path.write_bytes(data)
        return f"file://{path}"

    def read_artifact(self, artifact_id: str) -> Optional[bytes]:
        path = self._artifact_path(artifact_id)
        if not path.exists():
            return None
        try:
            return path.read_bytes()
        except (OSError, IOError):
            return None

    def delete_artifact(self, artifact_id: str) -> bool:
        path = self._artifact_path(artifact_id)
        if not path.exists():
            return False
        try:
            path.unlink()
            return True
        except (OSError, IOError):
            return False

    def list_artifacts(self, prefix: str = "") -> List[str]:
        artifacts = []
        for path in self.artifact_dir.iterdir():
            if path.is_file():
                artifact_id = path.name
                if not prefix or artifact_id.startswith(prefix):
                    artifacts.append(artifact_id)
        return sorted(artifacts)

    def get_artifact_size(self, artifact_id: str) -> Optional[int]:
        path = self._artifact_path(artifact_id)
        if not path.exists():
            return None
        try:
            return path.stat().st_size
        except (OSError, IOError):
            return None


class LocalDistributedLockBackend(DistributedLockBackend):
    """
    SQLite-based distributed locks for local/multi-process coordination.
    
    Uses SQLite's ability to handle concurrent readers + single writer via WAL.
    For true multi-instance coordination, use PostgreSQL backend.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn = sqlite3.connect(
            str(db_path),
            check_same_thread=False,
            timeout=5.0,
            isolation_level=None,
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS locks (
                    lock_id TEXT PRIMARY KEY,
                    expires_at REAL NOT NULL
                )
            """)
            self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_locks_expires ON locks(expires_at)
            """)

    def acquire_lock(
        self, lock_id: str, duration_seconds: float, wait_seconds: float = 0
    ) -> Optional[LockHandle]:
        expires_at = time.time() + duration_seconds
        deadline = time.time() + wait_seconds if wait_seconds > 0 else time.time()

        while time.time() <= deadline:
            try:
                with self._lock:
                    self._conn.execute(
                        "INSERT INTO locks (lock_id, expires_at) VALUES (?, ?)",
                        (lock_id, expires_at),
                    )
                    self._conn.commit()
                return LockHandle(lock_id, expires_at, self)
            except sqlite3.IntegrityError:
                # Lock already held
                if wait_seconds <= 0:
                    return None
                time.sleep(0.05)

        return None

    def release_lock(self, lock_id: str) -> bool:
        with self._lock:
            cursor = self._conn.execute("DELETE FROM locks WHERE lock_id = ?", (lock_id,))
            self._conn.commit()
        return cursor.rowcount > 0

    def extend_lock(self, lock_id: str, duration_seconds: float) -> bool:
        expires_at = time.time() + duration_seconds
        with self._lock:
            cursor = self._conn.execute(
                "UPDATE locks SET expires_at = ? WHERE lock_id = ?",
                (expires_at, lock_id),
            )
            self._conn.commit()
        return cursor.rowcount > 0

    def is_locked(self, lock_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT expires_at FROM locks WHERE lock_id = ?", (lock_id,)
            ).fetchone()
        return row is not None and row[0] > time.time()

    def close(self) -> None:
        with self._lock:
            if self._conn:
                try:
                    self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except Exception:
                    pass
                self._conn.close()


class LocalStorageBackendSet:
    """Factory for local storage backend implementations."""

    def __init__(self, hermes_home: Optional[Path] = None):
        self.hermes_home = hermes_home or get_hermes_home()
        self.storage_dir = self.hermes_home / "storage"
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def create(self) -> StorageBackendSet:
        """Create and return all local backend implementations."""
        db_path = self.storage_dir / "state.db"
        blob_dir = self.storage_dir / "blobs"
        artifact_dir = self.storage_dir / "artifacts"

        logger.info(f"Initializing local storage backends under {self.storage_dir}")

        return StorageBackendSet(
            structured=LocalStructuredStateBackend(db_path),
            encrypted_blobs=LocalEncryptedBlobBackend(blob_dir),
            artifacts=LocalArtifactStorageBackend(artifact_dir),
            locks=LocalDistributedLockBackend(db_path),
        )
