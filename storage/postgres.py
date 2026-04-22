"""
PostgreSQL storage backend implementation for stateless Hermes.

This backend moves all durable state to PostgreSQL, enabling multi-instance
coordination and stateless deployment (no persistent volumes required).

Architecture:
- StructuredState: JSONB columns with pg_trgm + tsvector for search
- EncryptedBlobs: BYTEA columns
- Artifacts: BYTEA columns (object storage is optional future work)
- Locks: locks table with holder_id + expiry; no advisory locks

psycopg3 notes:
- Use `psycopg` (not psycopg2) and `psycopg_pool.ConnectionPool`
- `with pool.connection() as conn:` auto-commits on success, rolls back on exception
- `conn.execute()` returns a Cursor; JSONB columns auto-deserialise to Python dicts
- BYTEA columns return bytes; pass bytes directly when writing
- `psycopg.errors.UniqueViolation` is the constraint-violation exception class
- `%s` is the placeholder; escape literal `%` in SQL as `%%`
"""

import logging
import os
import sys
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

# ── Allow `python3 storage/postgres.py` for the __main__ test block ──────────
if __name__ == "__main__":
    _pkg_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _pkg_root not in sys.path:
        sys.path.insert(0, _pkg_root)

logger = logging.getLogger(__name__)

# ── Optional psycopg3 import (graceful if not installed) ─────────────────────
try:
    import psycopg
    import psycopg.errors
    from psycopg.types.json import Jsonb
    from psycopg_pool import ConnectionPool
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False
    psycopg = None          # type: ignore[assignment]
    Jsonb = None            # type: ignore[assignment]
    ConnectionPool = None   # type: ignore[assignment]

# ── Abstract base-class imports (relative when used as package, absolute when
#    running as __main__) ─────────────────────────────────────────────────────
try:
    from .backends import (
        StructuredStateBackend,
        EncryptedBlobBackend,
        ArtifactStorageBackend,
        DistributedLockBackend,
        LockHandle,
        StorageBackendSet,
    )
except ImportError:
    from storage.backends import (   # type: ignore[no-redef]
        StructuredStateBackend,
        EncryptedBlobBackend,
        ArtifactStorageBackend,
        DistributedLockBackend,
        LockHandle,
        StorageBackendSet,
    )

# ── Schema DDL ────────────────────────────────────────────────────────────────
# Each entry is (sql, optional).  Optional statements are skipped with a warning
# rather than raising, to handle environments where pg_trgm is unavailable.
_DDL: List[Tuple[str, bool]] = [
    ("CREATE EXTENSION IF NOT EXISTS pg_trgm", True),
    (
        """CREATE TABLE IF NOT EXISTS structured_state (
            key         TEXT              PRIMARY KEY,
            value       JSONB             NOT NULL,
            created_at  DOUBLE PRECISION  NOT NULL,
            updated_at  DOUBLE PRECISION  NOT NULL
        )""",
        False,
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_structured_key_prefix"
        " ON structured_state USING btree (key text_pattern_ops)",
        False,
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_structured_fts"
        " ON structured_state USING GIN (to_tsvector('english', value::text))",
        False,
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_structured_trgm"
        " ON structured_state USING GIN ((value::text) gin_trgm_ops)",
        True,
    ),
    (
        """CREATE TABLE IF NOT EXISTS encrypted_blobs (
            key         TEXT              PRIMARY KEY,
            value       BYTEA             NOT NULL,
            created_at  DOUBLE PRECISION  NOT NULL,
            updated_at  DOUBLE PRECISION  NOT NULL
        )""",
        False,
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_blobs_key_prefix"
        " ON encrypted_blobs USING btree (key text_pattern_ops)",
        False,
    ),
    (
        """CREATE TABLE IF NOT EXISTS artifacts (
            artifact_id  TEXT              PRIMARY KEY,
            data         BYTEA             NOT NULL,
            size_bytes   INTEGER           NOT NULL,
            created_at   DOUBLE PRECISION  NOT NULL
        )""",
        False,
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_artifacts_id_prefix"
        " ON artifacts USING btree (artifact_id text_pattern_ops)",
        False,
    ),
    (
        """CREATE TABLE IF NOT EXISTS locks (
            lock_id    TEXT              PRIMARY KEY,
            holder_id  TEXT              NOT NULL,
            expires_at DOUBLE PRECISION  NOT NULL
        )""",
        False,
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_locks_expires ON locks (expires_at)",
        False,
    ),
]


def _init_schema(pool) -> None:
    """Execute all DDL statements; skip optional ones on failure."""
    for sql, optional in _DDL:
        try:
            with pool.connection() as conn:
                conn.execute(sql)
        except Exception as exc:
            if optional:
                logger.warning("Optional schema step skipped (%s...): %s", sql[:50], exc)
            else:
                logger.error("Required schema step failed: %s", exc)
                raise RuntimeError(f"PostgreSQL schema init failed: {exc}") from exc


# ── StructuredStateBackend ────────────────────────────────────────────────────

class PostgresStructuredStateBackend(StructuredStateBackend):
    """JSONB-backed structured state with tsvector + trigram search."""

    def __init__(self, pool) -> None:
        self.pool = pool

    def get_json(
        self, key: str, default: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "SELECT value FROM structured_state WHERE key = %s", (key,)
                )
                row = cur.fetchone()
            return row[0] if row is not None else default
        except Exception as exc:
            logger.error("get_json(%r): %s", key, exc)
            raise RuntimeError(f"get_json failed: {exc}") from exc

    def set_json(self, key: str, value: Dict[str, Any]) -> None:
        now = time.time()
        try:
            with self.pool.connection() as conn:
                conn.execute(
                    """INSERT INTO structured_state (key, value, created_at, updated_at)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (key) DO UPDATE
                           SET value      = EXCLUDED.value,
                               updated_at = EXCLUDED.updated_at""",
                    (key, Jsonb(value), now, now),
                )
        except Exception as exc:
            logger.error("set_json(%r): %s", key, exc)
            raise RuntimeError(f"set_json failed: {exc}") from exc

    def delete_json(self, key: str) -> bool:
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "DELETE FROM structured_state WHERE key = %s", (key,)
                )
                return cur.rowcount > 0
        except Exception as exc:
            logger.error("delete_json(%r): %s", key, exc)
            raise RuntimeError(f"delete_json failed: {exc}") from exc

    def list_keys(self, prefix: str = "") -> List[str]:
        try:
            with self.pool.connection() as conn:
                if prefix:
                    cur = conn.execute(
                        "SELECT key FROM structured_state"
                        " WHERE key LIKE %s ORDER BY key",
                        (prefix + "%",),
                    )
                else:
                    cur = conn.execute(
                        "SELECT key FROM structured_state ORDER BY key"
                    )
                rows = cur.fetchall()
            return [row[0] for row in rows]
        except Exception as exc:
            logger.error("list_keys(%r): %s", prefix, exc)
            raise RuntimeError(f"list_keys failed: {exc}") from exc

    def search(
        self, query: str, prefix: str = "", limit: int = 100
    ) -> List[Tuple[str, Dict[str, Any]]]:
        if not query:
            keys = self.list_keys(prefix)[:limit]
            return [(k, v) for k in keys if (v := self.get_json(k)) is not None]
        try:
            with self.pool.connection() as conn:
                if prefix:
                    cur = conn.execute(
                        """SELECT key, value FROM structured_state
                           WHERE key LIKE %s
                             AND (
                               to_tsvector('english', value::text)
                                   @@ plainto_tsquery('english', %s)
                               OR value::text ILIKE %s
                             )
                           ORDER BY key LIMIT %s""",
                        (prefix + "%", query, f"%{query}%", limit),
                    )
                else:
                    cur = conn.execute(
                        """SELECT key, value FROM structured_state
                           WHERE to_tsvector('english', value::text)
                                     @@ plainto_tsquery('english', %s)
                              OR value::text ILIKE %s
                           ORDER BY key LIMIT %s""",
                        (query, f"%{query}%", limit),
                    )
                rows = cur.fetchall()
            return [(row[0], row[1]) for row in rows]
        except Exception as exc:
            logger.error("search(%r): %s", query, exc)
            raise RuntimeError(f"search failed: {exc}") from exc

    def upsert_json(self, key: str, value: Dict[str, Any]) -> None:
        self.set_json(key, value)

    def batch_get(self, keys: List[str]) -> Dict[str, Dict[str, Any]]:
        if not keys:
            return {}
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "SELECT key, value FROM structured_state WHERE key = ANY(%s)",
                    (keys,),
                )
                rows = cur.fetchall()
            return {row[0]: row[1] for row in rows}
        except Exception as exc:
            logger.error("batch_get: %s", exc)
            raise RuntimeError(f"batch_get failed: {exc}") from exc

    def batch_set(self, items: Dict[str, Dict[str, Any]]) -> None:
        if not items:
            return
        now = time.time()
        try:
            with self.pool.connection() as conn:
                conn.executemany(
                    """INSERT INTO structured_state (key, value, created_at, updated_at)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (key) DO UPDATE
                           SET value      = EXCLUDED.value,
                               updated_at = EXCLUDED.updated_at""",
                    [(k, Jsonb(v), now, now) for k, v in items.items()],
                )
        except Exception as exc:
            logger.error("batch_set: %s", exc)
            raise RuntimeError(f"batch_set failed: {exc}") from exc

    def close(self) -> None:
        try:
            self.pool.close()
        except Exception:
            pass


# ── EncryptedBlobBackend ──────────────────────────────────────────────────────

class PostgresEncryptedBlobBackend(EncryptedBlobBackend):
    """BYTEA-backed encrypted blob storage."""

    def __init__(self, pool) -> None:
        self.pool = pool

    def get_blob(self, key: str, default: Optional[bytes] = None) -> Optional[bytes]:
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "SELECT value FROM encrypted_blobs WHERE key = %s", (key,)
                )
                row = cur.fetchone()
            return bytes(row[0]) if row is not None else default
        except Exception as exc:
            logger.error("get_blob(%r): %s", key, exc)
            raise RuntimeError(f"get_blob failed: {exc}") from exc

    def set_blob(self, key: str, value: bytes) -> None:
        now = time.time()
        try:
            with self.pool.connection() as conn:
                conn.execute(
                    """INSERT INTO encrypted_blobs (key, value, created_at, updated_at)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (key) DO UPDATE
                           SET value      = EXCLUDED.value,
                               updated_at = EXCLUDED.updated_at""",
                    (key, value, now, now),
                )
        except Exception as exc:
            logger.error("set_blob(%r): %s", key, exc)
            raise RuntimeError(f"set_blob failed: {exc}") from exc

    def delete_blob(self, key: str) -> bool:
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "DELETE FROM encrypted_blobs WHERE key = %s", (key,)
                )
                return cur.rowcount > 0
        except Exception as exc:
            logger.error("delete_blob(%r): %s", key, exc)
            raise RuntimeError(f"delete_blob failed: {exc}") from exc

    def list_keys(self, prefix: str = "") -> List[str]:
        try:
            with self.pool.connection() as conn:
                if prefix:
                    cur = conn.execute(
                        "SELECT key FROM encrypted_blobs"
                        " WHERE key LIKE %s ORDER BY key",
                        (prefix + "%",),
                    )
                else:
                    cur = conn.execute(
                        "SELECT key FROM encrypted_blobs ORDER BY key"
                    )
                rows = cur.fetchall()
            return [row[0] for row in rows]
        except Exception as exc:
            logger.error("list_keys(%r): %s", prefix, exc)
            raise RuntimeError(f"list_keys failed: {exc}") from exc

    def batch_get(self, keys: List[str]) -> Dict[str, Optional[bytes]]:
        if not keys:
            return {}
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "SELECT key, value FROM encrypted_blobs WHERE key = ANY(%s)",
                    (keys,),
                )
                rows = cur.fetchall()
            found = {row[0]: bytes(row[1]) for row in rows}
            return {k: found.get(k) for k in keys}
        except Exception as exc:
            logger.error("batch_get: %s", exc)
            raise RuntimeError(f"batch_get failed: {exc}") from exc

    def batch_delete(self, keys: List[str]) -> int:
        if not keys:
            return 0
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "DELETE FROM encrypted_blobs WHERE key = ANY(%s)", (keys,)
                )
                return cur.rowcount
        except Exception as exc:
            logger.error("batch_delete: %s", exc)
            raise RuntimeError(f"batch_delete failed: {exc}") from exc

    def close(self) -> None:
        try:
            self.pool.close()
        except Exception:
            pass


# ── ArtifactStorageBackend ────────────────────────────────────────────────────

class PostgresArtifactStorageBackend(ArtifactStorageBackend):
    """BYTEA-backed artifact storage; returns postgresql://artifact/<id> URIs."""

    def __init__(self, pool) -> None:
        self.pool = pool

    def write_artifact(self, artifact_id: str, data: bytes) -> str:
        now = time.time()
        try:
            with self.pool.connection() as conn:
                conn.execute(
                    """INSERT INTO artifacts (artifact_id, data, size_bytes, created_at)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (artifact_id) DO UPDATE
                           SET data       = EXCLUDED.data,
                               size_bytes = EXCLUDED.size_bytes""",
                    (artifact_id, data, len(data), now),
                )
            return f"postgresql://artifact/{artifact_id}"
        except Exception as exc:
            logger.error("write_artifact(%r): %s", artifact_id, exc)
            raise RuntimeError(f"write_artifact failed: {exc}") from exc

    def read_artifact(self, artifact_id: str) -> Optional[bytes]:
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "SELECT data FROM artifacts WHERE artifact_id = %s",
                    (artifact_id,),
                )
                row = cur.fetchone()
            return bytes(row[0]) if row is not None else None
        except Exception as exc:
            logger.error("read_artifact(%r): %s", artifact_id, exc)
            raise RuntimeError(f"read_artifact failed: {exc}") from exc

    def delete_artifact(self, artifact_id: str) -> bool:
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "DELETE FROM artifacts WHERE artifact_id = %s", (artifact_id,)
                )
                return cur.rowcount > 0
        except Exception as exc:
            logger.error("delete_artifact(%r): %s", artifact_id, exc)
            raise RuntimeError(f"delete_artifact failed: {exc}") from exc

    def list_artifacts(self, prefix: str = "") -> List[str]:
        try:
            with self.pool.connection() as conn:
                if prefix:
                    cur = conn.execute(
                        "SELECT artifact_id FROM artifacts"
                        " WHERE artifact_id LIKE %s ORDER BY artifact_id",
                        (prefix + "%",),
                    )
                else:
                    cur = conn.execute(
                        "SELECT artifact_id FROM artifacts ORDER BY artifact_id"
                    )
                rows = cur.fetchall()
            return [row[0] for row in rows]
        except Exception as exc:
            logger.error("list_artifacts(%r): %s", prefix, exc)
            raise RuntimeError(f"list_artifacts failed: {exc}") from exc

    def get_artifact_size(self, artifact_id: str) -> Optional[int]:
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "SELECT size_bytes FROM artifacts WHERE artifact_id = %s",
                    (artifact_id,),
                )
                row = cur.fetchone()
            return row[0] if row is not None else None
        except Exception as exc:
            logger.error("get_artifact_size(%r): %s", artifact_id, exc)
            raise RuntimeError(f"get_artifact_size failed: {exc}") from exc

    def close(self) -> None:
        try:
            self.pool.close()
        except Exception:
            pass


# ── DistributedLockBackend ────────────────────────────────────────────────────

class PostgresDistributedLockBackend(DistributedLockBackend):
    """
    PostgreSQL-backed distributed locks using a `locks` table with holder_id.

    Each acquire() generates a fresh UUID (holder_id) stored both in-memory
    and in the DB row.  Release/extend verify holder_id so stale or
    cross-instance operations are safe-guarded.

    Lock acquisition flow:
      1. Try INSERT; on UniqueViolation check whether existing row is expired.
      2. If expired, DELETE the stale row and retry immediately.
      3. If not expired and wait_seconds > 0, sleep 50 ms and retry until deadline.
    """

    def __init__(self, pool) -> None:
        self.pool = pool
        self._held_locks: Dict[str, str] = {}   # lock_id → holder_id
        self._mutex = threading.Lock()

    def acquire_lock(
        self, lock_id: str, duration_seconds: float, wait_seconds: float = 0
    ) -> Optional[LockHandle]:
        holder_id = str(uuid.uuid4())
        expires_at = time.time() + duration_seconds
        deadline = time.time() + wait_seconds

        while True:
            acquired = False
            try:
                with self.pool.connection() as conn:
                    conn.execute(
                        "INSERT INTO locks (lock_id, holder_id, expires_at)"
                        " VALUES (%s, %s, %s)",
                        (lock_id, holder_id, expires_at),
                    )
                    acquired = True
            except Exception as exc:
                # UniqueViolation means another holder owns the lock
                if psycopg and isinstance(exc, psycopg.errors.UniqueViolation):
                    pass
                else:
                    raise RuntimeError(f"acquire_lock failed: {exc}") from exc

            if acquired:
                with self._mutex:
                    self._held_locks[lock_id] = holder_id
                return LockHandle(lock_id, expires_at, self)

            # Lock held — evict it if expired, then retry or give up
            if self._clear_expired_lock(lock_id):
                continue

            if wait_seconds <= 0 or time.time() >= deadline:
                return None

            time.sleep(0.05)

    def _clear_expired_lock(self, lock_id: str) -> bool:
        """Delete a lock row only if it has already expired. Returns True if deleted."""
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "DELETE FROM locks WHERE lock_id = %s AND expires_at <= %s",
                    (lock_id, time.time()),
                )
                return cur.rowcount > 0
        except Exception:
            return False

    def release_lock(self, lock_id: str) -> bool:
        with self._mutex:
            holder_id = self._held_locks.pop(lock_id, None)
        if not holder_id:
            return False
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "DELETE FROM locks WHERE lock_id = %s AND holder_id = %s",
                    (lock_id, holder_id),
                )
                return cur.rowcount > 0
        except Exception as exc:
            raise RuntimeError(f"release_lock failed: {exc}") from exc

    def extend_lock(self, lock_id: str, duration_seconds: float) -> bool:
        with self._mutex:
            holder_id = self._held_locks.get(lock_id)
        if not holder_id:
            return False
        new_expires = time.time() + duration_seconds
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "UPDATE locks SET expires_at = %s"
                    " WHERE lock_id = %s AND holder_id = %s",
                    (new_expires, lock_id, holder_id),
                )
                return cur.rowcount > 0
        except Exception as exc:
            raise RuntimeError(f"extend_lock failed: {exc}") from exc

    def is_locked(self, lock_id: str) -> bool:
        try:
            with self.pool.connection() as conn:
                cur = conn.execute(
                    "SELECT expires_at FROM locks WHERE lock_id = %s", (lock_id,)
                )
                row = cur.fetchone()
            return row is not None and row[0] > time.time()
        except Exception as exc:
            raise RuntimeError(f"is_locked failed: {exc}") from exc

    def close(self) -> None:
        try:
            self.pool.close()
        except Exception:
            pass


# ── Factory ───────────────────────────────────────────────────────────────────

class PostgresStorageBackendSet:
    """Factory for PostgreSQL storage backend implementations."""

    def __init__(self, postgres_url: Optional[str] = None) -> None:
        self.postgres_url = postgres_url or os.environ.get(
            "HERMES_STORAGE_POSTGRES_URL", ""
        )
        if self.postgres_url:
            sanitized = (
                self.postgres_url.split("@")[-1]
                if "@" in self.postgres_url
                else self.postgres_url
            )
            logger.info("PostgreSQL backend configured: ...@%s", sanitized)

    def create(self) -> StorageBackendSet:
        """
        Open a connection pool, create/migrate the schema, and return a
        fully-initialised StorageBackendSet.

        Raises:
            RuntimeError: psycopg not installed, URL missing, or DB unreachable.
        """
        if not PSYCOPG_AVAILABLE:
            raise RuntimeError(
                "psycopg and psycopg_pool are required for the PostgreSQL backend. "
                "Install with: pip install 'psycopg[binary]' psycopg-pool"
            )

        url = self.postgres_url
        if not url:
            raise RuntimeError(
                "PostgreSQL URL required. Set HERMES_STORAGE_POSTGRES_URL "
                "or pass postgres_url= to PostgresStorageBackendSet."
            )

        try:
            pool = ConnectionPool(url, min_size=1, max_size=10)
            pool.open(wait=True, timeout=30.0)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to open PostgreSQL connection pool: {exc}"
            ) from exc

        _init_schema(pool)
        logger.info("PostgreSQL storage backends ready")

        return StorageBackendSet(
            structured=PostgresStructuredStateBackend(pool),
            encrypted_blobs=PostgresEncryptedBlobBackend(pool),
            artifacts=PostgresArtifactStorageBackend(pool),
            locks=PostgresDistributedLockBackend(pool),
        )


# ── Integration smoke-test ────────────────────────────────────────────────────
# Run with:  HERMES_TEST_POSTGRES_URL=postgresql://... python3 storage/postgres.py

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    url = os.environ.get("HERMES_TEST_POSTGRES_URL")
    if not url:
        print("Usage: HERMES_TEST_POSTGRES_URL=postgresql://... python3 storage/postgres.py")
        sys.exit(1)

    print("=== PostgreSQL Backend Smoke Tests ===\n")
    bs = PostgresStorageBackendSet(url).create()

    # ── Cleanup any leftover test keys from a previous run ──────────────────
    for k in ["__test:s1", "__test:s2", "__test:s3"]:
        bs.structured.delete_json(k)
    for k in ["__test:b1", "__test:b2"]:
        bs.encrypted_blobs.delete_blob(k)
    for a in ["__test-art-1"]:
        bs.artifacts.delete_artifact(a)
    for lk in ["__test:lock:1"]:
        bs.structured.delete_json(lk)   # no-op; just belt-and-braces

    # ── 1. StructuredState ──────────────────────────────────────────────────
    print("1. StructuredState")
    bs.structured.set_json("__test:s1", {"msg": "hello", "n": 1})
    bs.structured.set_json("__test:s2", {"msg": "world", "n": 2})
    bs.structured.batch_set({"__test:s3": {"msg": "batch", "n": 3}})

    got = bs.structured.get_json("__test:s1")
    assert got == {"msg": "hello", "n": 1}, f"Unexpected: {got}"

    keys = bs.structured.list_keys("__test:")
    assert "__test:s1" in keys and "__test:s2" in keys and "__test:s3" in keys, keys

    batch = bs.structured.batch_get(["__test:s1", "__test:s2"])
    assert batch["__test:s1"]["n"] == 1 and batch["__test:s2"]["n"] == 2

    results = bs.structured.search("hello", prefix="__test:")
    assert any(k == "__test:s1" for k, _ in results), f"Search missed: {results}"
    print(f"   search('hello') → {[(k, v['msg']) for k, v in results]}")

    deleted = bs.structured.delete_json("__test:s1")
    assert deleted
    assert bs.structured.get_json("__test:s1") is None
    bs.structured.delete_json("__test:s2")
    bs.structured.delete_json("__test:s3")
    print("   PASS\n")

    # ── 2. EncryptedBlobs ───────────────────────────────────────────────────
    print("2. EncryptedBlobs")
    raw = b"secret\x00\xff\xfe data"
    bs.encrypted_blobs.set_blob("__test:b1", raw)
    bs.encrypted_blobs.set_blob("__test:b2", b"another")

    assert bs.encrypted_blobs.get_blob("__test:b1") == raw
    assert bs.encrypted_blobs.get_blob("__test:b99") is None

    keys = bs.encrypted_blobs.list_keys("__test:")
    assert "__test:b1" in keys and "__test:b2" in keys, keys

    batch = bs.encrypted_blobs.batch_get(["__test:b1", "__test:b99"])
    assert batch["__test:b1"] == raw and batch["__test:b99"] is None

    n = bs.encrypted_blobs.batch_delete(["__test:b1", "__test:b2"])
    assert n == 2
    print("   PASS\n")

    # ── 3. ArtifactStorage ──────────────────────────────────────────────────
    print("3. ArtifactStorage")
    payload = b"artifact payload " * 64
    uri = bs.artifacts.write_artifact("__test-art-1", payload)
    assert uri == "postgresql://artifact/__test-art-1", uri

    assert bs.artifacts.read_artifact("__test-art-1") == payload
    assert bs.artifacts.get_artifact_size("__test-art-1") == len(payload)
    assert "__test-art-1" in bs.artifacts.list_artifacts("__test-")
    assert bs.artifacts.delete_artifact("__test-art-1")
    assert bs.artifacts.read_artifact("__test-art-1") is None
    print("   PASS\n")

    # ── 4. DistributedLocks ─────────────────────────────────────────────────
    print("4. DistributedLocks")
    h1 = bs.locks.acquire_lock("__test:lock:1", duration_seconds=30)
    assert h1 is not None, "First acquire should succeed"
    assert bs.locks.is_locked("__test:lock:1")

    h2 = bs.locks.acquire_lock("__test:lock:1", duration_seconds=30, wait_seconds=0)
    assert h2 is None, "Second acquire (no wait) should fail"

    assert bs.locks.extend_lock("__test:lock:1", 60)
    assert not h1.is_expired()

    assert bs.locks.release_lock("__test:lock:1")
    assert not bs.locks.is_locked("__test:lock:1")

    # Expired-lock eviction: acquire with 0.1s TTL, wait for it to expire, re-acquire
    h3 = bs.locks.acquire_lock("__test:lock:1", duration_seconds=0.1)
    assert h3 is not None
    time.sleep(0.2)
    h4 = bs.locks.acquire_lock("__test:lock:1", duration_seconds=10)
    assert h4 is not None, "Should evict expired lock and acquire"
    bs.locks.release_lock("__test:lock:1")
    print("   PASS\n")

    # ── lock_context helper (inherited from ABC) ─────────────────────────────
    with bs.locks.lock_context("__test:lock:ctx", duration_seconds=10) as lh:
        assert lh is not None
        assert bs.locks.is_locked("__test:lock:ctx")
    assert not bs.locks.is_locked("__test:lock:ctx")
    print("   lock_context: PASS\n")

    bs.close()
    print("All tests passed ✓")
