"""Abstract base classes for storage backends.

Each backend contract defines the operations needed for a specific storage domain:
- StructuredStateBackend: JSON/dict data with search capability
- EncryptedBlobBackend: Encrypted sensitive data (credentials, tokens)
- ArtifactStorageBackend: Large binary blobs
- DistributedLockBackend: Multi-instance coordination
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Generator, Tuple
import time


@dataclass
class LockHandle:
    """Represents an acquired distributed lock."""
    lock_id: str
    expires_at: float
    backend: "DistributedLockBackend"

    def is_expired(self) -> bool:
        """Check if the lock has expired."""
        return time.time() >= self.expires_at

    def extend(self, duration_seconds: float) -> None:
        """Extend the lock duration."""
        self.backend.extend_lock(self.lock_id, duration_seconds)
        self.expires_at = time.time() + duration_seconds


class StructuredStateBackend(ABC):
    """
    Stores JSON/dict data with search capability.

    Used for: session metadata, cron jobs, config, SOUL.md, memory entries,
    profile metadata, skills manifest, webhook state.
    """

    @abstractmethod
    def get_json(
        self, key: str, default: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Get a JSON object by key. Returns default if not found."""
        pass

    @abstractmethod
    def set_json(self, key: str, value: Dict[str, Any]) -> None:
        """Store a JSON object by key. Creates or overwrites."""
        pass

    @abstractmethod
    def delete_json(self, key: str) -> bool:
        """Delete a JSON object by key. Returns True if it existed."""
        pass

    @abstractmethod
    def list_keys(self, prefix: str = "") -> List[str]:
        """List all keys, optionally filtered by prefix."""
        pass

    @abstractmethod
    def search(
        self, query: str, prefix: str = "", limit: int = 100
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """Full-text search across values. Returns (key, value) tuples."""
        pass

    @abstractmethod
    def upsert_json(self, key: str, value: Dict[str, Any]) -> None:
        """Store JSON, creating if not present or updating if present."""
        pass

    @abstractmethod
    def batch_get(self, keys: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get multiple JSON objects in one operation."""
        pass

    @abstractmethod
    def batch_set(self, items: Dict[str, Dict[str, Any]]) -> None:
        """Set multiple JSON objects in one operation."""
        pass


class EncryptedBlobBackend(ABC):
    """
    Stores encrypted sensitive data.

    Used for: API keys, OAuth tokens, adapter session state, credentials.
    Automatically encrypted at rest. Large payloads may use object storage.
    """

    @abstractmethod
    def get_blob(self, key: str, default: Optional[bytes] = None) -> Optional[bytes]:
        """Get an encrypted blob by key. Returns default if not found."""
        pass

    @abstractmethod
    def set_blob(self, key: str, value: bytes) -> None:
        """Store an encrypted blob by key. Creates or overwrites."""
        pass

    @abstractmethod
    def delete_blob(self, key: str) -> bool:
        """Delete a blob by key. Returns True if it existed."""
        pass

    @abstractmethod
    def list_keys(self, prefix: str = "") -> List[str]:
        """List all blob keys, optionally filtered by prefix."""
        pass

    @abstractmethod
    def batch_get(self, keys: List[str]) -> Dict[str, Optional[bytes]]:
        """Get multiple blobs in one operation."""
        pass

    @abstractmethod
    def batch_delete(self, keys: List[str]) -> int:
        """Delete multiple blobs. Returns count deleted."""
        pass


class ArtifactStorageBackend(ABC):
    """
    Stores large binary artifacts.

    Used for: session transcripts, recordings, exports, large outputs.
    Can defer to object storage (S3, GCS) or local disk in stateless mode.
    """

    @abstractmethod
    def write_artifact(self, artifact_id: str, data: bytes) -> str:
        """Write an artifact and return its storage URI."""
        pass

    @abstractmethod
    def read_artifact(self, artifact_id: str) -> Optional[bytes]:
        """Read an artifact by ID. Returns None if not found."""
        pass

    @abstractmethod
    def delete_artifact(self, artifact_id: str) -> bool:
        """Delete an artifact. Returns True if it existed."""
        pass

    @abstractmethod
    def list_artifacts(self, prefix: str = "") -> List[str]:
        """List artifact IDs, optionally filtered by prefix."""
        pass

    @abstractmethod
    def get_artifact_size(self, artifact_id: str) -> Optional[int]:
        """Get artifact size in bytes. Returns None if not found."""
        pass


class DistributedLockBackend(ABC):
    """
    Multi-instance coordination via distributed locks.

    Used for: cron job claiming, single-run coordination, instance heartbeats.
    """

    @abstractmethod
    def acquire_lock(
        self, lock_id: str, duration_seconds: float, wait_seconds: float = 0
    ) -> Optional[LockHandle]:
        """
        Try to acquire a lock. If wait_seconds > 0, retry for that duration.
        Returns LockHandle if acquired, None if not available within timeout.
        """
        pass

    @abstractmethod
    def release_lock(self, lock_id: str) -> bool:
        """Release a lock. Returns True if it was held."""
        pass

    @abstractmethod
    def extend_lock(self, lock_id: str, duration_seconds: float) -> bool:
        """Extend a held lock. Returns True if extended."""
        pass

    @abstractmethod
    def is_locked(self, lock_id: str) -> bool:
        """Check if a lock is currently held."""
        pass

    @contextmanager
    def lock_context(
        self, lock_id: str, duration_seconds: float = 60, wait_seconds: float = 0
    ) -> Generator[Optional[LockHandle], None, None]:
        """Context manager for lock acquisition and automatic release."""
        lock_handle = self.acquire_lock(lock_id, duration_seconds, wait_seconds)
        try:
            yield lock_handle
        finally:
            if lock_handle:
                self.release_lock(lock_handle.lock_id)


@dataclass
class StorageBackendSet:
    """Container for all four storage backend implementations."""
    structured: StructuredStateBackend
    encrypted_blobs: EncryptedBlobBackend
    artifacts: ArtifactStorageBackend
    locks: DistributedLockBackend

    def close(self) -> None:
        """Close all backends and clean up resources."""
        for backend in [self.structured, self.encrypted_blobs, self.artifacts, self.locks]:
            if hasattr(backend, "close"):
                try:
                    backend.close()
                except Exception:
                    pass
