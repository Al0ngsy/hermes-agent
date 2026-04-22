"""Storage abstraction layer for Hermes Agent.

Separates state management into four backend contracts:
1. StructuredStateBackend - JSON/dict data (sessions, config, cron jobs)
2. EncryptedBlobBackend - Sensitive credentials and tokens
3. ArtifactStorageBackend - Large binary blobs (recordings, exports)
4. DistributedLockBackend - Multi-instance coordination (job claiming, etc.)

Each backend can be implemented via local filesystem/SQLite (legacy mode)
or PostgreSQL (stateless mode). Backend selection is driven by configuration.
"""

from .backends import (
    StructuredStateBackend,
    EncryptedBlobBackend,
    ArtifactStorageBackend,
    DistributedLockBackend,
    StorageBackendSet,
)
from .factory import create_storage_backends
from .skill_registry import SkillRegistry

__all__ = [
    "StructuredStateBackend",
    "EncryptedBlobBackend",
    "ArtifactStorageBackend",
    "DistributedLockBackend",
    "StorageBackendSet",
    "create_storage_backends",
    "SkillRegistry",
]
