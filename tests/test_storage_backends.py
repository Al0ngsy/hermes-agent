"""
Integration tests for storage abstraction layer.

Tests both local and PostgreSQL backends to ensure they comply with
the storage backend contracts.
"""

import pytest
import tempfile
import json
import time
from pathlib import Path
from typing import List

from storage.factory import create_storage_backends
from storage.backends import StorageBackendSet


class StorageBackendTests:
    """Base test class for storage backend validation."""

    @pytest.fixture
    def storage(self) -> StorageBackendSet:
        """Override in subclasses to provide appropriate backend."""
        raise NotImplementedError

    def test_structured_state_basic_get_set(self, storage):
        """Test basic get/set for structured state."""
        key = "test:key"
        value = {"name": "test", "count": 42}

        assert storage.structured.get_json(key) is None
        storage.structured.set_json(key, value)
        assert storage.structured.get_json(key) == value

    def test_structured_state_default(self, storage):
        """Test get_json with default value."""
        default = {"default": True}
        assert storage.structured.get_json("nonexistent", default) == default
        assert storage.structured.get_json("nonexistent") is None

    def test_structured_state_delete(self, storage):
        """Test delete operation."""
        key = "test:delete"
        value = {"data": "test"}

        storage.structured.set_json(key, value)
        assert storage.structured.get_json(key) == value
        assert storage.structured.delete_json(key) is True
        assert storage.structured.get_json(key) is None
        assert storage.structured.delete_json(key) is False

    def test_structured_state_list_keys(self, storage):
        """Test listing keys with optional prefix."""
        storage.structured.set_json("prefix:a", {"value": 1})
        storage.structured.set_json("prefix:b", {"value": 2})
        storage.structured.set_json("other:c", {"value": 3})

        all_keys = storage.structured.list_keys()
        assert len(all_keys) >= 3
        assert "prefix:a" in all_keys
        assert "prefix:b" in all_keys
        assert "other:c" in all_keys

        prefix_keys = storage.structured.list_keys("prefix:")
        assert "prefix:a" in prefix_keys
        assert "prefix:b" in prefix_keys
        assert "other:c" not in prefix_keys

    def test_structured_state_search(self, storage):
        """Test full-text search."""
        storage.structured.set_json("doc1", {"text": "hello world", "id": 1})
        storage.structured.set_json("doc2", {"text": "hello there", "id": 2})
        storage.structured.set_json("doc3", {"text": "goodbye", "id": 3})

        # Note: FTS search syntax varies by backend, so this is a simple test
        results = storage.structured.search("hello", limit=10)
        assert len(results) >= 2
        keys = [k for k, v in results]
        assert "doc1" in keys
        assert "doc2" in keys

    def test_structured_state_batch_operations(self, storage):
        """Test batch get and set."""
        items = {
            "batch:1": {"value": 1},
            "batch:2": {"value": 2},
            "batch:3": {"value": 3},
        }
        storage.structured.batch_set(items)

        retrieved = storage.structured.batch_get(list(items.keys()))
        assert retrieved == items

        # Test partial batch get
        partial = storage.structured.batch_get(["batch:1", "batch:2", "nonexistent"])
        assert partial.get("batch:1") == items["batch:1"]
        assert partial.get("batch:2") == items["batch:2"]
        assert "nonexistent" not in partial

    def test_encrypted_blob_basic(self, storage):
        """Test basic encrypted blob operations."""
        key = "secret:key"
        data = b"sensitive data"

        assert storage.encrypted_blobs.get_blob(key) is None
        storage.encrypted_blobs.set_blob(key, data)
        assert storage.encrypted_blobs.get_blob(key) == data

    def test_encrypted_blob_delete(self, storage):
        """Test encrypted blob deletion."""
        key = "secret:delete"
        data = b"temporary"

        storage.encrypted_blobs.set_blob(key, data)
        assert storage.encrypted_blobs.delete_blob(key) is True
        assert storage.encrypted_blobs.get_blob(key) is None
        assert storage.encrypted_blobs.delete_blob(key) is False

    def test_encrypted_blob_list_keys(self, storage):
        """Test listing encrypted blob keys."""
        storage.encrypted_blobs.set_blob("token:api_key", b"key1")
        storage.encrypted_blobs.set_blob("token:oauth", b"oauth_token")
        storage.encrypted_blobs.set_blob("cert:ssl", b"cert_data")

        all_keys = storage.encrypted_blobs.list_keys()
        assert "token:api_key" in all_keys
        assert "token:oauth" in all_keys
        assert "cert:ssl" in all_keys

        token_keys = storage.encrypted_blobs.list_keys("token:")
        assert "token:api_key" in token_keys
        assert "token:oauth" in token_keys
        assert "cert:ssl" not in token_keys

    def test_encrypted_blob_batch_operations(self, storage):
        """Test batch blob operations."""
        blobs = {
            "blob1": b"data1",
            "blob2": b"data2",
            "blob3": b"data3",
        }
        for k, v in blobs.items():
            storage.encrypted_blobs.set_blob(k, v)

        retrieved = storage.encrypted_blobs.batch_get(list(blobs.keys()))
        assert retrieved == blobs

        deleted = storage.encrypted_blobs.batch_delete(list(blobs.keys()))
        assert deleted == 3

    def test_artifact_storage_basic(self, storage):
        """Test basic artifact operations."""
        artifact_id = "session:123:transcript"
        data = b"session transcript data"

        uri = storage.artifacts.write_artifact(artifact_id, data)
        assert artifact_id in uri
        assert storage.artifacts.read_artifact(artifact_id) == data

    def test_artifact_storage_delete(self, storage):
        """Test artifact deletion."""
        artifact_id = "temp:artifact"
        data = b"temporary artifact"

        storage.artifacts.write_artifact(artifact_id, data)
        assert storage.artifacts.delete_artifact(artifact_id) is True
        assert storage.artifacts.read_artifact(artifact_id) is None
        assert storage.artifacts.delete_artifact(artifact_id) is False

    def test_artifact_storage_size(self, storage):
        """Test getting artifact size."""
        artifact_id = "sized:artifact"
        data = b"x" * 1000

        storage.artifacts.write_artifact(artifact_id, data)
        size = storage.artifacts.get_artifact_size(artifact_id)
        assert size == 1000

    def test_artifact_storage_list(self, storage):
        """Test listing artifacts."""
        storage.artifacts.write_artifact("export:2024-01", b"data1")
        storage.artifacts.write_artifact("export:2024-02", b"data2")
        storage.artifacts.write_artifact("recording:session1", b"data3")

        all_artifacts = storage.artifacts.list_artifacts()
        assert "export:2024-01" in all_artifacts
        assert "export:2024-02" in all_artifacts
        assert "recording:session1" in all_artifacts

        export_artifacts = storage.artifacts.list_artifacts("export:")
        assert "export:2024-01" in export_artifacts
        assert "export:2024-02" in export_artifacts
        assert "recording:session1" not in export_artifacts

    def test_distributed_locks_acquire_release(self, storage):
        """Test acquiring and releasing locks."""
        lock_id = "job:claim:123"

        lock_handle = storage.locks.acquire_lock(lock_id, duration_seconds=10)
        assert lock_handle is not None
        assert storage.locks.is_locked(lock_id) is True

        released = storage.locks.release_lock(lock_id)
        assert released is True
        assert storage.locks.is_locked(lock_id) is False

    def test_distributed_locks_already_held(self, storage):
        """Test acquiring an already-held lock."""
        lock_id = "job:exclusive:456"

        lock1 = storage.locks.acquire_lock(lock_id, duration_seconds=10)
        assert lock1 is not None

        lock2 = storage.locks.acquire_lock(lock_id, duration_seconds=10, wait_seconds=0)
        assert lock2 is None

        storage.locks.release_lock(lock_id)

    def test_distributed_locks_extend(self, storage):
        """Test extending a lock."""
        lock_id = "job:extend:789"

        lock_handle = storage.locks.acquire_lock(lock_id, duration_seconds=5)
        assert lock_handle is not None

        original_expiry = lock_handle.expires_at
        storage.locks.extend_lock(lock_id, duration_seconds=10)
        new_expiry = lock_handle.expires_at

        # New expiry should be greater (or approximately equal if extend was immediate)
        assert new_expiry >= original_expiry

        storage.locks.release_lock(lock_id)

    def test_distributed_locks_context_manager(self, storage):
        """Test lock context manager."""
        lock_id = "job:context:context_test"

        with storage.locks.lock_context(lock_id, duration_seconds=10) as lock_handle:
            if lock_handle:
                assert storage.locks.is_locked(lock_id) is True

        # Lock should be released after context
        assert storage.locks.is_locked(lock_id) is False


class LocalStorageBackendTests(StorageBackendTests):
    """Test suite for local (SQLite + filesystem) backend."""

    @pytest.fixture
    def storage(self) -> StorageBackendSet:
        """Create temporary storage for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backends = create_storage_backends(
                backend_type="local",
                # LocalStorageBackendSet uses get_hermes_home() by default,
                # so we'd need to patch it. For now, use default.
            )
            yield backends
            backends.close()

    def test_local_backend_created(self, storage):
        """Verify local backend was created."""
        from storage.local import (
            LocalStructuredStateBackend,
            LocalEncryptedBlobBackend,
            LocalArtifactStorageBackend,
            LocalDistributedLockBackend,
        )
        assert isinstance(storage.structured, LocalStructuredStateBackend)
        assert isinstance(storage.encrypted_blobs, LocalEncryptedBlobBackend)
        assert isinstance(storage.artifacts, LocalArtifactStorageBackend)
        assert isinstance(storage.locks, LocalDistributedLockBackend)


class PostgresStorageBackendTests(StorageBackendTests):
    """Test suite for PostgreSQL backend (placeholder for step 3+)."""

    @pytest.fixture(scope="session")
    def postgres_available(self):
        """Check if PostgreSQL is available for testing."""
        # This is a placeholder — in full implementation, check real Postgres
        import os
        return os.environ.get("HERMES_TEST_POSTGRES_URL") is not None

    @pytest.fixture
    def storage(self, postgres_available):
        """Create PostgreSQL storage for testing."""
        if not postgres_available:
            pytest.skip("PostgreSQL not available for testing")

        import os
        postgres_url = os.environ.get("HERMES_TEST_POSTGRES_URL")
        backends = create_storage_backends(
            backend_type="postgres",
            postgres_url=postgres_url,
        )
        yield backends
        backends.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
