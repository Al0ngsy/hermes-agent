# Storage Abstraction Layer

This module provides a unified interface for all durable state in Hermes, enabling seamless switching between local (SQLite + filesystem) and PostgreSQL backends for stateless operation.

## Architecture

The storage layer separates concerns into four independent backend contracts:

### 1. **StructuredStateBackend**
Stores JSON/dict data with full-text search capability.

**Used for:**
- Session metadata, message history
- Cron jobs and execution state
- Config files (SOUL.md, profile settings)
- Memory entries and user knowledge
- Skills manifest and hub metadata
- Webhook state and profile metadata

**Interface:**
```python
def get_json(key: str, default: Optional[Dict]) -> Optional[Dict]
def set_json(key: str, value: Dict) -> None
def delete_json(key: str) -> bool
def list_keys(prefix: str = "") -> List[str]
def search(query: str, prefix: str = "", limit: int = 100) -> List[Tuple[str, Dict]]
def batch_get(keys: List[str]) -> Dict[str, Dict]
def batch_set(items: Dict[str, Dict]) -> None
```

**Backends:**
- **Local:** SQLite table with FTS5 full-text search
- **PostgreSQL:** JSONB columns with pg_trgm for search (future)

### 2. **EncryptedBlobBackend**
Stores encrypted sensitive data, automatically at rest.

**Used for:**
- API keys and OAuth tokens
- Adapter session state (WhatsApp, Matrix, Weixin, etc.)
- Pairing and browser persistence data
- Credentials and authentication blobs

**Interface:**
```python
def get_blob(key: str, default: Optional[bytes] = None) -> Optional[bytes]
def set_blob(key: str, value: bytes) -> None
def delete_blob(key: str) -> bool
def list_keys(prefix: str = "") -> List[str]
def batch_get(keys: List[str]) -> Dict[str, Optional[bytes]]
def batch_delete(keys: List[str]) -> int
```

**Backends:**
- **Local:** File-based storage under `HERMES_HOME/storage/blobs/` with 0600 permissions
- **PostgreSQL:** BYTEA columns, optionally compressed (future)

### 3. **ArtifactStorageBackend**
Stores large binary artifacts with optional compression/deduplication.

**Used for:**
- Session transcripts and exports
- Browser recordings and screenshots
- Large job outputs
- Model reasoning traces

**Interface:**
```python
def write_artifact(artifact_id: str, data: bytes) -> str
def read_artifact(artifact_id: str) -> Optional[bytes]
def delete_artifact(artifact_id: str) -> bool
def list_artifacts(prefix: str = "") -> List[str]
def get_artifact_size(artifact_id: str) -> Optional[int]
```

**Backends:**
- **Local:** Direct filesystem writes under `HERMES_HOME/storage/artifacts/`
- **PostgreSQL:** BYTEA columns or defer to object storage (S3, GCS) (future)

### 4. **DistributedLockBackend**
Multi-instance coordination via distributed locks with TTL.

**Used for:**
- Cron job claiming (single-fire execution)
- One-shot job deduplication
- Instance heartbeats and leader election

**Interface:**
```python
def acquire_lock(lock_id: str, duration_seconds: float, wait_seconds: float = 0) -> Optional[LockHandle]
def release_lock(lock_id: str) -> bool
def extend_lock(lock_id: str, duration_seconds: float) -> bool
def is_locked(lock_id: str) -> bool
@contextmanager
def lock_context(lock_id: str, duration_seconds: float = 60, wait_seconds: float = 0)
```

**Backends:**
- **Local:** SQLite with advisory lock semantics (single-machine only)
- **PostgreSQL:** Advisory locks with heartbeat table (true multi-instance) (future)

## Usage

### Initialization

```python
from storage.factory import create_storage_backends

# Automatically detect backend from HERMES_STORAGE_BACKEND env var
backends = create_storage_backends()

# Or explicitly specify
backends = create_storage_backends(
    backend_type="postgres",
    postgres_url="postgresql://user:pass@localhost/hermes"
)
```

### Environment Variables

- **HERMES_STORAGE_BACKEND:** "local" (default) or "postgres"
- **HERMES_STORAGE_POSTGRES_URL:** PostgreSQL connection string for postgres backend

### Structured State Example

```python
# Store session metadata
session_data = {
    "id": "session_123",
    "started_at": time.time(),
    "user_id": "user_456",
    "messages": [],
}
backends.structured.set_json("sessions:session_123", session_data)

# Search sessions by user
results = backends.structured.search(
    query="user_456",
    prefix="sessions:",
    limit=10
)

# Batch operations
sessions = backends.structured.batch_get([
    "sessions:session_123",
    "sessions:session_456",
])
```

### Encrypted Blobs Example

```python
# Store API key
token = b"sk-1234567890abcdef"
backends.encrypted_blobs.set_blob("auth:openrouter:api_key", token)

# Retrieve securely
token = backends.encrypted_blobs.get_blob("auth:openrouter:api_key")
```

### Distributed Locks Example

```python
# Claim a cron job (mutual exclusion)
lock = backends.locks.acquire_lock(
    lock_id="cron:daily_task",
    duration_seconds=3600,
    wait_seconds=5  # Wait up to 5 seconds if locked
)

if lock:
    try:
        # Execute the job
        run_cron_job()
    finally:
        backends.locks.release_lock(lock.lock_id)

# Or use context manager for automatic cleanup
with backends.locks.lock_context("cron:daily_task", duration_seconds=3600):
    run_cron_job()
```

## Migration Path

### Current State (Local Backend Only)

Existing code continues to work unchanged:
- SessionDB remains SQLite-backed
- HERMES_HOME files are read/written normally
- No changes required for backward compatibility

### Stateless Deployment (PostgreSQL Backend)

To run Hermes without persistent volumes:

1. Set environment variables:
   ```bash
   export HERMES_STORAGE_BACKEND=postgres
   export HERMES_STORAGE_POSTGRES_URL=postgresql://...
   ```

2. Hermes automatically uses PostgreSQL instead of local disk

3. No volume mounts needed; all state is durable in the database

## Implementation Status

### Completed (Step 1-2)
✅ Storage abstraction layer with four backend contracts
✅ Local backend (SQLite + filesystem)
✅ Factory function with environment variable support
✅ Test suite (local backend tested)

### Planned (Steps 3-10)
⬜ PostgreSQL backend implementation (structured, blobs, artifacts, locks)
⬜ Migrate SessionDB to use storage layer
⬜ Migrate cron/jobs.py to use storage layer
⬜ Migrate config/SOUL/memory to use storage layer
⬜ Migrate auth/adapter state to use storage layer
⬜ Migrate skills metadata to use storage layer
⬜ Rework logging and artifacts
⬜ Migration tooling (import existing HERMES_HOME)
⬜ Integration tests (multi-instance, restart, recovery)
⬜ Regression tests (zero-PVC assertions)

## Testing

### Run Local Backend Tests
```bash
python3 -c "
from storage.factory import create_storage_backends

backends = create_storage_backends(backend_type='local')

# Test structured state
backends.structured.set_json('test:key', {'value': 42})
assert backends.structured.get_json('test:key') == {'value': 42}

# Test encrypted blob
backends.encrypted_blobs.set_blob('secret', b'data')
assert backends.encrypted_blobs.get_blob('secret') == b'data'

# Test artifacts
backends.artifacts.write_artifact('artifact1', b'content')
assert backends.artifacts.read_artifact('artifact1') == b'content'

# Test locks
lock = backends.locks.acquire_lock('lock1', duration_seconds=10)
assert lock is not None

backends.close()
print('All tests passed!')
"
```

### Future: Run Full Test Suite with PostgreSQL
```bash
export HERMES_TEST_POSTGRES_URL=postgresql://...
pytest tests/test_storage_backends.py -v
```

## Design Decisions

1. **Four separate backends:** Isolates concerns (structured vs. sensitive vs. large vs. coordination), enabling independent optimization (e.g., encryption, compression, object storage).

2. **Key-value abstraction:** Simple and flexible, avoiding SQL complexity in higher layers. Backends can use arbitrary storage (files, SQL, object storage).

3. **No Redis:** PostgreSQL advisory locks are sufficient for multi-instance coordination. Redis adds operational complexity.

4. **Object storage optional:** Large artifacts can defer to S3/GCS later, but core state stays in PostgreSQL.

5. **Backward compatible:** Local backend wraps existing behavior, no forced migration.

## Next Steps

See `.plans/hermes-stateless-external-state.md` for full roadmap.
