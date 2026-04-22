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

| Variable | Default | Description |
|----------|---------|-------------|
| `HERMES_STORAGE_BACKEND` | `local` | `local` uses SQLite + filesystem under `HERMES_HOME`. Set to `postgres` to move all durable state to PostgreSQL. |
| `HERMES_STORAGE_POSTGRES_URL` | _(none)_ | Required when `HERMES_STORAGE_BACKEND=postgres`. Full DSN, e.g. `postgresql://user:pass@host:5432/hermes` |
| `HERMES_STATELESS` | _(unset)_ | Set to `1` to disable file-based log handlers (logs go to stdout/stderr only). Recommended alongside `postgres` backend. |

**Database auto-initialization:** When using `HERMES_STORAGE_BACKEND=postgres`, Hermes automatically creates all required tables on the first connection via `CREATE TABLE IF NOT EXISTS` statements. You only need to create the database and grant `CREATE TABLE` privileges. No manual schema scripts required.

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

### Completed (Steps 1–10)
✅ Storage abstraction layer — four backend contracts (`StructuredStateBackend`, `EncryptedBlobBackend`, `ArtifactStorageBackend`, `DistributedLockBackend`)
✅ Local backend — SQLite FTS5 + filesystem (default, zero-config)
✅ PostgreSQL backend — psycopg3, JSONB, pg_trgm, BYTEA, lock table with TTL; schema auto-created on first connect
✅ Factory function with environment variable selection
✅ Session migration — `hermes_state.py` (`SessionDB`) reads/writes via storage backend
✅ Cron migration — `cron/jobs.py` uses distributed locks + artifact storage for outputs
✅ Config / SOUL / memory migration — `hermes_cli/config.py`, `agent/prompt_builder.py`, `tools/memory_tool.py`
✅ Auth / adapter migration — `hermes_cli/auth.py` uses encrypted blobs backend
✅ Skills migration — `tools/skills_tool.py` and `tools/skill_manager_tool.py` use `SkillRegistry`
✅ Logging rework — `hermes_logging.py`: `configure_stateless_logging()`, `ArtifactLogger`, `HERMES_STATELESS` env var
✅ Migration tooling — `startup_init_backends()`, `HermesMigration.import_all()`, `validate_parity()`
✅ Integration tests — 32 tests (restart continuity across all subsystems)
✅ Multi-replica tests — 13 tests (distributed lock exclusivity, concurrent session writes)
✅ Durable-write regression tests — 15 tests (no unexpected disk writes in postgres mode)

## Testing

### Run the Full Test Suite
```bash
pytest tests/test_stateless_integration.py   # 32 tests — restart continuity
pytest tests/test_multi_replica.py           # 13 tests — distributed locks + concurrency
pytest tests/test_stateless_durable_writes.py # 15 tests — no unexpected disk writes
```

### Quick Smoke Test (Local Backend)
```bash
python3 -c "
from storage.factory import create_storage_backends

backends = create_storage_backends(backend_type='local')
backends.structured.set_json('test:key', {'value': 42})
assert backends.structured.get_json('test:key') == {'value': 42}
backends.encrypted_blobs.set_blob('secret', b'data')
assert backends.encrypted_blobs.get_blob('secret') == b'data'
backends.artifacts.write_artifact('artifact1', b'content')
assert backends.artifacts.read_artifact('artifact1') == b'content'
lock = backends.locks.acquire_lock('lock1', duration_seconds=10)
assert lock is not None
backends.close()
print('All tests passed!')
"
```

### Run Tests with PostgreSQL
```bash
export HERMES_TEST_POSTGRES_URL=postgresql://user:pass@localhost:5432/hermes_test
pytest tests/test_stateless_integration.py tests/test_multi_replica.py -v
```


## Design Decisions

1. **Four separate backends:** Isolates concerns (structured vs. sensitive vs. large vs. coordination), enabling independent optimization (e.g., encryption, compression, object storage).

2. **Key-value abstraction:** Simple and flexible, avoiding SQL complexity in higher layers. Backends can use arbitrary storage (files, SQL, object storage).

3. **No Redis:** PostgreSQL TTL-based lock table is sufficient for multi-instance coordination. Redis adds operational complexity.

4. **Object storage optional:** Large artifacts can defer to S3/GCS later, but core state stays in PostgreSQL.

5. **Backward compatible:** Local backend wraps existing behavior with no forced migration. All modules fall back to filesystem/SQLite when no backend is configured.
