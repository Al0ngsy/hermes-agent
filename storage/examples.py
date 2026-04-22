"""
Example: Using the storage abstraction layer in existing code.

This demonstrates how Hermes modules will gradually migrate to use the
storage backends instead of direct file I/O and SQLite access.
"""

from storage.factory import create_storage_backends
from typing import List, Dict, Optional
import time


# ==============================================================================
# Example 1: Session Management Migration
# ==============================================================================

class SessionManager:
    """Example of migrating session storage to use the abstraction layer."""

    def __init__(self):
        self.storage = create_storage_backends()

    def save_session(self, session_id: str, session_data: Dict) -> None:
        """Save a session to structured storage."""
        key = f"sessions:{session_id}"
        self.storage.structured.set_json(key, session_data)

    def load_session(self, session_id: str) -> Optional[Dict]:
        """Load a session from structured storage."""
        key = f"sessions:{session_id}"
        return self.storage.structured.get_json(key)

    def search_sessions(self, query: str, limit: int = 100) -> List[Dict]:
        """Search sessions by text content."""
        results = self.storage.structured.search(
            query=query,
            prefix="sessions:",
            limit=limit
        )
        return [data for _, data in results]

    def list_sessions(self) -> List[str]:
        """List all session IDs."""
        keys = self.storage.structured.list_keys("sessions:")
        return [k.replace("sessions:", "") for k in keys]


# ==============================================================================
# Example 2: Cron Job Coordination with Distributed Locks
# ==============================================================================

class CronScheduler:
    """Example of using distributed locks for cron job coordination."""

    def __init__(self):
        self.storage = create_storage_backends()

    def save_job(self, job_id: str, job_config: Dict) -> None:
        """Save a cron job configuration."""
        key = f"cron:jobs:{job_id}"
        self.storage.structured.set_json(key, job_config)

    def claim_job(self, job_id: str, duration_seconds: int = 3600) -> bool:
        """
        Try to claim a job for execution (mutual exclusion).

        Returns True if this instance claims the job, False if another
        instance already has it.
        """
        lock_id = f"cron:claim:{job_id}"

        # Try to acquire lock with no wait (non-blocking)
        lock = self.storage.locks.acquire_lock(
            lock_id,
            duration_seconds=duration_seconds,
            wait_seconds=0
        )

        return lock is not None

    def execute_job(self, job_id: str) -> bool:
        """
        Execute a cron job with automatic cleanup.

        Uses context manager to ensure lock is always released.
        """
        lock_id = f"cron:claim:{job_id}"

        with self.storage.locks.lock_context(lock_id, duration_seconds=3600) as lock:
            if not lock:
                # Another instance claimed this job
                return False

            # Safe to execute — we hold the lock
            job = self.storage.structured.get_json(f"cron:jobs:{job_id}")
            if job:
                print(f"Executing job: {job_id}")
                # ... run job ...
                return True

        return False


# ==============================================================================
# Example 3: Credential Management with Encrypted Blobs
# ==============================================================================

class CredentialStore:
    """Example of storing encrypted credentials."""

    def __init__(self):
        self.storage = create_storage_backends()

    def save_api_key(self, provider: str, api_key: str) -> None:
        """Store an API key securely."""
        key = f"auth:api_key:{provider}"
        self.storage.encrypted_blobs.set_blob(key, api_key.encode("utf-8"))

    def load_api_key(self, provider: str) -> Optional[str]:
        """Load an API key securely."""
        key = f"auth:api_key:{provider}"
        blob = self.storage.encrypted_blobs.get_blob(key)
        return blob.decode("utf-8") if blob else None

    def save_oauth_token(self, provider: str, token_data: bytes) -> None:
        """Store OAuth token data (already encrypted/serialized)."""
        key = f"auth:oauth:{provider}"
        self.storage.encrypted_blobs.set_blob(key, token_data)

    def load_oauth_token(self, provider: str) -> Optional[bytes]:
        """Load OAuth token data."""
        key = f"auth:oauth:{provider}"
        return self.storage.encrypted_blobs.get_blob(key)

    def list_providers(self) -> List[str]:
        """List all configured auth providers."""
        keys = self.storage.encrypted_blobs.list_keys("auth:api_key:")
        return [k.replace("auth:api_key:", "") for k in keys]


# ==============================================================================
# Example 4: Session Export and Artifact Storage
# ==============================================================================

class SessionExporter:
    """Example of storing large session exports."""

    def __init__(self):
        self.storage = create_storage_backends()

    def export_session_transcript(self, session_id: str, transcript_bytes: bytes) -> str:
        """Export a session transcript and return its URI."""
        artifact_id = f"export:session:{session_id}:{int(time.time())}"
        uri = self.storage.artifacts.write_artifact(artifact_id, transcript_bytes)
        return uri

    def load_session_transcript(self, session_id: str) -> Optional[bytes]:
        """Load the most recent session transcript."""
        # List all exports for this session
        artifacts = self.storage.artifacts.list_artifacts(f"export:session:{session_id}:")

        if not artifacts:
            return None

        # Get the most recent (assuming timestamp in artifact ID)
        latest = sorted(artifacts)[-1]
        return self.storage.artifacts.read_artifact(latest)

    def cleanup_old_transcripts(self, session_id: str, keep_count: int = 5) -> int:
        """Delete old transcripts, keeping only the most recent."""
        artifacts = self.storage.artifacts.list_artifacts(f"export:session:{session_id}:")
        artifacts_sorted = sorted(artifacts)

        deleted = 0
        for artifact_id in artifacts_sorted[:-keep_count]:
            if self.storage.artifacts.delete_artifact(artifact_id):
                deleted += 1

        return deleted


# ==============================================================================
# Example 5: Config and SOUL Management
# ==============================================================================

class ConfigManager:
    """Example of managing Hermes configuration and SOUL."""

    def __init__(self):
        self.storage = create_storage_backends()

    def load_config(self) -> Dict:
        """Load Hermes config."""
        config = self.storage.structured.get_json("config:main", default={})
        return config

    def save_config(self, config: Dict) -> None:
        """Save Hermes config."""
        self.storage.structured.set_json("config:main", config)

    def load_soul(self) -> str:
        """Load SOUL (system of understanding and learning)."""
        soul_data = self.storage.structured.get_json("config:soul", default={})
        return soul_data.get("content", "")

    def save_soul(self, soul_content: str) -> None:
        """Save SOUL."""
        soul_data = {
            "content": soul_content,
            "updated_at": time.time(),
        }
        self.storage.structured.set_json("config:soul", soul_data)

    def load_memory(self) -> Dict:
        """Load built-in memory entries."""
        memory = self.storage.structured.get_json("config:memory", default={})
        return memory

    def add_memory_entry(self, key: str, entry: str) -> None:
        """Add an entry to memory."""
        memory = self.load_memory()
        memory[key] = {
            "content": entry,
            "created_at": time.time(),
        }
        self.storage.structured.set_json("config:memory", memory)


# ==============================================================================
# Main: Demonstration
# ==============================================================================

def main():
    """Demonstrate the storage abstraction in action."""

    print("=" * 60)
    print("Storage Abstraction Layer - Usage Examples")
    print("=" * 60)

    # Initialize storage
    storage = create_storage_backends()
    print(f"\nStorage backend type: {type(storage.structured).__name__}")

    # Example 1: Sessions
    print("\n1. Session Management:")
    sessions = SessionManager()
    sessions.save_session("session_001", {
        "user_id": "user_123",
        "started_at": time.time(),
        "messages": ["Hello", "How are you?"]
    })
    print(f"  Loaded session: {sessions.load_session('session_001')}")

    # Example 2: Cron
    print("\n2. Cron Job Coordination:")
    cron = CronScheduler()
    cron.save_job("daily_cleanup", {
        "schedule": "0 2 * * *",
        "skill": "cleanup",
    })
    if cron.execute_job("daily_cleanup"):
        print("  ✓ Executed cron job")
    else:
        print("  ✗ Job already claimed by another instance")

    # Example 3: Credentials
    print("\n3. Credential Storage:")
    creds = CredentialStore()
    creds.save_api_key("openai", "sk-test123")
    key = creds.load_api_key("openai")
    print(f"  Loaded API key: {key[:6]}..." if key else "  Failed to load key")

    # Example 4: Artifacts
    print("\n4. Session Exports:")
    exporter = SessionExporter()
    uri = exporter.export_session_transcript("session_001", b"Transcript content")
    print(f"  Exported to: {uri}")

    # Example 5: Config
    print("\n5. Config & SOUL:")
    config = ConfigManager()
    config.save_soul("I am Hermes, an AI agent.")
    soul = config.load_soul()
    print(f"  Loaded SOUL: {soul}")

    storage.close()
    print("\n" + "=" * 60)
    print("All examples completed! ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
