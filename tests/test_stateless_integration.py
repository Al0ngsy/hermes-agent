"""
Stateless integration tests for the Hermes storage abstraction layer.

Simulates container restart scenarios:
  1. Initialize local backends (SQLite + filesystem)
  2. Write state across all subsystems
  3. Close backends (simulate container stop)
  4. Re-open backends pointing to the same directory
  5. Verify all state survived the restart

These tests run entirely with the local backend — no PostgreSQL required.
PostgreSQL tests are explicitly skipped.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_temp_backends(tmp_path: Path):
    """Create a LocalStorageBackendSet pointing to a temp directory."""
    from storage.local import LocalStorageBackendSet

    return LocalStorageBackendSet(hermes_home=tmp_path).create()


def _reset_cron_globals():
    """Reset cron.jobs module-level storage globals."""
    try:
        import cron.jobs as cron_jobs

        cron_jobs._structured = None
        cron_jobs._artifacts = None
        cron_jobs._locks = None
    except Exception:
        pass


def _reset_config_globals():
    """Reset hermes_cli.config module-level storage backend."""
    try:
        from hermes_cli import config as hermes_config

        hermes_config._config_storage_backend = None
    except Exception:
        pass


def _reset_auth_globals():
    """Reset hermes_cli.auth module-level auth backend."""
    try:
        from hermes_cli import auth as hermes_auth

        hermes_auth._auth_backend = None
    except Exception:
        pass


def _reset_soul_globals():
    """Reset agent.prompt_builder module-level soul backend."""
    try:
        from agent import prompt_builder

        prompt_builder._soul_backend = None
    except Exception:
        pass


def _reset_migration_globals():
    """Reset storage.migration._backends_initialized guard."""
    try:
        import storage.migration as migration_mod

        migration_mod._backends_initialized = False
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_storage_module_globals():
    """Reset all storage-wiring module globals before and after each test.

    Prevents state leakage between tests that wire different backends.
    """
    _reset_cron_globals()
    _reset_config_globals()
    _reset_auth_globals()
    _reset_soul_globals()
    _reset_migration_globals()
    yield
    _reset_cron_globals()
    _reset_config_globals()
    _reset_auth_globals()
    _reset_soul_globals()
    _reset_migration_globals()


# ---------------------------------------------------------------------------
# TestSessionContinuity
# ---------------------------------------------------------------------------


class TestSessionContinuity:
    """Verify session metadata and messages survive a backend restart."""

    def test_session_survives_restart(self, tmp_path):
        """Create session + messages, close backends, reopen, verify data."""
        from hermes_state import SessionDB

        storage_dir = tmp_path / "hermes"

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        db1 = SessionDB(backend=backends1.structured)

        session_id = "test-session-abc123"
        db1.create_session(session_id, source="cli", model="claude-opus-4")
        db1.append_message(session_id, role="user", content="Hello, Hermes!")
        db1.append_message(session_id, role="assistant", content="Hello! How can I help?")
        db1.append_message(session_id, role="user", content="What is 2 + 2?")

        # Verify pre-close
        assert db1.get_session(session_id) is not None
        assert len(db1.get_messages(session_id)) == 3

        backends1.close()

        # --- run 2: read ---
        backends2 = make_temp_backends(storage_dir)
        db2 = SessionDB(backend=backends2.structured)

        session = db2.get_session(session_id)
        assert session is not None, "Session not found after restart"
        assert session["id"] == session_id
        assert session["source"] == "cli"
        assert session["model"] == "claude-opus-4"

        messages = db2.get_messages(session_id)
        assert len(messages) == 3, f"Expected 3 messages, got {len(messages)}"
        assert messages[0]["role"] == "user"
        assert messages[0]["content"] == "Hello, Hermes!"
        assert messages[1]["role"] == "assistant"
        assert messages[2]["role"] == "user"

        backends2.close()

    def test_session_search_survives_restart(self, tmp_path):
        """Write sessions, close, reopen, verify search returns results."""
        from hermes_state import SessionDB

        storage_dir = tmp_path / "hermes"

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        db1 = SessionDB(backend=backends1.structured)

        session_id = "search-session-xyz789"
        db1.create_session(session_id, source="gateway", model="gpt-5")
        db1.append_message(
            session_id, role="user", content="Tell me about quantum computing"
        )

        backends1.close()

        # --- run 2: search ---
        backends2 = make_temp_backends(storage_dir)
        db2 = SessionDB(backend=backends2.structured)

        sessions = db2.search_sessions(limit=10)
        assert len(sessions) >= 1
        session_ids = [s["id"] for s in sessions]
        assert session_id in session_ids

        # Filter by source
        gateway_sessions = db2.search_sessions(source="gateway", limit=10)
        gw_ids = [s["id"] for s in gateway_sessions]
        assert session_id in gw_ids

        # Non-existent source returns empty
        cli_sessions = db2.search_sessions(source="cli", limit=10)
        cli_ids = [s["id"] for s in cli_sessions]
        assert session_id not in cli_ids

        backends2.close()

    def test_multiple_sessions_survive_restart(self, tmp_path):
        """Multiple sessions survive a backend close/reopen."""
        from hermes_state import SessionDB

        storage_dir = tmp_path / "hermes"

        session_ids = ["multi-sess-001", "multi-sess-002", "multi-sess-003"]

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        db1 = SessionDB(backend=backends1.structured)

        for i, sid in enumerate(session_ids):
            db1.create_session(sid, source="cli", model=f"model-{i}")
            db1.append_message(sid, role="user", content=f"Message in session {i}")

        backends1.close()

        # --- run 2: read ---
        backends2 = make_temp_backends(storage_dir)
        db2 = SessionDB(backend=backends2.structured)

        assert db2.session_count() == 3

        for sid in session_ids:
            s = db2.get_session(sid)
            assert s is not None, f"Session {sid} missing after restart"
            msgs = db2.get_messages(sid)
            assert len(msgs) == 1

        backends2.close()

    def test_session_end_state_survives_restart(self, tmp_path):
        """Session end reason persists across restart."""
        from hermes_state import SessionDB

        storage_dir = tmp_path / "hermes"

        # --- run 1: write and end ---
        backends1 = make_temp_backends(storage_dir)
        db1 = SessionDB(backend=backends1.structured)

        session_id = "ended-session-001"
        db1.create_session(session_id, source="cli")
        db1.end_session(session_id, end_reason="user_request")

        session = db1.get_session(session_id)
        assert session["end_reason"] == "user_request"
        backends1.close()

        # --- run 2: verify end state ---
        backends2 = make_temp_backends(storage_dir)
        db2 = SessionDB(backend=backends2.structured)

        session = db2.get_session(session_id)
        assert session is not None
        assert session["end_reason"] == "user_request"
        assert session["ended_at"] is not None
        backends2.close()

    def test_fts_search_across_messages(self, tmp_path):
        """Full-text search across message content works after restart."""
        from hermes_state import SessionDB

        storage_dir = tmp_path / "hermes"

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        db1 = SessionDB(backend=backends1.structured)

        session_id = "fts-session-001"
        db1.create_session(session_id, source="cli")
        db1.append_message(session_id, role="user", content="uniquequantumstring is interesting")

        backends1.close()

        # --- run 2: FTS search directly on structured backend ---
        backends2 = make_temp_backends(storage_dir)

        results = backends2.structured.search("uniquequantumstring")
        assert len(results) > 0, "FTS search returned no results after restart"

        backends2.close()


# ---------------------------------------------------------------------------
# TestCronContinuity
# ---------------------------------------------------------------------------


class TestCronContinuity:
    """Verify cron jobs survive a backend restart and locking works."""

    def test_cron_job_survives_restart(self, tmp_path):
        """Add a cron job, close backends, reopen, verify job is still there."""
        import cron.jobs as cron_jobs

        storage_dir = tmp_path / "hermes"

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        cron_jobs.init_storage(
            backends1.structured, backends1.artifacts, backends1.locks
        )

        job_id = "test-job-continuity-001"
        job_dict = {
            "id": job_id,
            "name": "Test Continuity Job",
            "prompt": "Echo hello",
            "schedule": {"kind": "interval", "minutes": 60, "display": "every 60m"},
            "skills": [],
            "skill": None,
            "model": None,
            "provider": None,
            "base_url": None,
            "script": None,
            "schedule_display": "every 60m",
            "repeat": {"times": None, "completed": 0},
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "created_at": "2025-01-01T00:00:00+00:00",
            "next_run_at": None,
            "last_run_at": None,
            "last_status": None,
            "last_error": None,
            "last_delivery_error": None,
            "deliver": "local",
            "origin": None,
        }
        added = cron_jobs.add_job(job_dict)
        assert added["id"] == job_id

        backends1.close()
        _reset_cron_globals()

        # --- run 2: read ---
        backends2 = make_temp_backends(storage_dir)
        cron_jobs.init_storage(
            backends2.structured, backends2.artifacts, backends2.locks
        )

        retrieved = cron_jobs.get_job(job_id)
        assert retrieved is not None, f"Job {job_id} missing after restart"
        assert retrieved["id"] == job_id
        assert retrieved["name"] == "Test Continuity Job"
        assert retrieved["prompt"] == "Echo hello"
        assert retrieved["enabled"] is True

        jobs = cron_jobs.list_jobs(include_disabled=True)
        job_ids = [j["id"] for j in jobs]
        assert job_id in job_ids

        backends2.close()

    def test_cron_multiple_jobs_survive_restart(self, tmp_path):
        """Multiple cron jobs survive a backend restart."""
        import cron.jobs as cron_jobs

        storage_dir = tmp_path / "hermes"

        job_ids = ["multi-cron-001", "multi-cron-002", "multi-cron-003"]

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        cron_jobs.init_storage(
            backends1.structured, backends1.artifacts, backends1.locks
        )

        for jid in job_ids:
            cron_jobs.add_job(
                {
                    "id": jid,
                    "name": f"Job {jid}",
                    "prompt": f"Task {jid}",
                    "schedule": {"kind": "interval", "minutes": 30, "display": "every 30m"},
                    "skills": [],
                    "skill": None,
                    "model": None,
                    "provider": None,
                    "base_url": None,
                    "script": None,
                    "schedule_display": "every 30m",
                    "repeat": {"times": None, "completed": 0},
                    "enabled": True,
                    "state": "scheduled",
                    "paused_at": None,
                    "paused_reason": None,
                    "created_at": "2025-01-01T00:00:00+00:00",
                    "next_run_at": None,
                    "last_run_at": None,
                    "last_status": None,
                    "last_error": None,
                    "last_delivery_error": None,
                    "deliver": "local",
                    "origin": None,
                }
            )

        backends1.close()
        _reset_cron_globals()

        # --- run 2: read ---
        backends2 = make_temp_backends(storage_dir)
        cron_jobs.init_storage(
            backends2.structured, backends2.artifacts, backends2.locks
        )

        all_jobs = cron_jobs.list_jobs(include_disabled=True)
        found_ids = {j["id"] for j in all_jobs}
        for jid in job_ids:
            assert jid in found_ids, f"Job {jid} missing after restart"

        backends2.close()

    def test_distributed_lock_prevents_double_claim(self, tmp_path):
        """One instance can claim a job; a second claim attempt returns None."""
        storage_dir = tmp_path / "hermes"
        backends = make_temp_backends(storage_dir)

        lock_id = "cron:claim:exclusive-job-001"

        # First claim succeeds
        handle1 = backends.locks.acquire_lock(lock_id, duration_seconds=60)
        assert handle1 is not None, "First lock acquisition should succeed"
        assert backends.locks.is_locked(lock_id) is True

        # Second claim with no wait returns None
        handle2 = backends.locks.acquire_lock(lock_id, duration_seconds=60, wait_seconds=0)
        assert handle2 is None, "Second acquisition on held lock should return None"

        # Release and verify
        backends.locks.release_lock(lock_id)
        assert backends.locks.is_locked(lock_id) is False

        backends.close()

    def test_lock_survives_restart(self, tmp_path):
        """Distributed lock state persists across backend close/reopen."""
        storage_dir = tmp_path / "hermes"

        # --- run 1: acquire lock ---
        backends1 = make_temp_backends(storage_dir)
        lock_id = "cron:claim:persistent-lock-001"
        handle = backends1.locks.acquire_lock(lock_id, duration_seconds=3600)
        assert handle is not None
        backends1.close()

        # --- run 2: lock should still be held ---
        backends2 = make_temp_backends(storage_dir)
        assert backends2.locks.is_locked(lock_id) is True, "Lock should still be held after restart"

        # Second acquire should still fail
        handle2 = backends2.locks.acquire_lock(lock_id, duration_seconds=60, wait_seconds=0)
        assert handle2 is None

        backends2.close()


# ---------------------------------------------------------------------------
# TestConfigSoulContinuity
# ---------------------------------------------------------------------------


class TestConfigSoulContinuity:
    """Verify config and SOUL content survive a backend restart."""

    def test_config_survives_restart(self, tmp_path):
        """Save config via ConfigBackend, close, reopen, verify config loads."""
        from hermes_cli.config import ConfigBackend

        storage_dir = tmp_path / "hermes"

        config_data = {
            "model": "claude-opus-4",
            "toolsets": ["terminal", "file"],
            "max_turns": 50,
            "memory": {"memory_enabled": True},
        }

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        cb1 = ConfigBackend(backends1.structured)
        cb1.save_config(config_data)
        backends1.close()

        # --- run 2: read ---
        backends2 = make_temp_backends(storage_dir)
        cb2 = ConfigBackend(backends2.structured)
        loaded = cb2.load_config()

        assert loaded == config_data, f"Config mismatch: {loaded!r} != {config_data!r}"
        backends2.close()

    def test_soul_content_survives_restart(self, tmp_path):
        """Save SOUL.md content, close, reopen, verify it loads correctly."""
        from agent import prompt_builder

        storage_dir = tmp_path / "hermes"
        soul_content = "# My Custom SOUL\n\nYou are a helpful assistant.\n"

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        prompt_builder.init_storage(backends1.structured)
        backends1.structured.set_json("config:soul", {"content": soul_content})
        backends1.close()
        _reset_soul_globals()

        # --- run 2: read via prompt_builder ---
        backends2 = make_temp_backends(storage_dir)
        prompt_builder.init_storage(backends2.structured)
        loaded_soul = prompt_builder.load_soul_md()

        assert loaded_soul is not None, "SOUL content should be returned after restart"
        assert "Custom SOUL" in loaded_soul, f"SOUL content mismatch: {loaded_soul!r}"

        backends2.close()

    def test_config_and_soul_together_survive_restart(self, tmp_path):
        """Config and SOUL data both survive the same backend restart."""
        from hermes_cli.config import ConfigBackend

        storage_dir = tmp_path / "hermes"

        config_data = {"model": "claude-opus-4", "toolsets": ["file"]}
        soul_content = "# Soul\n\nBe kind and helpful.\n"

        # --- run 1: write both ---
        backends1 = make_temp_backends(storage_dir)

        cb1 = ConfigBackend(backends1.structured)
        cb1.save_config(config_data)
        backends1.structured.set_json("config:soul", {"content": soul_content})

        backends1.close()

        # --- run 2: verify both ---
        backends2 = make_temp_backends(storage_dir)

        cb2 = ConfigBackend(backends2.structured)
        loaded_config = cb2.load_config()
        assert loaded_config == config_data

        soul_data = backends2.structured.get_json("config:soul")
        assert soul_data is not None
        assert soul_data["content"] == soul_content

        backends2.close()

    def test_config_update_persists(self, tmp_path):
        """Config can be updated and the updated version survives restart."""
        from hermes_cli.config import ConfigBackend

        storage_dir = tmp_path / "hermes"

        # --- run 1: write initial config ---
        backends1 = make_temp_backends(storage_dir)
        cb1 = ConfigBackend(backends1.structured)
        cb1.save_config({"model": "gpt-5", "max_turns": 10})
        backends1.close()

        # --- run 2: update config ---
        backends2 = make_temp_backends(storage_dir)
        cb2 = ConfigBackend(backends2.structured)
        updated = {"model": "claude-opus-4", "max_turns": 100}
        cb2.save_config(updated)
        backends2.close()

        # --- run 3: verify update persisted ---
        backends3 = make_temp_backends(storage_dir)
        cb3 = ConfigBackend(backends3.structured)
        loaded = cb3.load_config()
        assert loaded["model"] == "claude-opus-4"
        assert loaded["max_turns"] == 100
        backends3.close()


# ---------------------------------------------------------------------------
# TestAuthContinuity
# ---------------------------------------------------------------------------


class TestAuthContinuity:
    """Verify auth store survives a backend restart."""

    def test_auth_store_survives_restart(self, tmp_path):
        """Save auth store, close backends, reopen, verify credentials intact."""
        from hermes_cli.auth import AuthBackend

        storage_dir = tmp_path / "hermes"

        auth_store = {
            "providers": {
                "anthropic": {"api_key": "sk-ant-test123"},
                "openai": {"api_key": "sk-openai-test456"},
            },
            "default_provider": "anthropic",
        }

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        ab1 = AuthBackend(backends1.encrypted_blobs)
        ab1.save_auth_store(auth_store)
        backends1.close()

        # --- run 2: read ---
        backends2 = make_temp_backends(storage_dir)
        ab2 = AuthBackend(backends2.encrypted_blobs)
        loaded = ab2.load_auth_store()

        assert loaded == auth_store, f"Auth store mismatch after restart: {loaded!r}"
        assert loaded["providers"]["anthropic"]["api_key"] == "sk-ant-test123"
        assert loaded["providers"]["openai"]["api_key"] == "sk-openai-test456"
        assert loaded["default_provider"] == "anthropic"
        backends2.close()

    def test_auth_store_empty_if_never_written(self, tmp_path):
        """Auth store returns empty dict if nothing was written."""
        from hermes_cli.auth import AuthBackend

        storage_dir = tmp_path / "hermes"
        backends = make_temp_backends(storage_dir)
        ab = AuthBackend(backends.encrypted_blobs)
        loaded = ab.load_auth_store()
        assert loaded == {}, f"Expected empty dict, got {loaded!r}"
        backends.close()

    def test_auth_store_overwrite_persists(self, tmp_path):
        """Overwriting auth store, the latest version survives restart."""
        from hermes_cli.auth import AuthBackend

        storage_dir = tmp_path / "hermes"

        # --- run 1: write initial ---
        backends1 = make_temp_backends(storage_dir)
        ab1 = AuthBackend(backends1.encrypted_blobs)
        ab1.save_auth_store({"providers": {"old": {"api_key": "old-key"}}})
        backends1.close()

        # --- run 2: overwrite ---
        backends2 = make_temp_backends(storage_dir)
        ab2 = AuthBackend(backends2.encrypted_blobs)
        ab2.save_auth_store({"providers": {"new": {"api_key": "new-key"}}})
        backends2.close()

        # --- run 3: verify latest ---
        backends3 = make_temp_backends(storage_dir)
        ab3 = AuthBackend(backends3.encrypted_blobs)
        loaded = ab3.load_auth_store()
        assert "new" in loaded["providers"]
        assert "old" not in loaded["providers"]
        backends3.close()

    def test_auth_store_with_many_providers(self, tmp_path):
        """Auth store with many providers survives restart intact."""
        from hermes_cli.auth import AuthBackend

        storage_dir = tmp_path / "hermes"

        providers = {
            f"provider-{i}": {"api_key": f"key-{i}", "base_url": f"https://api{i}.example.com"}
            for i in range(20)
        }
        auth_store = {"providers": providers, "version": 2}

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        ab1 = AuthBackend(backends1.encrypted_blobs)
        ab1.save_auth_store(auth_store)
        backends1.close()

        # --- run 2: read ---
        backends2 = make_temp_backends(storage_dir)
        ab2 = AuthBackend(backends2.encrypted_blobs)
        loaded = ab2.load_auth_store()

        assert loaded["version"] == 2
        assert len(loaded["providers"]) == 20
        for i in range(20):
            name = f"provider-{i}"
            assert loaded["providers"][name]["api_key"] == f"key-{i}"
        backends2.close()


# ---------------------------------------------------------------------------
# TestSkillsContinuity
# ---------------------------------------------------------------------------


class TestSkillsContinuity:
    """Verify installed skills survive a backend restart."""

    def test_skill_survives_restart(self, tmp_path):
        """Install a skill, close backends, reopen, verify skill is present."""
        from storage.skill_registry import SkillRegistry

        storage_dir = tmp_path / "hermes"

        manifest = {
            "name": "web-search",
            "version": "1.0.0",
            "description": "Searches the web",
            "author": "test",
        }

        # --- run 1: install ---
        backends1 = make_temp_backends(storage_dir)
        registry1 = SkillRegistry(backends1.structured)
        registry1.install_skill("web-search", manifest, source="hub")
        backends1.close()

        # --- run 2: verify ---
        backends2 = make_temp_backends(storage_dir)
        registry2 = SkillRegistry(backends2.structured)

        skills = registry2.list_skills()
        skill_ids = [s["id"] for s in skills]
        assert "web-search" in skill_ids, "Skill missing after restart"

        record = registry2.get_skill("web-search")
        assert record is not None
        assert record["manifest"]["name"] == "web-search"
        assert record["manifest"]["version"] == "1.0.0"
        assert record["source"] == "hub"

        backends2.close()

    def test_multiple_skills_survive_restart(self, tmp_path):
        """Multiple installed skills all survive a backend restart."""
        from storage.skill_registry import SkillRegistry

        storage_dir = tmp_path / "hermes"

        skills_to_install = [
            ("skill-alpha", {"name": "skill-alpha", "version": "0.1.0"}),
            ("skill-beta", {"name": "skill-beta", "version": "0.2.0"}),
            ("skill-gamma", {"name": "skill-gamma", "version": "0.3.0"}),
        ]

        # --- run 1: install ---
        backends1 = make_temp_backends(storage_dir)
        registry1 = SkillRegistry(backends1.structured)
        for skill_id, manifest in skills_to_install:
            registry1.install_skill(skill_id, manifest, source="user")
        backends1.close()

        # --- run 2: verify ---
        backends2 = make_temp_backends(storage_dir)
        registry2 = SkillRegistry(backends2.structured)

        all_skills = registry2.list_skills()
        found_ids = {s["id"] for s in all_skills}

        for skill_id, manifest in skills_to_install:
            assert skill_id in found_ids, f"Skill {skill_id} missing after restart"
            record = registry2.get_skill(skill_id)
            assert record["manifest"]["version"] == manifest["version"]

        backends2.close()

    def test_skill_removal_persists(self, tmp_path):
        """Removing a skill is persisted — it stays gone after restart."""
        from storage.skill_registry import SkillRegistry

        storage_dir = tmp_path / "hermes"

        # --- run 1: install two skills ---
        backends1 = make_temp_backends(storage_dir)
        registry1 = SkillRegistry(backends1.structured)
        registry1.install_skill("keep-skill", {"name": "keep"}, source="hub")
        registry1.install_skill("remove-skill", {"name": "remove"}, source="hub")
        backends1.close()

        # --- run 2: remove one ---
        backends2 = make_temp_backends(storage_dir)
        registry2 = SkillRegistry(backends2.structured)
        removed = registry2.remove_skill("remove-skill")
        assert removed is True
        backends2.close()

        # --- run 3: verify removal persisted ---
        backends3 = make_temp_backends(storage_dir)
        registry3 = SkillRegistry(backends3.structured)

        skill_ids = {s["id"] for s in registry3.list_skills()}
        assert "keep-skill" in skill_ids, "keep-skill should still exist"
        assert "remove-skill" not in skill_ids, "remove-skill should be gone"

        backends3.close()

    def test_skill_hub_meta_survives_restart(self, tmp_path):
        """Hub metadata survives backend restart."""
        from storage.skill_registry import SkillRegistry

        storage_dir = tmp_path / "hermes"

        hub_meta = {
            "last_sync": time.time(),
            "skills": [
                {"id": "hub-skill-1", "name": "Hub Skill 1"},
                {"id": "hub-skill-2", "name": "Hub Skill 2"},
            ],
            "version": "2025-01",
        }

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        registry1 = SkillRegistry(backends1.structured)
        registry1.set_hub_meta(hub_meta)
        backends1.close()

        # --- run 2: read ---
        backends2 = make_temp_backends(storage_dir)
        registry2 = SkillRegistry(backends2.structured)
        loaded_meta = registry2.get_hub_meta()

        assert loaded_meta is not None
        assert loaded_meta["version"] == "2025-01"
        assert len(loaded_meta["skills"]) == 2
        backends2.close()


# ---------------------------------------------------------------------------
# TestMigrationAndRestart
# ---------------------------------------------------------------------------


class TestMigrationAndRestart:
    """Simulate a full 'fresh container' startup scenario across all subsystems."""

    def test_full_stateless_restart(self, tmp_path):
        """
        Full container restart simulation:
        1. Wire all modules to local backends
        2. Write state across all subsystems
        3. Close everything (simulates container stop)
        4. Re-wire modules to same directory
        5. Assert all written state is present
        """
        import cron.jobs as cron_jobs
        from hermes_cli.config import ConfigBackend
        from hermes_cli.auth import AuthBackend
        from hermes_state import SessionDB
        from storage.skill_registry import SkillRegistry

        storage_dir = tmp_path / "hermes_full"

        # ===========================================================
        # PHASE 1: Write state across all subsystems
        # ===========================================================
        backends1 = make_temp_backends(storage_dir)

        # Wire cron
        cron_jobs.init_storage(
            backends1.structured, backends1.artifacts, backends1.locks
        )

        # --- Sessions ---
        db1 = SessionDB(backend=backends1.structured)
        session_id = "full-restart-session-001"
        db1.create_session(session_id, source="cli", model="claude-opus-4")
        db1.append_message(session_id, role="user", content="Full restart test message")

        # --- Cron jobs ---
        job_id = "full-restart-job-001"
        cron_jobs.add_job(
            {
                "id": job_id,
                "name": "Restart Test Job",
                "prompt": "Check system health",
                "schedule": {"kind": "interval", "minutes": 120, "display": "every 2h"},
                "skills": [],
                "skill": None,
                "model": None,
                "provider": None,
                "base_url": None,
                "script": None,
                "schedule_display": "every 2h",
                "repeat": {"times": None, "completed": 0},
                "enabled": True,
                "state": "scheduled",
                "paused_at": None,
                "paused_reason": None,
                "created_at": "2025-01-01T00:00:00+00:00",
                "next_run_at": None,
                "last_run_at": None,
                "last_status": None,
                "last_error": None,
                "last_delivery_error": None,
                "deliver": "local",
                "origin": None,
            }
        )

        # --- Config ---
        config_data = {"model": "claude-opus-4", "max_turns": 25}
        cb1 = ConfigBackend(backends1.structured)
        cb1.save_config(config_data)

        # --- Auth ---
        auth_store = {
            "providers": {"anthropic": {"api_key": "sk-ant-restart-test"}}
        }
        ab1 = AuthBackend(backends1.encrypted_blobs)
        ab1.save_auth_store(auth_store)

        # --- Skills ---
        registry1 = SkillRegistry(backends1.structured)
        registry1.install_skill(
            "restart-test-skill",
            {"name": "restart-test-skill", "version": "1.0.0"},
            source="hub",
        )

        # --- Artifacts ---
        artifact_id = "restart-test:export:transcript:001"
        artifact_data = b"Full restart test transcript content"
        backends1.artifacts.write_artifact(artifact_id, artifact_data)

        # --- SOUL.md ---
        soul_content = "# Restart Test Soul\n\nYou survive restarts.\n"
        backends1.structured.set_json("config:soul", {"content": soul_content})

        # CLOSE EVERYTHING (container stop)
        backends1.close()
        _reset_cron_globals()

        # ===========================================================
        # PHASE 2: Re-wire modules (container start)
        # ===========================================================
        backends2 = make_temp_backends(storage_dir)
        cron_jobs.init_storage(
            backends2.structured, backends2.artifacts, backends2.locks
        )

        # --- Verify sessions ---
        db2 = SessionDB(backend=backends2.structured)
        session = db2.get_session(session_id)
        assert session is not None, "Session missing after full restart"
        assert session["source"] == "cli"
        msgs = db2.get_messages(session_id)
        assert len(msgs) == 1
        assert msgs[0]["content"] == "Full restart test message"

        # --- Verify cron jobs ---
        job = cron_jobs.get_job(job_id)
        assert job is not None, "Cron job missing after full restart"
        assert job["name"] == "Restart Test Job"
        assert job["prompt"] == "Check system health"

        # --- Verify config ---
        cb2 = ConfigBackend(backends2.structured)
        loaded_config = cb2.load_config()
        assert loaded_config == config_data, f"Config mismatch after restart: {loaded_config}"

        # --- Verify auth ---
        ab2 = AuthBackend(backends2.encrypted_blobs)
        loaded_auth = ab2.load_auth_store()
        assert loaded_auth["providers"]["anthropic"]["api_key"] == "sk-ant-restart-test"

        # --- Verify skills ---
        registry2 = SkillRegistry(backends2.structured)
        skill_ids = {s["id"] for s in registry2.list_skills()}
        assert "restart-test-skill" in skill_ids, "Skill missing after full restart"

        # --- Verify artifacts ---
        retrieved = backends2.artifacts.read_artifact(artifact_id)
        assert retrieved == artifact_data, "Artifact data corrupted after restart"

        # --- Verify SOUL ---
        soul_data = backends2.structured.get_json("config:soul")
        assert soul_data is not None
        assert "Restart Test Soul" in soul_data["content"]

        backends2.close()

    def test_idempotent_startup_wiring(self, tmp_path):
        """Wiring modules multiple times is safe (idempotent)."""
        import cron.jobs as cron_jobs

        storage_dir = tmp_path / "hermes"
        backends = make_temp_backends(storage_dir)

        # Wire once
        cron_jobs.init_storage(
            backends.structured, backends.artifacts, backends.locks
        )

        # Wire again with same backends — should not raise or corrupt
        cron_jobs.init_storage(
            backends.structured, backends.artifacts, backends.locks
        )

        # Data operations still work
        job_id = "idempotent-job-001"
        cron_jobs.add_job(
            {
                "id": job_id,
                "name": "Idempotent Test",
                "prompt": "Test",
                "schedule": {"kind": "interval", "minutes": 60, "display": "every 1h"},
                "skills": [],
                "skill": None,
                "model": None,
                "provider": None,
                "base_url": None,
                "script": None,
                "schedule_display": "every 1h",
                "repeat": {"times": None, "completed": 0},
                "enabled": True,
                "state": "scheduled",
                "paused_at": None,
                "paused_reason": None,
                "created_at": "2025-01-01T00:00:00+00:00",
                "next_run_at": None,
                "last_run_at": None,
                "last_status": None,
                "last_error": None,
                "last_delivery_error": None,
                "deliver": "local",
                "origin": None,
            }
        )

        job = cron_jobs.get_job(job_id)
        assert job is not None

        backends.close()

    def test_hermes_migration_import_sessions(self, tmp_path):
        """HermesMigration can import sessions from a legacy state.db."""
        import sqlite3
        from storage.migration import HermesMigration

        storage_dir = tmp_path / "hermes"
        storage_dir.mkdir(parents=True, exist_ok=True)

        # Create a minimal legacy state.db
        db_path = storage_dir / "state.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                user_id TEXT,
                model TEXT,
                model_config TEXT,
                system_prompt TEXT,
                parent_session_id TEXT,
                started_at REAL NOT NULL,
                ended_at REAL,
                end_reason TEXT,
                message_count INTEGER DEFAULT 0,
                tool_call_count INTEGER DEFAULT 0,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                cache_read_tokens INTEGER DEFAULT 0,
                cache_write_tokens INTEGER DEFAULT 0,
                reasoning_tokens INTEGER DEFAULT 0,
                billing_provider TEXT,
                billing_base_url TEXT,
                billing_mode TEXT,
                estimated_cost_usd REAL,
                actual_cost_usd REAL,
                cost_status TEXT,
                cost_source TEXT,
                pricing_version TEXT,
                title TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT,
                tool_call_id TEXT,
                tool_calls TEXT,
                tool_name TEXT,
                timestamp REAL NOT NULL,
                token_count INTEGER,
                finish_reason TEXT,
                reasoning TEXT,
                reasoning_details TEXT,
                codex_reasoning_items TEXT
            )
        """)
        conn.execute(
            "INSERT INTO sessions (id, source, model, started_at) VALUES (?, ?, ?, ?)",
            ("legacy-session-001", "cli", "claude-opus-4", time.time()),
        )
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
            ("legacy-session-001", "user", "Legacy message content", time.time()),
        )
        conn.commit()
        conn.close()

        # Run migration
        backends = make_temp_backends(storage_dir)
        migration = HermesMigration(storage_dir, backends)
        result = migration.import_sessions()

        assert result.success, f"Migration failed: {result.errors}"
        assert result.items_imported >= 1

        # Verify data is now in backend
        from hermes_state import SessionDB

        db = SessionDB(backend=backends.structured)
        session = db.get_session("legacy-session-001")
        assert session is not None
        assert session["source"] == "cli"

        backends.close()

    def test_hermes_migration_import_auth(self, tmp_path):
        """HermesMigration imports auth.json into encrypted blob backend."""
        from storage.migration import HermesMigration

        storage_dir = tmp_path / "hermes"
        storage_dir.mkdir(parents=True, exist_ok=True)

        # Create legacy auth.json
        auth_data = {
            "providers": {
                "anthropic": {"api_key": "sk-ant-migrate-test"},
            }
        }
        auth_file = storage_dir / "auth.json"
        auth_file.write_text(json.dumps(auth_data))

        # Run migration
        backends = make_temp_backends(storage_dir)
        migration = HermesMigration(storage_dir, backends)
        result = migration.import_auth()

        assert result.success, f"Auth migration failed: {result.errors}"
        assert result.items_imported >= 1

        # Verify data is now in backend
        from hermes_cli.auth import AuthBackend

        ab = AuthBackend(backends.encrypted_blobs)
        loaded = ab.load_auth_store()
        assert loaded["providers"]["anthropic"]["api_key"] == "sk-ant-migrate-test"

        backends.close()

    def test_hermes_migration_import_config(self, tmp_path):
        """HermesMigration imports config.yaml and SOUL.md into backends."""
        from storage.migration import HermesMigration

        storage_dir = tmp_path / "hermes"
        storage_dir.mkdir(parents=True, exist_ok=True)

        # Create legacy config.yaml
        config_content = "model: claude-opus-4\nmax_turns: 30\n"
        config_file = storage_dir / "config.yaml"
        config_file.write_text(config_content)

        # Create legacy SOUL.md
        soul_content = "# Legacy Soul\n\nYou are a legacy assistant.\n"
        soul_file = storage_dir / "SOUL.md"
        soul_file.write_text(soul_content)

        # Run migration
        backends = make_temp_backends(storage_dir)
        migration = HermesMigration(storage_dir, backends)
        result = migration.import_config()

        assert result.success, f"Config migration failed: {result.errors}"

        # Verify config
        config_data = backends.structured.get_json("config:main")
        assert config_data is not None
        assert "claude-opus-4" in config_data.get("content", "")

        # Verify SOUL
        soul_data = backends.structured.get_json("config:soul")
        assert soul_data is not None
        assert "Legacy Soul" in soul_data.get("content", "")

        backends.close()


# ---------------------------------------------------------------------------
# TestArtifactLogger
# ---------------------------------------------------------------------------


class TestArtifactLogger:
    """Verify ArtifactLogger writes/reads/deletes artifacts via backend."""

    def test_write_and_read_export(self, tmp_path):
        """Write an export artifact and read it back."""
        from hermes_logging import ArtifactLogger

        storage_dir = tmp_path / "hermes"
        backends = make_temp_backends(storage_dir)

        logger = ArtifactLogger(backends.artifacts)

        content = b"This is a test export artifact.\nLine 2.\n"
        uri = logger.write_export("test-export-001", content, export_type="text")
        assert uri is not None, "write_export should return a URI"

        # Manually find and verify the artifact
        artifact_ids = backends.artifacts.list_artifacts()
        assert len(artifact_ids) >= 1

        # Find our artifact
        matching = [aid for aid in artifact_ids if "test-export-001" in aid]
        assert len(matching) >= 1, f"Export artifact not found among {artifact_ids}"

        retrieved = backends.artifacts.read_artifact(matching[0])
        assert retrieved == content

        backends.close()

    def test_write_session_transcript(self, tmp_path):
        """Write a session transcript artifact."""
        from hermes_logging import ArtifactLogger

        storage_dir = tmp_path / "hermes"
        backends = make_temp_backends(storage_dir)

        logger = ArtifactLogger(backends.artifacts)

        session_id = "transcript-session-abc"
        transcript = "User: Hello\nAssistant: Hi there!\nUser: Goodbye\n"
        uri = logger.write_session_transcript(session_id, transcript)
        assert uri is not None

        artifact_ids = backends.artifacts.list_artifacts()
        matching = [aid for aid in artifact_ids if session_id in aid]
        assert len(matching) >= 1

        retrieved_bytes = backends.artifacts.read_artifact(matching[0])
        assert retrieved_bytes.decode("utf-8") == transcript

        backends.close()

    def test_artifact_delete(self, tmp_path):
        """Delete an artifact via backend and verify it is gone."""
        storage_dir = tmp_path / "hermes"
        backends = make_temp_backends(storage_dir)

        artifact_id = "delete-me:artifact:001"
        data = b"Temporary artifact data"
        backends.artifacts.write_artifact(artifact_id, data)

        assert backends.artifacts.read_artifact(artifact_id) == data

        deleted = backends.artifacts.delete_artifact(artifact_id)
        assert deleted is True

        assert backends.artifacts.read_artifact(artifact_id) is None

        # Second delete returns False
        assert backends.artifacts.delete_artifact(artifact_id) is False

        backends.close()

    def test_artifact_survives_restart(self, tmp_path):
        """Artifact written to backend is readable after backend close/reopen."""
        from hermes_logging import ArtifactLogger

        storage_dir = tmp_path / "hermes"

        # --- run 1: write ---
        backends1 = make_temp_backends(storage_dir)
        logger1 = ArtifactLogger(backends1.artifacts)
        content = b"Persistent artifact content"
        uri = logger1.write_export("persistent-test", content, export_type="text")
        assert uri is not None

        # Find artifact id
        artifact_ids = backends1.artifacts.list_artifacts()
        matching = [aid for aid in artifact_ids if "persistent-test" in aid]
        assert len(matching) >= 1
        artifact_id = matching[0]

        backends1.close()

        # --- run 2: read ---
        backends2 = make_temp_backends(storage_dir)
        retrieved = backends2.artifacts.read_artifact(artifact_id)
        assert retrieved == content, "Artifact content changed after restart"

        size = backends2.artifacts.get_artifact_size(artifact_id)
        assert size == len(content)

        backends2.close()

    def test_artifact_logger_no_backend_returns_none(self):
        """ArtifactLogger with no backend returns None gracefully."""
        from hermes_logging import ArtifactLogger

        logger = ArtifactLogger(artifact_backend=None)
        uri = logger.write_export("no-backend", b"data", export_type="text")
        assert uri is None

    def test_multiple_artifacts_listed_correctly(self, tmp_path):
        """Multiple artifacts are listed with correct prefix filtering."""
        storage_dir = tmp_path / "hermes"
        backends = make_temp_backends(storage_dir)

        # Write artifacts with different prefixes
        ids = [
            "session:abc:transcript",
            "session:def:transcript",
            "exporttext:report:001",
        ]
        for aid in ids:
            backends.artifacts.write_artifact(aid, f"data for {aid}".encode())

        all_artifacts = backends.artifacts.list_artifacts()
        for aid in ids:
            assert aid in all_artifacts

        session_artifacts = backends.artifacts.list_artifacts("session:")
        assert "session:abc:transcript" in session_artifacts
        assert "session:def:transcript" in session_artifacts
        assert "exporttext:report:001" not in session_artifacts

        backends.close()


# ---------------------------------------------------------------------------
# PostgreSQL tests — skipped (no PG available in CI)
# ---------------------------------------------------------------------------


@pytest.mark.skip(
    reason="PostgreSQL backend requires a live DB (HERMES_TEST_POSTGRES_URL). "
    "Run manually with: HERMES_TEST_POSTGRES_URL=postgresql://... pytest"
)
class TestPostgresBackend:
    """Placeholder for PostgreSQL backend tests requiring a live DB."""

    def test_postgres_session_continuity(self, tmp_path):
        raise NotImplementedError

    def test_postgres_auth_continuity(self, tmp_path):
        raise NotImplementedError

    def test_postgres_cron_continuity(self, tmp_path):
        raise NotImplementedError
