"""Regression tests: stateless mode must NOT make durable writes under HERMES_HOME.

In stateless mode (HERMES_STATELESS=1 + HERMES_STORAGE_BACKEND=postgres), all
state goes through storage backends and HERMES_HOME is ephemeral scratch only.

Each test class exercises a key module and uses FileWriteAuditor to assert that
no unexpected files are created under HERMES_HOME during backend-backed operations.

If a test reveals a real bug (a module still writes to disk despite having a
backend configured), the failure message clearly states what was written and where.
"""

import logging
import time
from pathlib import Path
from typing import List, Set

import pytest


# ---------------------------------------------------------------------------
# Filesystem write auditor
# ---------------------------------------------------------------------------

def get_all_files(directory: Path) -> Set[str]:
    """Return all file paths recursively under *directory* as relative strings."""
    return {
        str(p.relative_to(directory))
        for p in directory.rglob('*')
        if p.is_file()
    }


# Patterns permitted under HERMES_HOME even in stateless mode.
# Only temp/cache artifacts should ever appear here; durable state must not.
STATELESS_ALLOWED_PATTERNS = [
    "storage/",   # local backend storage dir (test uses local backend)
    "cache/",     # cache is explicitly allowed
    "tmp/",       # temp files allowed
    ".tmp",       # temp file suffix
]


def _is_allowed(path: str, allowed_patterns: List[str]) -> bool:
    """Return True if *path* matches any of the allowed patterns."""
    for pattern in allowed_patterns:
        if pattern in path:
            return True
    return False


class FileWriteAuditor:
    """Snapshot-based tracker of unexpected file creations under a watched directory.

    Usage:
        auditor = FileWriteAuditor(hermes_home)
        # ... exercise the module under test ...
        auditor.assert_no_durable_writes()

    The auditor takes a baseline snapshot at construction time and compares against
    the current state when assert_no_durable_writes() is called.  Files matching
    STATELESS_ALLOWED_PATTERNS are exempted from the assertion.
    """

    def __init__(self, watch_dir: Path):
        self.watch_dir = watch_dir
        self._baseline: Set[str] = get_all_files(watch_dir)

    def new_files(self) -> Set[str]:
        """Return the set of files created since the baseline snapshot."""
        return get_all_files(self.watch_dir) - self._baseline

    def assert_no_durable_writes(self, allowed_patterns: List[str] = None):
        """Assert that no unexpected durable files were created under watch_dir."""
        new = self.new_files()
        forbidden = {
            f for f in new
            if not _is_allowed(f, allowed_patterns or STATELESS_ALLOWED_PATTERNS)
        }
        assert not forbidden, (
            "Unexpected durable writes in stateless mode under "
            f"{self.watch_dir}:\n"
            + "\n".join(f"  {f}" for f in sorted(forbidden))
        )


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def stateless_env(tmp_path, monkeypatch):
    """Set up a stateless environment with a fresh HERMES_HOME and local backends.

    - HERMES_HOME points to a temp dir that is audited for unexpected writes
    - HERMES_STATELESS=1 and HERMES_STORAGE_BACKEND=postgres signal stateless mode
    - Local backends (SQLite) are created under a *separate* sibling dir so that
      their own SQLite files never appear under the audited HERMES_HOME
    """
    hermes_home = tmp_path / 'hermes_home'
    hermes_home.mkdir()
    monkeypatch.setenv('HERMES_HOME', str(hermes_home))
    monkeypatch.setenv('HERMES_STATELESS', '1')
    monkeypatch.setenv('HERMES_STORAGE_BACKEND', 'postgres')

    # Storage backend is intentionally placed outside hermes_home so its SQLite
    # files don't appear in the audit.
    from storage.local import LocalStorageBackendSet
    backends = LocalStorageBackendSet(hermes_home=tmp_path / 'storage').create()

    yield {'hermes_home': hermes_home, 'backends': backends}

    backends.close()


# ---------------------------------------------------------------------------
# TestSessionDBStateless
# ---------------------------------------------------------------------------

class TestSessionDBStateless:
    """SessionDB with backend= must not touch state.db under HERMES_HOME."""

    def test_session_db_no_state_db_written(self, stateless_env):
        """SessionDB with backend= does not create state.db under HERMES_HOME."""
        from hermes_state import SessionDB

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        db = SessionDB(backend=stateless_env['backends'].structured)
        db.create_session('test_session', source='cli', model='gpt-4', system_prompt='test')
        db.append_message('test_session', role='user', content='hello')
        db.close()

        auditor.assert_no_durable_writes()
        assert not (stateless_env['hermes_home'] / 'state.db').exists(), \
            "state.db must NOT be created when backend= is used"

    def test_session_db_no_wal_sidecar_files(self, stateless_env):
        """No SQLite WAL/SHM sidecar files are created under HERMES_HOME."""
        from hermes_state import SessionDB

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        db = SessionDB(backend=stateless_env['backends'].structured)
        db.create_session('s1', source='gateway', model='claude-3')
        db.append_message('s1', role='assistant', content='response')
        db.end_session('s1', end_reason='user_exit')
        db.close()

        auditor.assert_no_durable_writes()
        for suffix in ('-wal', '-shm', '.db'):
            found = list(stateless_env['hermes_home'].rglob(f'*{suffix}'))
            assert not found, \
                f"SQLite artifact '*{suffix}' must not appear under HERMES_HOME; found: {found}"

    def test_session_db_multiple_sessions_no_writes(self, stateless_env):
        """Multiple backend-backed sessions produce no HERMES_HOME writes."""
        from hermes_state import SessionDB

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        db = SessionDB(backend=stateless_env['backends'].structured)
        for i in range(5):
            sid = f'session_{i}'
            db.create_session(sid, source='cli', model='gpt-4o')
            db.append_message(sid, role='user', content=f'message {i}')
            db.append_message(sid, role='assistant', content=f'reply {i}')
            db.end_session(sid, end_reason='user_exit')
        db.close()

        auditor.assert_no_durable_writes()


# ---------------------------------------------------------------------------
# TestCronJobsStateless
# ---------------------------------------------------------------------------

class TestCronJobsStateless:
    """cron.jobs with init_storage() must not write jobs.json or output files."""

    @pytest.fixture(autouse=True)
    def reset_cron_globals(self):
        """Restore cron.jobs module-level storage globals after each test."""
        import cron.jobs as jobs_mod
        orig_structured = jobs_mod._structured
        orig_artifacts = jobs_mod._artifacts
        orig_locks = jobs_mod._locks
        yield
        jobs_mod._structured = orig_structured
        jobs_mod._artifacts = orig_artifacts
        jobs_mod._locks = orig_locks

    def test_cron_no_jobs_json_written(self, stateless_env):
        """cron.jobs with init_storage() does not create jobs.json under HERMES_HOME."""
        import cron.jobs as jobs_mod

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        backends = stateless_env['backends']
        jobs_mod.init_storage(
            structured=backends.structured,
            artifacts=backends.artifacts,
            locks=backends.locks,
        )

        # create_job and list_jobs exercise load/save cycles.
        jobs_mod.create_job(
            prompt='echo hello',
            schedule='every 1d',
            name='test_job',
        )
        jobs_mod.list_jobs()

        auditor.assert_no_durable_writes()
        assert not (stateless_env['hermes_home'] / 'cron' / 'jobs.json').exists(), \
            "jobs.json must NOT be created when storage backend is configured"

    def test_cron_no_output_files_written(self, stateless_env):
        """cron job output goes to the artifact backend, not the filesystem."""
        import cron.jobs as jobs_mod

        backends = stateless_env['backends']
        jobs_mod.init_storage(
            structured=backends.structured,
            artifacts=backends.artifacts,
            locks=backends.locks,
        )

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        jobs_mod.save_job_output('test-output-job', 'Output should go to artifact backend.')

        auditor.assert_no_durable_writes()
        assert not (stateless_env['hermes_home'] / 'cron' / 'output').exists(), \
            "cron/output/ must NOT be created when artifact backend is configured"

    def test_cron_load_save_roundtrip_no_file(self, stateless_env):
        """load_jobs/save_jobs round-trip through backend with no file I/O."""
        import cron.jobs as jobs_mod

        backends = stateless_env['backends']
        jobs_mod.init_storage(
            structured=backends.structured,
            artifacts=backends.artifacts,
            locks=backends.locks,
        )

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        # Explicitly save and reload the jobs dict.
        jobs: dict = {}
        jobs_mod.save_jobs(jobs)
        loaded = jobs_mod.load_jobs()
        assert isinstance(loaded, dict)

        auditor.assert_no_durable_writes()


# ---------------------------------------------------------------------------
# TestAuthStateless
# ---------------------------------------------------------------------------

class TestAuthStateless:
    """Auth operations with backend must not write auth.json or .lock files."""

    @pytest.fixture(autouse=True)
    def reset_auth_globals(self):
        """Restore hermes_cli.auth._auth_backend after each test."""
        import hermes_cli.auth as auth_mod
        orig = auth_mod._auth_backend
        yield
        auth_mod._auth_backend = orig

    def test_auth_no_auth_json_written(self, stateless_env):
        """AuthBackend.save_auth_store() does not create auth.json under HERMES_HOME."""
        import hermes_cli.auth as auth_mod

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        auth_mod.init_storage(stateless_env['backends'].encrypted_blobs)

        fake_store = {
            'active_provider': 'openrouter',
            'providers': {'openrouter': {'api_key': 'test-key'}},
        }
        auth_mod._save_auth_store(fake_store)
        auth_mod._load_auth_store()

        auditor.assert_no_durable_writes()
        assert not (stateless_env['hermes_home'] / 'auth.json').exists(), \
            "auth.json must NOT be created when AuthBackend is configured"

    def test_auth_no_lock_file_written(self, stateless_env):
        """Auth operations do not create .lock files when backend is active."""
        import hermes_cli.auth as auth_mod

        auth_mod.init_storage(stateless_env['backends'].encrypted_blobs)

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        # _auth_store_lock() should skip file locking when backend is set.
        with auth_mod._auth_store_lock():
            pass

        auditor.assert_no_durable_writes()
        lock_files = list(stateless_env['hermes_home'].rglob('*.lock'))
        assert not lock_files, \
            f".lock files must NOT be created when AuthBackend is configured; found: {lock_files}"

    def test_auth_roundtrip_no_file(self, stateless_env):
        """Save and reload auth store via backend with no file writes."""
        import hermes_cli.auth as auth_mod

        auth_mod.init_storage(stateless_env['backends'].encrypted_blobs)

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        original = {
            'active_provider': 'nous',
            'providers': {
                'nous': {'api_key': 'sk-test', 'expires_at': time.time() + 3600}
            },
        }
        auth_mod._save_auth_store(original)
        reloaded = auth_mod._load_auth_store()

        # Data round-trips correctly.
        assert reloaded.get('active_provider') == 'nous'

        auditor.assert_no_durable_writes()
        assert not list(stateless_env['hermes_home'].rglob('auth*')), \
            "No auth* files should exist under HERMES_HOME when backend is active"


# ---------------------------------------------------------------------------
# TestLoggingStateless
# ---------------------------------------------------------------------------

class TestLoggingStateless:
    """configure_stateless_logging() must not create log files under HERMES_HOME."""

    @pytest.fixture(autouse=True)
    def reset_logging_handlers(self):
        """Remove any FileHandlers added during the test to keep the suite clean."""
        root = logging.getLogger()
        handlers_before = list(root.handlers)
        yield
        for h in list(root.handlers):
            if h not in handlers_before:
                root.removeHandler(h)
                if hasattr(h, 'close'):
                    h.close()

    def test_no_log_files_created(self, stateless_env):
        """configure_stateless_logging() does not create log files."""
        from hermes_logging import configure_stateless_logging

        configure_stateless_logging()
        logger = logging.getLogger('stateless_test')
        logger.info('This should NOT go to a file')

        auditor = FileWriteAuditor(stateless_env['hermes_home'])
        logger.info('Post-audit log message')

        auditor.assert_no_durable_writes()
        log_files = list(stateless_env['hermes_home'].rglob('*.log'))
        assert not log_files, \
            f"*.log files must NOT be created in stateless mode; found: {log_files}"

    def test_configure_stateless_removes_existing_file_handlers(self, stateless_env):
        """configure_stateless_logging() removes pre-existing FileHandlers."""
        from hermes_logging import configure_stateless_logging
        from logging.handlers import RotatingFileHandler

        log_dir = stateless_env['hermes_home'] / 'logs'
        log_dir.mkdir()
        handler = RotatingFileHandler(str(log_dir / 'test.log'))
        logging.getLogger().addHandler(handler)

        configure_stateless_logging()

        file_handlers = [
            h for h in logging.getLogger().handlers
            if isinstance(h, logging.FileHandler)
        ]
        assert not file_handlers, \
            "configure_stateless_logging() must remove all FileHandler instances from root logger"

    def test_stateless_logging_no_logs_dir_created(self, stateless_env):
        """configure_stateless_logging() does not create a logs/ directory."""
        from hermes_logging import configure_stateless_logging

        configure_stateless_logging()
        logging.getLogger('hermes.test').warning('test warning')

        assert not (stateless_env['hermes_home'] / 'logs').exists(), \
            "logs/ directory must NOT be created by configure_stateless_logging()"


# ---------------------------------------------------------------------------
# TestConfigStateless
# ---------------------------------------------------------------------------

class TestConfigStateless:
    """ConfigBackend.save_config() must not write config.yaml under HERMES_HOME."""

    def test_config_backend_no_yaml_written(self, stateless_env):
        """ConfigBackend.save_config() stores config in the backend, not config.yaml."""
        from hermes_cli.config import ConfigBackend

        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        config_backend = ConfigBackend(stateless_env['backends'].structured)
        fake_config = {'model': 'gpt-4', 'provider': 'openrouter'}
        config_backend.save_config(fake_config)

        loaded = config_backend.load_config()
        assert loaded == fake_config

        auditor.assert_no_durable_writes()
        assert not (stateless_env['hermes_home'] / 'config.yaml').exists(), \
            "config.yaml must NOT be created when ConfigBackend is used"

    def test_config_backend_update_no_yaml_written(self, stateless_env):
        """Multiple ConfigBackend saves do not create config.yaml."""
        from hermes_cli.config import ConfigBackend

        config_backend = ConfigBackend(stateless_env['backends'].structured)
        auditor = FileWriteAuditor(stateless_env['hermes_home'])

        for i in range(3):
            config_backend.save_config({'iteration': i, 'model': f'model-{i}'})

        latest = config_backend.load_config()
        assert latest['iteration'] == 2

        auditor.assert_no_durable_writes()
        assert not (stateless_env['hermes_home'] / 'config.yaml').exists()

    def test_config_backend_empty_home_stays_empty(self, stateless_env):
        """ConfigBackend operations leave HERMES_HOME completely empty."""
        from hermes_cli.config import ConfigBackend

        config_backend = ConfigBackend(stateless_env['backends'].structured)
        config_backend.save_config({'key': 'value'})
        config_backend.load_config()

        # The hermes_home directory should have no files at all.
        all_files = get_all_files(stateless_env['hermes_home'])
        assert not all_files, \
            f"HERMES_HOME must stay empty after ConfigBackend ops; found: {all_files}"
