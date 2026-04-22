"""Multi-replica concurrency tests for the Hermes storage abstraction layer.

Uses ``threading`` to simulate multiple "instances" sharing the same local
SQLite backend — the same scenario as N stateless containers writing to one
shared database.

Test surface:
- DistributedLockBackend  — mutual exclusion, TTL, extend
- StructuredStateBackend  — concurrent session writes, search visibility
- CronJob claiming        — single-fire execution via locks
- Config access           — read-after-write consistency
"""

import threading
import time

import pytest

from storage.local import LocalStorageBackendSet


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def storage(tmp_path):
    backends = LocalStorageBackendSet(hermes_home=tmp_path).create()
    yield backends
    backends.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_threads(fn_list):
    """Start every callable in *fn_list* as a thread and join them all."""
    threads = [threading.Thread(target=fn) for fn in fn_list]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# ---------------------------------------------------------------------------
# TestDistributedLocks
# ---------------------------------------------------------------------------

class TestDistributedLocks:

    def test_single_claim_among_threads(self, storage):
        """Only one of N threads should successfully claim a given lock."""
        lock_id = "job:daily"
        n = 10
        results = []
        mu = threading.Lock()
        barrier = threading.Barrier(n)

        def try_acquire():
            barrier.wait()
            handle = storage.locks.acquire_lock(lock_id, duration_seconds=10)
            with mu:
                results.append(handle)

        _run_threads([try_acquire] * n)

        winners = [h for h in results if h is not None]
        assert len(winners) == 1, f"Expected exactly 1 winner, got {len(winners)}"
        storage.locks.release_lock(lock_id)

    def test_lock_released_after_context(self, storage):
        """Lock is released when context exits, allowing re-acquisition."""
        lock_id = "job:ctx_release"

        with storage.locks.lock_context(lock_id, duration_seconds=10) as handle:
            assert handle is not None
            assert storage.locks.is_locked(lock_id)

        assert not storage.locks.is_locked(lock_id)

        handle2 = storage.locks.acquire_lock(lock_id, duration_seconds=10)
        assert handle2 is not None, "Released lock must be re-acquirable"
        storage.locks.release_lock(lock_id)

    def test_lock_expiry(self, storage):
        """An expired lock can be acquired by another thread/instance."""
        lock_id = "job:expiry_test"

        handle = storage.locks.acquire_lock(lock_id, duration_seconds=0.1)
        assert handle is not None
        assert not handle.is_expired()

        time.sleep(0.2)  # Let TTL elapse
        assert handle.is_expired()

        handle2 = storage.locks.acquire_lock(lock_id, duration_seconds=10)
        assert handle2 is not None, "Expired lock should be acquirable"
        storage.locks.release_lock(lock_id)

    def test_extend_prevents_expiry(self, storage):
        """Extending a lock prevents it from expiring."""
        lock_id = "job:extend_test"

        handle = storage.locks.acquire_lock(lock_id, duration_seconds=0.1)
        assert handle is not None

        handle.extend(10.0)          # Push expiry 10 s into the future
        time.sleep(0.2)              # Original 0.1 s TTL would have elapsed

        assert storage.locks.is_locked(lock_id), "Extended lock must still be held"

        stolen = storage.locks.acquire_lock(lock_id, duration_seconds=10)
        assert stolen is None, "Extended lock must not be re-claimable"

        storage.locks.release_lock(lock_id)

    def test_concurrent_different_locks(self, storage):
        """Different lock IDs don't interfere with each other."""
        n = 10
        results = []
        mu = threading.Lock()
        barrier = threading.Barrier(n)
        lock_ids = [f"job:unique:{i}" for i in range(n)]

        def try_acquire(idx):
            barrier.wait()
            handle = storage.locks.acquire_lock(lock_ids[idx], duration_seconds=10)
            with mu:
                results.append(handle)
            if handle:
                storage.locks.release_lock(lock_ids[idx])

        _run_threads([lambda i=i: try_acquire(i) for i in range(n)])

        winners = [h for h in results if h is not None]
        assert len(winners) == n, "Each unique-ID lock acquisition must succeed"


# ---------------------------------------------------------------------------
# TestConcurrentSessionWrites
# ---------------------------------------------------------------------------

class TestConcurrentSessionWrites:

    def test_concurrent_session_creation(self, storage):
        """Multiple threads creating sessions concurrently — no data loss."""
        n_threads = 20
        n_msgs = 5
        barrier = threading.Barrier(n_threads)
        errors = []
        err_lock = threading.Lock()

        def create_session(idx):
            try:
                barrier.wait()
                sid = f"session:thread{idx}"
                storage.structured.set_json(f"{sid}:meta", {
                    "id": sid, "thread_idx": idx,
                })
                for m in range(n_msgs):
                    storage.structured.set_json(
                        f"{sid}:msg:{m:02d}",
                        {"role": "user", "content": f"msg {m} thread {idx}"},
                    )
            except Exception as exc:
                with err_lock:
                    errors.append(exc)

        _run_threads([lambda i=i: create_session(i) for i in range(n_threads)])

        assert not errors, f"Thread exceptions: {errors}"

        for idx in range(n_threads):
            sid = f"session:thread{idx}"
            meta = storage.structured.get_json(f"{sid}:meta")
            assert meta is not None, f"Session {idx} metadata missing"
            assert meta["thread_idx"] == idx

            msgs = storage.structured.list_keys(prefix=f"{sid}:msg:")
            assert len(msgs) == n_msgs, (
                f"Session {idx}: expected {n_msgs} messages, got {len(msgs)}"
            )

    def test_concurrent_message_append(self, storage):
        """Multiple threads appending to the SAME session — no messages lost."""
        sid = "session:shared"
        n_threads = 10
        msgs_per_thread = 10
        barrier = threading.Barrier(n_threads)
        errors = []
        err_lock = threading.Lock()

        def append_messages(thread_idx):
            try:
                barrier.wait()
                for local_idx in range(msgs_per_thread):
                    # Keys are unique per (thread, local_idx), so no overwrites.
                    key = f"{sid}:msg:{thread_idx:02d}:{local_idx:02d}"
                    storage.structured.set_json(key, {
                        "role": "user",
                        "content": f"t{thread_idx} msg {local_idx}",
                    })
            except Exception as exc:
                with err_lock:
                    errors.append(exc)

        _run_threads([lambda i=i: append_messages(i) for i in range(n_threads)])

        assert not errors, f"Thread exceptions: {errors}"

        all_keys = storage.structured.list_keys(prefix=f"{sid}:msg:")
        expected = n_threads * msgs_per_thread
        assert len(all_keys) == expected, (
            f"Expected {expected} messages, found {len(all_keys)}"
        )

    def test_search_returns_all_concurrent_writes(self, storage):
        """Search sees all sessions written by concurrent writers."""
        n_threads = 5
        sessions_per_thread = 4
        barrier = threading.Barrier(n_threads)
        errors = []
        err_lock = threading.Lock()
        # Collect written terms so we can verify them after.
        written_terms: list[str] = []
        terms_lock = threading.Lock()

        def write_sessions(tidx):
            try:
                barrier.wait()
                for sidx in range(sessions_per_thread):
                    # All-alphanumeric so FTS5 unicode61 tokenizer
                    # treats each as a single indivisible token.
                    term = f"xsearch{tidx}{sidx}xmarker"
                    storage.structured.set_json(
                        f"session:t{tidx}:s{sidx}",
                        {"thread": tidx, "session": sidx, "tag": term},
                    )
                    with terms_lock:
                        written_terms.append(term)
            except Exception as exc:
                with err_lock:
                    errors.append(exc)

        _run_threads([lambda i=i: write_sessions(i) for i in range(n_threads)])

        assert not errors, f"Thread exceptions: {errors}"

        for term in written_terms:
            results = storage.structured.search(term, limit=10)
            assert len(results) == 1, (
                f"Search for '{term}' returned {len(results)} results, expected 1"
            )


# ---------------------------------------------------------------------------
# TestCronJobClaiming
# ---------------------------------------------------------------------------

class TestCronJobClaiming:

    def test_single_job_claimed_once(self, storage):
        """Among N concurrent claim attempts, exactly 1 wins."""
        lock_id = "cron:claim:test_job"
        n = 10
        results = []
        mu = threading.Lock()
        barrier = threading.Barrier(n)

        def claim():
            barrier.wait()
            handle = storage.locks.acquire_lock(lock_id, duration_seconds=10)
            with mu:
                results.append(handle)

        _run_threads([claim] * n)

        winners = [h for h in results if h is not None]
        assert len(winners) == 1, f"Expected exactly 1 job winner, got {len(winners)}"
        storage.locks.release_lock(lock_id)

    def test_job_not_reclaimed_while_running(self, storage):
        """A job being processed cannot be claimed by another instance."""
        lock_id = "cron:claim:running_job"
        n_competing = 4

        # Instance A holds the lock (simulates job in progress).
        handle_a = storage.locks.acquire_lock(lock_id, duration_seconds=10)
        assert handle_a is not None

        results = []
        mu = threading.Lock()
        barrier = threading.Barrier(n_competing)

        def try_claim():
            barrier.wait()
            h = storage.locks.acquire_lock(lock_id, duration_seconds=10)
            with mu:
                results.append(h)

        _run_threads([try_claim] * n_competing)

        winners = [h for h in results if h is not None]
        assert len(winners) == 0, "No competing thread should steal the running-job lock"

        storage.locks.release_lock(lock_id)

    def test_job_reclaim_after_release(self, storage):
        """After lock release, another instance can claim the job."""
        lock_id = "cron:claim:releasable_job"

        handle_a = storage.locks.acquire_lock(lock_id, duration_seconds=10)
        assert handle_a is not None
        storage.locks.release_lock(lock_id)

        result: list = [None]
        done = threading.Event()

        def claim_after_release():
            result[0] = storage.locks.acquire_lock(lock_id, duration_seconds=10)
            done.set()

        t = threading.Thread(target=claim_after_release)
        t.start()
        done.wait(timeout=5)
        t.join()

        assert result[0] is not None, "Instance B must claim the lock after A releases it"
        storage.locks.release_lock(lock_id)


# ---------------------------------------------------------------------------
# TestConcurrentConfigAccess
# ---------------------------------------------------------------------------

class TestConcurrentConfigAccess:

    def test_concurrent_config_reads_consistent(self, storage):
        """Concurrent readers all see the same config value."""
        key = "config:hermes"
        value = {"model": "gpt4", "max_turns": 50, "features": ["memory", "tools"]}
        storage.structured.set_json(key, value)

        n = 20
        results = []
        mu = threading.Lock()
        errors = []
        err_lock = threading.Lock()
        barrier = threading.Barrier(n)

        def read_config():
            try:
                barrier.wait()
                v = storage.structured.get_json(key)
                with mu:
                    results.append(v)
            except Exception as exc:
                with err_lock:
                    errors.append(exc)

        _run_threads([read_config] * n)

        assert not errors, f"Reader exceptions: {errors}"
        assert len(results) == n
        for v in results:
            assert v == value, f"Inconsistent read: got {v}"

    def test_config_write_then_read_consistent(self, storage):
        """After a write completes, all subsequent readers see the new value."""
        key = "config:update_test"
        old_val = {"version": 1, "feature": "off"}
        new_val = {"version": 2, "feature": "on"}

        storage.structured.set_json(key, old_val)
        storage.structured.set_json(key, new_val)  # Overwrite before threads start

        n = 15
        results = []
        mu = threading.Lock()
        barrier = threading.Barrier(n)

        def read_config():
            barrier.wait()
            v = storage.structured.get_json(key)
            with mu:
                results.append(v)

        _run_threads([read_config] * n)

        assert len(results) == n
        for v in results:
            assert v == new_val, f"Reader saw stale value: {v}"
