## Plan: Hermes Stateless External State

Make PostgreSQL the single required durable backend for Hermes state, add optional object storage only for bulky artifacts, and treat HERMES_HOME as ephemeral scratch only. The implementation should replace every durable write that currently lands under HERMES_HOME: sessions, gateway state, cron, auth/token blobs, built-in memory, SOUL, config, skills, profile metadata, and adapter-specific state.

**Steps**
1. Lock the target architecture and acceptance criteria. Define stateless mode as: a fresh container with no mounted volume can restart or reschedule without losing any Hermes state; local disk may hold temp/cache data only; PostgreSQL is the system of record; object storage is optional and only for large artifacts. This step is blocking for all others.
2. Introduce backend contracts and backend selection. Add a small storage layer that separates structured state, encrypted blob state, artifact/blob storage, and distributed locking. Keep the existing local filesystem/SQLite behavior behind a compatibility backend so the refactor can land incrementally. This step is blocking for the remaining implementation work.
3. Move core conversation state to PostgreSQL. Replace the current SessionDB, gateway session mapping, transcript metadata, response replay state, and session search indexes with PostgreSQL-backed stores. Recreate FTS5 behavior with PostgreSQL full-text search and, where needed, pg_trgm-based matching. This depends on step 2.
4. Move scheduler and coordination state to PostgreSQL. Replace cron/jobs.json, cron execution state, due-job claiming, and single-run coordination with PostgreSQL tables plus advisory locks. Store small job outputs inline and large outputs as artifacts. This depends on step 2 and can run in parallel with step 3.
5. Move instance identity and curated state to PostgreSQL. Replace HERMES_HOME-backed config, SOUL.md, built-in memory files, profile metadata, skins, plans, webhook state, and other text-first durable state with PostgreSQL rows/documents. Keep workspace-local AGENTS.md or project context files out of scope because they belong to the project repo, not Hermes instance state. This depends on step 2 and can run in parallel with steps 3 and 4.
6. Move mutable credentials and adapter state off disk. Replace auth.json, OAuth/MCP token blobs, pairing state, WhatsApp session state, Matrix crypto persistence, Weixin context tokens, telephony state, browser persistence, and similar adapter-specific durable files with encrypted PostgreSQL storage, using object storage only where the payload is too large or binary-heavy. This depends on step 2; some adapters will also depend on steps 3 through 5.
7. Rework skills and manifests so they do not require local durable directories. Keep bundled skills in the image or repo checkout, but move user-created skills, hub-installed skills, bundled manifests, hub metadata, and other mutable skill state into PostgreSQL, with optional object storage for large skill assets. This depends on step 5.
8. Eliminate durable logging and artifact assumptions under HERMES_HOME. Send operational logs to stdout/stderr by default, persist only user-visible exports or large binary artifacts through the artifact backend, and make checkpoints, browser recordings, trajectories, and similar features either remote-backed or explicitly disabled in stateless mode. This depends on step 2 and can run in parallel with steps 3 through 7.
9. Add migration and cutover tooling. Provide commands to import an existing HERMES_HOME into PostgreSQL and object storage, validate parity, and support a temporary read-local/write-remote or dual-write mode during rollout. Update Docker and deployment bootstrapping so startup no longer seeds persistent directories or assumes a writable volume. This depends on steps 3 through 8.
10. Harden for multi-replica operation and prove zero-PVC behavior. Add concurrency tests, restart/reschedule tests, adapter recovery tests, and an assertion suite that fails if stateless mode writes durable data under HERMES_HOME. This depends on steps 3 through 9.

**Relevant files**
- Forked Hermes repo: hermes_state.py, gateway/session.py, gateway/platforms/api_server.py, cron/jobs.py, cron/scheduler.py — core session, gateway, and scheduler durability to replace first.
- Forked Hermes repo: hermes_cli/config.py, hermes_constants.py, agent/prompt_builder.py, hermes_cli/auth.py — HERMES_HOME assumptions, config persistence, SOUL loading, and auth storage.
- Forked Hermes repo: tools/skills_tool.py, tools/skill_manager_tool.py, tools/skills_sync.py, memory tool implementation, webhook and profile management code — mutable skill, memory, and instance-level state.
- Forked Hermes repo: gateway platform adapters and platform-specific helpers — WhatsApp, Matrix, Weixin, telephony, browser persistence, pairing, webhook, and token storage paths.
- Current deployment wrapper: /Users/anh/WorkGit/_TRADING/smpl-trading-bot/Dockerfile and /Users/anh/WorkGit/_TRADING/smpl-trading-bot/docker/hermes-entrypoint.sh — remove assumptions that Hermes must bootstrap a persistent HERMES_HOME tree once the fork supports stateless mode.

**Verification**
1. Add a stateless integration profile that runs Hermes with only PostgreSQL configured, no mounted volume, and an empty writable filesystem; verify session continuity, memory continuity, SOUL/config loading, cron continuity, and adapter reconnect after container restart.
2. Run multi-replica tests where two Hermes instances share the same PostgreSQL database; verify session search correctness, single-fire cron execution, no duplicate job claims, and safe concurrent gateway traffic.
3. Add regression tests that fail on unexpected durable writes under HERMES_HOME in stateless mode, while still allowing explicitly temporary cache directories.
4. Validate migration by importing a real HERMES_HOME sample and comparing session counts, searchable content, memory entries, skills, and adapter credentials before and after cutover.

**Decisions**
- Prefer PostgreSQL for almost all durable state. Do not introduce Redis unless later performance data proves PostgreSQL advisory locks are insufficient.
- Treat object storage as optional and narrow in scope: large artifacts, exports, recordings, and other blob-heavy data. Do not move core structured state there.
- Keep bundled skills immutable in the image or source tree; only mutable skill state must move off disk.
- Treat workspace-local context files such as AGENTS.md as project data, not Hermes durable instance state.
- In stateless mode, any feature that still requires durable local disk is a bug or must be explicitly disabled until migrated.

**Further Considerations**
1. Swarm split recommendation: one agent for storage abstractions and config plumbing, one for sessions/search/response store, one for cron and locking, one for config/SOUL/memory/skills, one for auth plus adapter state, and one for migration/tests/docs.
2. The cleanest first milestone is not “all adapters complete”; it is “core Hermes works with PostgreSQL only and zero PVC.” After that, adapter-specific state migrations can finish behind a clearly documented stateless support matrix.
3. The deployment proof should include a read-only root filesystem plus tmpfs-only scratch paths. That catches hidden file writes much earlier than a normal container run.