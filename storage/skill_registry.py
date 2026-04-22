"""Storage-backed registry for mutable skill metadata.

Only user-created and hub-installed skills are stored here.
Bundled skills (those in the repo's skills/ directory) stay on disk and
are never written to this registry.

Key naming scheme:
  "skills:installed:{skill_id}"  →  skill record dict
  "skills:index"                 →  {"installed": [skill_id, ...], "updated_at": float}
  "skills:hub:meta"              →  {"last_sync": float, "skills": [...], ...}
"""

import logging
import time
from typing import Any, Dict, List, Optional

from .backends import StructuredStateBackend

logger = logging.getLogger(__name__)

_KEY_PREFIX = "skills:installed:"
_INDEX_KEY = "skills:index"
_HUB_META_KEY = "skills:hub:meta"


class SkillRegistry:
    """Stores mutable skill metadata in the structured state backend.

    Manages user-created and hub-installed skill records.  Bundled skills
    are not tracked here — they remain on-disk in the repo's skills/ dir.
    """

    def __init__(self, backend: StructuredStateBackend) -> None:
        self.backend = backend

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _skill_key(self, skill_id: str) -> str:
        return f"{_KEY_PREFIX}{skill_id}"

    def _load_index(self) -> Dict[str, Any]:
        return self.backend.get_json(_INDEX_KEY) or {"installed": [], "updated_at": 0.0}

    def _save_index(self, index: Dict[str, Any]) -> None:
        index["updated_at"] = time.time()
        self.backend.set_json(_INDEX_KEY, index)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def install_skill(
        self, skill_id: str, manifest: dict, source: str = "hub"
    ) -> None:
        """Register a skill in the backend.

        Args:
            skill_id: Unique identifier for the skill (filesystem-safe name).
            manifest: Skill metadata (name, version, description, …).
            source:   Origin of the skill, e.g. ``"hub"`` or ``"user"``.
        """
        record: Dict[str, Any] = {
            "id": skill_id,
            "source": source,
            "manifest": manifest,
            "installed_at": time.time(),
        }
        self.backend.set_json(self._skill_key(skill_id), record)

        index = self._load_index()
        if skill_id not in index["installed"]:
            index["installed"].append(skill_id)
        self._save_index(index)

        logger.debug("SkillRegistry: installed skill '%s' (source=%s)", skill_id, source)

    def remove_skill(self, skill_id: str) -> bool:
        """Remove a skill from the registry.

        Returns:
            True if the skill existed and was removed, False otherwise.
        """
        existed = self.backend.delete_json(self._skill_key(skill_id))

        index = self._load_index()
        try:
            index["installed"].remove(skill_id)
            self._save_index(index)
        except ValueError:
            pass  # wasn't in the index — that's fine

        if existed:
            logger.debug("SkillRegistry: removed skill '%s'", skill_id)
        return existed

    def get_skill(self, skill_id: str) -> Optional[dict]:
        """Return the skill record, or None if not found."""
        return self.backend.get_json(self._skill_key(skill_id))

    def list_skills(self) -> List[dict]:
        """Return all installed skill records."""
        index = self._load_index()
        skill_ids = index.get("installed", [])
        if not skill_ids:
            return []

        keys = [self._skill_key(sid) for sid in skill_ids]
        records = self.backend.batch_get(keys)

        result = []
        for sid in skill_ids:
            record = records.get(self._skill_key(sid))
            if record is not None:
                result.append(record)
        return result

    def search_skills(self, query: str) -> List[dict]:
        """Full-text search across installed skill records.

        Returns a list of skill records whose stored JSON matches *query*.
        Falls back to an empty list when the backend has no matches.
        """
        hits = self.backend.search(query, prefix=_KEY_PREFIX)
        return [value for _key, value in hits]

    def update_manifest(self, skill_id: str, manifest: dict) -> None:
        """Update the manifest for an already-installed skill.

        No-op if the skill is not registered.
        """
        record = self.get_skill(skill_id)
        if record is None:
            logger.warning(
                "SkillRegistry.update_manifest: skill '%s' not found", skill_id
            )
            return
        record["manifest"] = manifest
        record["updated_at"] = time.time()
        self.backend.set_json(self._skill_key(skill_id), record)

    # ------------------------------------------------------------------
    # Hub metadata helpers
    # ------------------------------------------------------------------

    def get_hub_meta(self) -> Optional[dict]:
        """Return cached hub metadata, or None if never synced."""
        return self.backend.get_json(_HUB_META_KEY)

    def set_hub_meta(self, meta: dict) -> None:
        """Store hub metadata (available skills, version info, last sync)."""
        meta = dict(meta)
        meta["last_sync"] = meta.get("last_sync", time.time())
        self.backend.set_json(_HUB_META_KEY, meta)
