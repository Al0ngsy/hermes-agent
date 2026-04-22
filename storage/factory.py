"""Factory function to select and initialize storage backends based on configuration."""

import logging
import os
from typing import Optional

from .backends import StorageBackendSet

logger = logging.getLogger(__name__)


def create_storage_backends(
    backend_type: Optional[str] = None,
    postgres_url: Optional[str] = None,
) -> StorageBackendSet:
    """
    Create a complete storage backend set based on configuration.

    Args:
        backend_type: Override backend selection. Either "local" or "postgres".
                     If None, reads HERMES_STORAGE_BACKEND env var (default: "local").
        postgres_url: PostgreSQL connection URL for postgres backend.
                     If None, reads HERMES_STORAGE_POSTGRES_URL env var.

    Returns:
        StorageBackendSet with all four backends initialized and ready.

    Raises:
        ValueError: If backend_type is invalid or postgres_url is missing for postgres backend.
        RuntimeError: If backend initialization fails.
    """
    if backend_type is None:
        backend_type = os.environ.get("HERMES_STORAGE_BACKEND", "local").strip().lower()

    backend_type = backend_type.strip().lower()

    if backend_type == "local":
        logger.info("Initializing local storage backends (SQLite + filesystem)")
        from .local import LocalStorageBackendSet
        return LocalStorageBackendSet().create()

    elif backend_type == "postgres":
        if postgres_url is None:
            postgres_url = os.environ.get("HERMES_STORAGE_POSTGRES_URL", "").strip()

        if not postgres_url:
            raise ValueError(
                "PostgreSQL backend requires HERMES_STORAGE_POSTGRES_URL env var or postgres_url parameter"
            )

        logger.info("Initializing PostgreSQL storage backends")
        from .postgres import PostgresStorageBackendSet
        return PostgresStorageBackendSet(postgres_url).create()

    else:
        raise ValueError(
            f"Unknown storage backend type: {backend_type}. Expected 'local' or 'postgres'."
        )
