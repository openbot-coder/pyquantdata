"""存储层：DuckDB 连接、migrations、联邦视图。"""

from .db import CatalogNotInitialized, configure_query_conn, open_catalog
from .migrations import (
    KNOWN_MAX_VERSION,
    MIGRATIONS,
    SchemaTooNewError,
    applied_versions,
    current_version,
    migrate,
)

__all__ = [
    "CatalogNotInitialized",
    "KNOWN_MAX_VERSION",
    "MIGRATIONS",
    "SchemaTooNewError",
    "applied_versions",
    "configure_query_conn",
    "current_version",
    "migrate",
    "open_catalog",
]
