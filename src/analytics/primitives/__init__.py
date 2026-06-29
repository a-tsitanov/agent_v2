# src/analytics/primitives/__init__.py
"""Importing this package registers every primitive into catalog.CATALOG.

Each family module calls catalog.register() for its primitives as a side effect.
Importing this package is therefore sufficient to populate the full CATALOG
(Wave-0 + Wave-1).
"""

from src.analytics.primitives import (
    aggregations,  # noqa: F401
    centrality,  # noqa: F401
    communities,  # noqa: F401
    connections,  # noqa: F401
    dynamics,  # noqa: F401
    events,  # noqa: F401
    quality,  # noqa: F401
    rollups,  # noqa: F401
    signals,  # noqa: F401
)
