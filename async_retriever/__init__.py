"""Top-level package."""
import asyncio
import sys

from .async_retriever import delete_url_cache, retrieve
from .exceptions import InvalidInputType, InvalidInputValue, ServiceError
from .print_versions import show_versions

try:
    import importlib.metadata as metadata
except ImportError:
    import importlib_metadata as metadata  # type: ignore[no-redef]

try:
    __version__ = metadata.version("async_retriever")
except Exception:
    __version__ = "999"

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

__all__ = [
    "retrieve",
    "delete_url_cache",
    "InvalidInputType",
    "InvalidInputValue",
    "ServiceError",
    "show_versions",
]
