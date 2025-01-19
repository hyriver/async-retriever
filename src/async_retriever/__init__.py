"""Top-level package."""

from __future__ import annotations

import asyncio
import sys
from importlib.metadata import PackageNotFoundError, version

from async_retriever import exceptions
from async_retriever.async_retriever import (
    delete_url_cache,
    retrieve,
    retrieve_binary,
    retrieve_json,
    retrieve_text,
)
from async_retriever.print_versions import show_versions
from async_retriever.streaming import generate_filename, stream_write

try:
    __version__ = version("async_retriever")
except PackageNotFoundError:
    __version__ = "999"

if sys.platform == "win32":  # pragma: no cover
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

__all__ = [
    "__version__",
    "delete_url_cache",
    "exceptions",
    "generate_filename",
    "retrieve",
    "retrieve_binary",
    "retrieve_json",
    "retrieve_text",
    "show_versions",
    "stream_write",
]
