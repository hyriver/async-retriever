"""Top-level package."""
import asyncio
import importlib.metadata
import sys

from .async_retriever import (
    delete_url_cache,
    retrieve,
    retrieve_binary,
    retrieve_json,
    retrieve_text,
    stream_write,
)
from .exceptions import InvalidInputType, InvalidInputValue, ServiceError
from .print_versions import show_versions

__version__ = importlib.metadata.version("async_retriever")

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

__all__ = [
    "retrieve",
    "stream_write",
    "retrieve_text",
    "retrieve_json",
    "retrieve_binary",
    "delete_url_cache",
    "InvalidInputType",
    "InvalidInputValue",
    "ServiceError",
    "show_versions",
]
