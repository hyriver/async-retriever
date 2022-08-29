"""Top-level package."""
import asyncio
import sys
from importlib.metadata import PackageNotFoundError, version

from .async_retriever import (
    delete_url_cache,
    retrieve,
    retrieve_binary,
    retrieve_json,
    retrieve_text,
    stream_write,
)
from .exceptions import InputTypeError, InputValueError, ServiceError
from .print_versions import show_versions

try:
    __version__ = version("async_retriever")
except PackageNotFoundError:
    __version__ = "999"

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

__all__ = [
    "retrieve",
    "stream_write",
    "retrieve_text",
    "retrieve_json",
    "retrieve_binary",
    "delete_url_cache",
    "InputTypeError",
    "InputValueError",
    "ServiceError",
    "show_versions",
]
