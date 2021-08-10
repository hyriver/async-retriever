"""Top-level package."""
import asyncio
import sys

from pkg_resources import DistributionNotFound, get_distribution

from .async_retriever import clean_cache, retrieve
from .exceptions import InvalidInputType, InvalidInputValue, ServiceError
from .print_versions import show_versions

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    __version__ = "999"

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

__all__ = [
    "retrieve",
    "clean_cache",
    "InvalidInputType",
    "InvalidInputValue",
    "ServiceError",
    "show_versions",
]
