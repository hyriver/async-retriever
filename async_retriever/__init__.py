"""Top-level package."""
import asyncio
import sys

from pkg_resources import DistributionNotFound, get_distribution

from .async_retriever import retrieve
from .exceptions import InvalidInputType, InvalidInputValue
from .print_versions import show_versions

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
