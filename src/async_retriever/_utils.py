"""Core async functions."""

from __future__ import annotations

import asyncio
import atexit
import os
from datetime import datetime
from inspect import signature
from itertools import repeat
from pathlib import Path
from ssl import PROTOCOL_TLS_CLIENT, SSLContext
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeVar

import orjson as json
from aiohttp import ClientResponseError, ClientSession
from aiohttp_client_cache import SQLiteBackend

from async_retriever.exceptions import (
    InputTypeError,
    InputValueError,
    ServiceError,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine, Sequence

    from aiohttp.client import _RequestContextManager  # pyright: ignore[reportPrivateUsage]
    from aiohttp.typedefs import StrOrURL

    T = TypeVar("T")

MAX_HOSTS = 4  # Maximum connections to a single host (rate-limited service)
TIMEOUT = 2 * 60  # Timeout for requests in seconds


class _AsyncLoopThread(Thread):
    _instance: _AsyncLoopThread | None = None
    _lock = Lock()
    _cleanup_registered = False

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.loop = asyncio.new_event_loop()
        self._running = Event()

    @classmethod
    def _cleanup(cls) -> None:
        """Safe cleanup method for atexit."""
        with cls._lock:
            instance = cls._instance
            if instance is not None:
                instance.stop()
                cls._instance = None

    @classmethod
    def get_instance(cls) -> _AsyncLoopThread:
        """Get or create the singleton instance of AsyncLoopThread."""
        if cls._instance is None or not cls._instance.is_alive():
            with cls._lock:
                # Check again in case another thread created instance
                if cls._instance is None or not cls._instance.is_alive():
                    instance = cls()
                    instance.start()
                    if not cls._cleanup_registered:
                        atexit.register(cls._cleanup)
                        cls._cleanup_registered = True
                    cls._instance = instance
        return cls._instance

    def run(self) -> None:
        """Run the event loop in this thread."""
        asyncio.set_event_loop(self.loop)
        self._running.set()
        try:
            self.loop.run_forever()
        finally:
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()
            self._running.clear()

    def stop(self) -> None:
        """Stop the event loop thread."""
        if self._running.is_set():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._running.wait()  # Wait for event to be cleared
            self.join()  # Wait for thread to finish


def _get_loop_handler() -> _AsyncLoopThread:
    """Get the global event loop thread handler, creating it if needed."""
    return _AsyncLoopThread.get_instance()


def run_in_event_loop(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine in the dedicated asyncio event loop."""
    handler = _get_loop_handler()
    return asyncio.run_coroutine_threadsafe(coro, handler.loop).result()


def create_cachefile(db_name: str | Path | None = None) -> Path:
    """Create a cache folder in the current working directory."""
    if db_name is not None:
        fname = Path(db_name)
    else:
        fname = Path(os.getenv("HYRIVER_CACHE_NAME", Path("cache", "aiohttp_cache.sqlite")))
    # Delete cache files created before v0.4 (2023-03-01) since from then on
    # the expiration date of caches are set to 1 week.
    if fname.exists() and datetime.fromtimestamp(fname.stat().st_ctime) < datetime(2023, 3, 1):
        fname.unlink()
    fname.parent.mkdir(parents=True, exist_ok=True)
    return fname


async def retriever(
    uid: int,
    url: StrOrURL,
    s_kwds: dict[str, dict[str, Any]],
    session: Callable[[StrOrURL], _RequestContextManager],
    read_type: Literal["text", "read", "json"],
    r_kwds: dict[str, None],
    raise_status: bool,
) -> (
    tuple[int, str]
    | tuple[int, bytes]
    | tuple[int, dict[str, Any]]
    | tuple[int, list[dict[str, Any]]]
    | tuple[int, None]
):
    """Create an async request and return the response as binary.

    Parameters
    ----------
    uid : int
        ID of the URL for sorting after returning the results
    url : str
        URL to be retrieved
    s_kwds : dict
        Arguments to be passed to requests
    session : ClientSession
        A ClientSession for sending the request
    read_type : str
        Return response as ``text``, ``read``, or ``json``.
    r_kwds : dict
        Keywords to pass to the response read function.
        It is ``{"content_type": None}`` if ``read`` is ``json``
        else an empty ``dict``.
    raise_status : bool
        Raise an exception if the response status is not 200. If
        ``False`` return ``None``.

    Returns
    -------
    tuple
        A tuple of the URL ID and the response content
    """
    async with session(url, **s_kwds) as response:
        try:
            return uid, await getattr(response, read_type)(**r_kwds)
        except UnicodeDecodeError:
            return uid, await response.text(encoding="latin1")
        except (ClientResponseError, ValueError) as ex:
            if raise_status:
                raise ServiceError(await response.text(), str(response.url)) from ex
            return uid, None


async def delete_url(
    url: StrOrURL,
    method: Literal["get", "post"],
    cache_name: Path,
    **kwargs: dict[str, Any],
) -> None:
    """Delete cached response associated with ``url``."""
    cache = SQLiteBackend(cache_name=str(cache_name))
    await cache.delete_url(url, method.upper(), **kwargs)


class BaseRetriever:
    """Base class for async retriever."""

    def __init__(
        self,
        urls: Sequence[StrOrURL],
        read_method: Literal["text", "json", "binary"],
        request_kwds: Sequence[dict[str, Any]] | None = None,
        request_method: Literal["get", "post"] = "get",
        cache_name: Path | str | None = None,
        ssl: SSLContext | bool = True,
    ) -> None:
        """Validate inputs to retrieve function."""
        ssl_cert = os.getenv("HYRIVER_SSL_CERT")
        if ssl_cert:
            self.ssl = SSLContext(PROTOCOL_TLS_CLIENT)
            self.ssl.load_verify_locations(ssl_cert)
        else:
            self.ssl = ssl

        valid_methods = ["get", "post"]
        if request_method.lower() not in valid_methods:
            raise InputValueError("method", valid_methods)
        self.request_method: Literal["get", "post"] = request_method
        valid_reads = ["binary", "json", "text"]
        if read_method not in valid_reads:
            raise InputValueError("read", valid_reads)
        self.read_method: Literal["read", "text", "json"] = (
            "read" if read_method == "binary" else read_method
        )
        self.r_kwds = {"content_type": None, "loads": json.loads} if read_method == "json" else {}

        self.url_kwds = self.generate_requests(urls, request_kwds)

        self.cache_name = create_cachefile(cache_name)

    @staticmethod
    def generate_requests(
        urls: Sequence[StrOrURL],
        request_kwds: Sequence[dict[str, Any]] | None,
    ) -> zip[tuple[int, StrOrURL, dict[str, Any]]]:
        """Generate urls and keywords."""
        if not isinstance(urls, (list, tuple)):
            raise InputTypeError("``urls``", "list of str", "[url1, ...]")

        url_id = range(len(urls))

        if request_kwds is None:
            return zip(url_id, urls, repeat({"headers": None}))

        if len(urls) != len(request_kwds):
            raise InputTypeError("urls/request_kwds", "sequences of the same size")

        valid_kwds = signature(ClientSession._request)  # pyright: ignore[reportPrivateUsage]
        not_found = [p for kwds in request_kwds for p in kwds if p not in valid_kwds.parameters]
        if not_found:
            invalids = f"request_kwds ({', '.join(not_found)})"
            raise InputValueError(invalids, list(valid_kwds.parameters))

        return zip(url_id, urls, request_kwds)
