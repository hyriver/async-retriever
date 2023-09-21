"""Core async functions."""
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Iterable, Literal, Sequence

import ujson as json
from aiohttp import ClientResponseError, ClientSession
from aiohttp_client_cache import SQLiteBackend

from async_retriever.exceptions import (
    DependencyError,
    InputTypeError,
    InputValueError,
    ServiceError,
)

if TYPE_CHECKING:
    from aiohttp.client import _RequestContextManager  # type: ignore
    from aiohttp.typedefs import StrOrURL


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
    read_type: Literal["text", "json", "binary"],
    r_kwds: dict[str, None],
    raise_status: bool,
) -> tuple[int, str | Awaitable[str | bytes | dict[str, Any]] | None]:
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
        Return response as ``text``, ``bytes``, or ``json``.
    r_kwds : dict
        Keywords to pass to the response read function.
        It is ``{"content_type": None}`` if ``read`` is ``json``
        else an empty ``dict``.
    raise_status : bool
        Raise an exception if the response status is not 200. If
        ``False`` return ``None``.

    Returns
    -------
    bytes
        The retrieved response as binary.
    """
    async with session(url, **s_kwds) as response:
        try:
            return uid, await getattr(response, read_type)(**r_kwds)
        except (ClientResponseError, ValueError) as ex:
            if raise_status:
                raise ServiceError(await response.text(), str(response.url)) from ex
            return uid, None


async def stream_session(
    url: StrOrURL,
    s_kwds: dict[str, dict[str, Any] | None],
    session: Callable[[StrOrURL], _RequestContextManager],
    filepath: Path,
    chunk_size: int | None = None,
) -> None:
    """Stream the response to a file."""
    async with session(url, **s_kwds) as response:
        if response.status != 200:
            raise ServiceError(await response.text(), str(response.url))
        with filepath.open("wb") as fd:
            if chunk_size is None:
                async for chunk, _ in response.content.iter_chunks():
                    fd.write(chunk)
            else:
                async for chunk in response.content.iter_chunked(chunk_size):
                    fd.write(chunk)


def get_event_loop() -> tuple[asyncio.AbstractEventLoop, bool]:
    """Create an event loop."""
    try:
        loop = asyncio.get_running_loop()
        new_loop = False
    except RuntimeError:
        loop = asyncio.new_event_loop()
        new_loop = True
    asyncio.set_event_loop(loop)
    if "IPython" in sys.modules:
        if importlib.util.find_spec("nest_asyncio") is None:
            raise DependencyError
        import nest_asyncio  # type: ignore

        nest_asyncio.apply(loop)
    return loop, new_loop


async def delete_url(
    url: StrOrURL,
    method: Literal["get", "GET", "post", "POST"],
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
        file_paths: list[str | Path] | None = None,
        read_method: Literal["text", "json", "binary"] | None = None,
        request_kwds: Sequence[dict[str, Any]] | None = None,
        request_method: Literal["get", "GET", "post", "POST"] = "GET",
        cache_name: Path | str | None = None,
    ) -> None:
        """Validate inputs to retrieve function."""
        self.request_method = request_method.upper()
        valid_methods = ["GET", "POST"]
        if self.request_method not in valid_methods:
            raise InputValueError("method", valid_methods)

        self.file_paths = None
        if file_paths is not None:
            if not isinstance(file_paths, (list, tuple)):
                raise InputTypeError("file_paths", "list of paths")
            self.file_paths = [Path(f) for f in file_paths]
            for f in self.file_paths:
                f.parent.mkdir(parents=True, exist_ok=True)

        self.read_method = None
        self.r_kwds = None
        if read_method is not None:
            valid_reads = ["binary", "json", "text"]
            if read_method not in valid_reads:
                raise InputValueError("read", valid_reads)
            self.read_method = "read" if read_method == "binary" else read_method
            self.r_kwds = (
                {"content_type": None, "loads": json.loads} if read_method == "json" else {}
            )

        self.url_kwds = self.generate_requests(urls, request_kwds, self.file_paths)

        self.cache_name = create_cachefile(cache_name)

    @staticmethod
    def generate_requests(
        urls: Sequence[StrOrURL],
        request_kwds: Sequence[dict[str, Any]] | None,
        file_paths: list[Path] | None,
    ) -> Iterable[tuple[int | Path, StrOrURL, dict[str, Any]]]:
        """Generate urls and keywords."""
        if not isinstance(urls, (list, tuple)):
            raise InputTypeError("``urls``", "list of str", "[url1, ...]")

        if file_paths is None:
            url_id = range(len(urls))
        else:
            if len(urls) != len(file_paths):
                msg = "``urls`` and ``file_paths`` must have the same size."
                raise ValueError(msg)
            url_id = file_paths

        if request_kwds is None:
            return zip(url_id, urls, len(urls) * [{"headers": None}])

        if len(urls) != len(request_kwds):
            msg = "``urls`` and ``request_kwds`` must have the same size."
            raise ValueError(msg)

        session_kwds = inspect.signature(ClientSession._request).parameters.keys()  # type: ignore
        not_found = [p for kwds in request_kwds for p in kwds if p not in session_kwds]
        if not_found:
            invalids = ", ".join(not_found)
            raise InputValueError(f"request_kwds ({invalids})", list(session_kwds))

        return zip(url_id, urls, request_kwds)
