"""Core async functions."""
import asyncio
import inspect
import sys
from pathlib import Path
from typing import Any, Awaitable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import ujson as json
from aiohttp import ClientResponseError, ClientSession
from aiohttp.typedefs import StrOrURL
from aiohttp_client_cache import CachedSession, SQLiteBackend

from .exceptions import InputTypeError, InputValueError, ServiceError

try:
    import nest_asyncio
except ImportError:
    nest_asyncio = None

EXPIRE = -1


def create_cachefile(db_name: Union[str, Path, None] = None) -> Path:
    """Create a cache folder in the current working directory."""
    fname = Path("cache", "aiohttp_cache.sqlite") if db_name is None else Path(db_name)
    fname.parent.mkdir(parents=True, exist_ok=True)
    return fname


async def retriever(
    uid: int,
    url: StrOrURL,
    s_kwds: Dict[str, Optional[Dict[str, Any]]],
    session: CachedSession,
    read_type: str,
    r_kwds: Dict[str, None],
) -> Tuple[int, Union[str, Awaitable[Union[str, bytes, Dict[str, Any]]]]]:
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
        Return response as text, bytes, or json.
    r_kwds : dict
        Keywords to pass to the response read function.
        It is ``{"content_type": None}`` if ``read`` is ``json``
        else an empty ``dict``.

    Returns
    -------
    bytes
        The retrieved response as binary.
    """
    async with session(url, **s_kwds) as response:
        try:
            return uid, await getattr(response, read_type)(**r_kwds)
        except (ClientResponseError, ValueError) as ex:
            raise ServiceError(await response.text(), str(response.url)) from ex


async def stream_session(
    url: StrOrURL,
    s_kwds: Dict[str, Optional[Dict[str, Any]]],
    session: ClientSession,
    filepath: Path,
) -> None:
    """Stream the response to a file."""
    async with session(url, **s_kwds) as response:
        if response.status != 200:
            raise ServiceError(await response.text(), str(response.url))

        with filepath.open("wb") as fd:
            async for chunk, _ in response.content.iter_chunks():
                fd.write(chunk)


def get_event_loop() -> Tuple[asyncio.AbstractEventLoop, bool]:
    """Create an event loop."""
    try:
        loop = asyncio.get_running_loop()
        new_loop = False
    except RuntimeError:
        loop = asyncio.new_event_loop()
        new_loop = True
    asyncio.set_event_loop(loop)
    if "IPython" in sys.modules:
        if nest_asyncio is None:
            raise ImportError("nest-asyncio")
        nest_asyncio.apply(loop)
    return loop, new_loop


async def delete_url(
    url: StrOrURL, method: str = "GET", cache_name: Optional[Path] = None, **kwargs: Dict[str, Any]
) -> None:
    """Delete cached response associated with ``url``."""
    cache = SQLiteBackend(cache_name=cache_name)
    await cache.delete_url(url, method, **kwargs)


class BaseRetriever:
    """Base class for async retriever."""

    def __init__(
        self,
        urls: Sequence[StrOrURL],
        file_paths: Optional[List[Union[str, Path]]] = None,
        read_method: Optional[str] = None,
        request_kwds: Optional[Sequence[Dict[str, Any]]] = None,
        request_method: str = "GET",
        cache_name: Optional[Union[Path, str]] = None,
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
        request_kwds: Optional[Sequence[Dict[str, Any]]],
        file_paths: Optional[List[Path]],
    ) -> Iterable[Tuple[Union[int, Path], StrOrURL, Dict[str, Any]]]:
        """Generate urls and keywords."""
        if not isinstance(urls, (list, tuple)):
            raise InputTypeError("``urls``", "list of str", "[url1, ...]")

        if file_paths is None:
            url_id = range(len(urls))
        else:
            if len(urls) != len(file_paths):
                msg = "``urls`` and ``file_paths`` must have the same size."
                raise ValueError(msg)
            url_id = file_paths  # type: ignore

        if request_kwds is None:
            return zip(url_id, urls, len(urls) * [{"headers": None}])

        if len(urls) != len(request_kwds):
            msg = "``urls`` and ``request_kwds`` must have the same size."
            raise ValueError(msg)

        session_kwds = inspect.signature(CachedSession._request).parameters.keys()
        not_found = [p for kwds in request_kwds for p in kwds if p not in session_kwds]
        if len(not_found) > 0:
            invalids = ", ".join(not_found)
            raise InputValueError(f"request_kwds ({invalids})", list(session_kwds))

        return zip(url_id, urls, request_kwds)
