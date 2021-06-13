"""Core async functions."""
import asyncio
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import cytoolz as tlz
import nest_asyncio
import orjson as json
from aiohttp_client_cache import CacheBackend, CachedSession, SQLiteBackend

from .exceptions import InvalidInputType, InvalidInputValue

EXPIRE = 24 * 60 * 60


def _create_cachefile(db_name: str = "aiohttp_cache") -> Path:
    """Create a cache folder in the current working directory."""
    Path("cache").mkdir(exist_ok=True)
    return Path("cache", f"{db_name}.sqlite")


@dataclass
class AsyncRequest:
    """Async send/request.

    Parameters
    ----------
    url : str
        URL to be retrieved
    session_req : ClientSession
        A ClientSession for sending the request
    kwds: dict
        Arguments to be passed to requests
    """

    url: str
    session_req: CachedSession
    kwds: Dict[str, Optional[Dict[str, Any]]]

    async def binary(self) -> bytes:
        """Create an async request and return the response as binary.

        Returns
        -------
        bytes
            The retrieved response as binary.
        """
        async with self.session_req(self.url, **self.kwds) as response:
            return await response.read()

    async def json(self) -> Dict[str, Any]:
        """Create an async request and return the response as json.

        Returns
        -------
        dict
            The retrieved response as json.
        """
        async with self.session_req(self.url, **self.kwds) as response:
            return await response.json()

    async def text(self) -> str:
        """Create an async request and return the response as a string.

        Returns
        -------
        str
            The retrieved response as string.
        """
        async with self.session_req(self.url, **self.kwds) as response:
            return await response.text()


async def _async_session(
    url_kwds: Tuple[Tuple[str, Dict[str, Any]], ...],
    read: str,
    request_method: str,
    cache_name: Union[Path, str],
) -> Callable[..., Awaitable[Any]]:
    """Create an async session for sending requests.

    Parameters
    ----------
    url_kwds : list of tuples of urls and payloads
        A list of URLs or URLs with their payloads to be retrieved.
    read : str
        The method for returning the request; binary, json, and text.
    request_method : str
        The request type; GET or POST.
    cache_name : str, optional
        Path to a folder for caching the session, defaults to
        ``cache/aiohttp_cache.sqlite``.

    Returns
    -------
    asyncio.gather
        An async gather function
    """
    cache = SQLiteBackend(
        cache_name=cache_name,
        expire_after=EXPIRE,
        allowed_methods=("GET", "POST"),
        timeout=2.5,
    )

    async with CachedSession(json_serialize=json.dumps, cache=cache) as session:
        request_func = getattr(session, request_method.lower())
        tasks = (getattr(AsyncRequest(u, request_func, kwds), read)() for u, kwds in url_kwds)
        return await asyncio.gather(*tasks, return_exceptions=True)


async def _clean_cache(cache_name: Union[Path, str]) -> None:
    """Remove expired responses from the cache file."""
    cache = CacheBackend(cache_name=cache_name)
    await cache.delete_expired_responses()


def retrieve(
    urls: List[str],
    read: str,
    request_kwds: Optional[List[Dict[str, Any]]] = None,
    request_method: str = "GET",
    max_workers: int = 8,
    cache_name: Optional[Union[Path, str]] = None,
) -> List[Union[str, Dict[str, Any], bytes]]:
    r"""Send async requests.

    Parameters
    ----------
    urls : list of str
        List of URLs.
    read : str
        Method for returning the request; ``binary``, ``json``, and ``text``.
    request_kwds : list of dict, optional
        List of requests keywords corresponding to input URLs (1 on 1 mapping), defaults to None.
        For example, ``[{"params": {...}, "headers": {...}}, ...]``.
    request_method : str, optional
        Request type; ``GET`` (``get``) or ``POST`` (``post``). Defaults to ``GET``.
    max_workers : int, optional
        Maximum number of async processes, defaults to 8.
    cache_name : str, optional
        Path to a folder for caching the session, defaults to ``cache/aiohttp_cache.sqlite``.

    Returns
    -------
    list
        List of responses which are not necessarily in the order of input requests.

    Examples
    --------
    >>> import async_retriever as ar
    >>> stations = ["01646500", "08072300", "11073495"]
    >>> url = "https://waterservices.usgs.gov/nwis/site"
    >>> urls, kwds = zip(
    ...     *[
    ...         (url, {"params": {"format": "rdb", "sites": s, "siteStatus": "all"}})
    ...         for s in stations
    ...     ]
    ... )
    >>> resp = ar.retrieve(urls, "text", request_kwds=kwds)
    >>> resp[0].split('\n')[-2].split('\t')[1]
    '01646500'
    """
    if not isinstance(urls, (list, tuple)):
        raise InvalidInputType("``urls``", "list of str", "[url1, ...]")

    valid_methods = ["GET", "POST"]
    if request_method.upper() not in valid_methods:
        raise InvalidInputValue("method", valid_methods)

    valid_reads = ["binary", "json", "text"]
    if read not in valid_reads:
        raise InvalidInputValue("read", valid_reads)

    if request_kwds is None:
        url_kwds = zip(urls, len(urls) * [{"headers": None}])
    else:
        if len(urls) != len(request_kwds):
            raise ValueError("``urls`` and ``request_kwds`` must have the same size.")

        session_kwds = inspect.signature(CachedSession._request).parameters.keys()
        not_found = [p for kwds in request_kwds for p in kwds if p not in session_kwds]
        if len(not_found) > 0:
            invalids = ", ".join(not_found)
            raise InvalidInputValue(f"request_kwds ({invalids})", list(session_kwds))

        url_kwds = zip(urls, request_kwds)

    loop = asyncio.new_event_loop()
    nest_asyncio.apply(loop)
    asyncio.set_event_loop(loop)

    cache_name = _create_cachefile() if cache_name is None else cache_name
    chunked_reqs = tlz.partition_all(max_workers, url_kwds)
    results = (
        loop.run_until_complete(_async_session(c, read, request_method.upper(), cache_name))
        for c in chunked_reqs
    )

    asyncio.run(_clean_cache(cache_name))

    return list(tlz.concat(results))
