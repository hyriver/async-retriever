"""Core async functions."""
import asyncio
import inspect
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import cytoolz as tlz
import nest_asyncio
import orjson as json
from aiohttp_client_cache import CacheBackend, CachedSession, SQLiteBackend

from .exceptions import InvalidInputType, InvalidInputValue

EXPIRE = 24 * 60 * 60


def _create_cachefile(db_name: str = "aiohttp_cache") -> Optional[Path]:
    """Create a cache file if dependencies are met."""
    if sys.platform.startswith("win"):
        return Path(tempfile.gettempdir(), f"{db_name}.sqlite")

    return Path(f"~/.cache/{db_name}.sqlite")


async def _request_binary(
    url: str,
    session_req: CachedSession,
    **kwds: Dict[str, Optional[Dict[str, Any]]],
) -> bytes:
    """Create an async request and return the response as binary.

    Parameters
    ----------
    url : str
        URL to be retrieved
    session_req : ClientSession
        A ClientSession for sending the request
    **kwds: dict
        Arguments to be passed to requests

    Returns
    -------
    bytes
        The retrieved response as binary
    """
    async with session_req(url, **kwds) as response:
        return await response.read()


async def _request_json(
    url: str,
    session_req: CachedSession,
    **kwds: Dict[str, Optional[Dict[str, Any]]],
) -> Dict[str, Any]:
    """Create an async request and return the response as json.

    Parameters
    ----------
    url : str
        URL to be retrieved.
    session_req : ClientSession
        A ClientSession for sending the request
    **kwds: dict
        Arguments to be passed to requests

    Returns
    -------
    dict
        The retrieved response as json
    """
    async with session_req(url, **kwds) as response:
        return await response.json()


async def _request_text(
    url: str,
    session_req: CachedSession,
    **kwds: Dict[str, Optional[Dict[str, Any]]],
) -> str:
    """Create an async request and return the response as a string.

    Parameters
    ----------
    url : str
        URL to be retrieved
    session_req : ClientSession
        A ClientSession for sending the request
    **kwds: dict
        Arguments to be passed to requests

    Returns
    -------
    dict
        The retrieved response as string
    """
    async with session_req(url, **kwds) as response:
        return await response.text()


async def _async_session(
    url_kwds: Tuple[Tuple[str, Dict[str, Any]], ...],
    read: str,
    request: str,
    cache_name: Optional[Union[Path, str]],
) -> Callable:
    """Create an async session for sending requests.

    Parameters
    ----------
    url_kwds : list of tuples of urls and payloads
        A list of URLs or URLs with their payloads to be retrieved.
    read : str
        The method for returning the request; binary, json, and text.
    request : str
        The request type; GET or POST.
    cache_name : str
        Path to a folder for caching the session.
        It is recommended to use caching when you're going to make multiple
        requests with a session. It can significantly speed up the function.

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
        read_method = {"binary": _request_binary, "json": _request_json, "text": _request_text}
        if read not in read_method:
            raise InvalidInputValue("read", list(read_method.keys()))

        request_method = {"GET": session.get, "POST": session.post}
        if request not in request_method:
            raise InvalidInputValue("method", list(request_method.keys()))

        tasks = (read_method[read](u, request_method[request], **kwds) for u, kwds in url_kwds)
        return await asyncio.gather(*tasks, return_exceptions=True)  # type: ignore


async def _clean_cache(cache_name: Union[Path, str]) -> None:
    """Remove expired responses from the cache file."""
    cache = CacheBackend(cache_name=cache_name)
    await cache.delete_expired_responses()


def retrieve(
    urls: List[str],
    read: str,
    request_kwds: Optional[List[Dict[str, Any]]] = None,
    request: str = "GET",
    max_workers: int = 8,
    cache_name: Optional[Union[Path, str]] = None,
) -> List[Union[str, Dict[str, Any], bytes]]:
    """Send async requests.

    This function is based on
    `this <https://github.com/HydrologicEngineeringCenter/data-retrieval-scripts/blob/master/qpe_async_download.py>`__
    script.

    Parameters
    ----------
    urls : list of str
        A list of URLs.
    read : str
        The method for returning the request; binary, json, and text.
    request_kwds : list of dict, optional
        A list of requests kwds corresponding to input URLs (1 on 1 mapping), defaults to None.
        For example, [{"params": {...}, "headers": {...}}, ...].
    request : str, optional
        The request type; GET or POST, defaults to GET.
    max_workers : int, optional
        The maximum number of async processes, defaults to 8.
    cache_name : str, optional
        Path to a file for caching the session, default to None which uses a file
        called aiohttp_cache.sqlite under the systems' cache directory: ~/.cache
        for Linux and MacOS, and %Temp% for Windows.

    Returns
    -------
    list
        A list of responses which are not in the order of input requests.

    Examples
    --------
    >>> import async_retriever as ar
    >>> stations = ["01646500", "08072300", "11073495"]
    >>> base = "https://waterservices.usgs.gov/nwis/site"
    >>> urls, kwds = zip(
    ...     *[
    ...         (base, {"params": {"format": "rdb", "sites": s, "siteStatus": "all"}})
    ...         for s in stations
    ...     ]
    ... )
    >>> resp = ar.retrieve(urls, "text", request_kwds=kwds)
    >>> resp[0].split('\\n')[-2].split('\\t')[1]
    '01646500'
    """
    if not isinstance(urls, Iterable):
        raise InvalidInputType("``urls``", "iterable of str")

    if request_kwds is None:
        url_kwds = zip(urls, len(urls) * [{"headers": None}])
    else:
        if len(urls) != len(request_kwds):
            raise ValueError("``urls`` and ``request_kwds`` must have the same size.")

        session_kwds = inspect.signature(CachedSession._request).parameters.keys()
        not_found = [p for kwds in request_kwds for p in kwds if p not in session_kwds]
        if len(not_found) > 0:
            raise InvalidInputValue("request_kwds", list(session_kwds))

        url_kwds = zip(urls, request_kwds)

    cache_name = _create_cachefile() if cache_name is None else cache_name
    chunked_urls = tlz.partition_all(max_workers, url_kwds)

    loop = asyncio.new_event_loop()
    nest_asyncio.apply(loop)

    asyncio.set_event_loop(loop)

    results = (
        loop.run_until_complete(_async_session(c, read, request, cache_name)) for c in chunked_urls
    )

    if cache_name is not None:
        asyncio.run(_clean_cache(cache_name))

    return list(tlz.concat(results))
