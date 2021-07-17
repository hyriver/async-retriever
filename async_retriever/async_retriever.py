"""Core async functions."""
import asyncio
import inspect
import socket
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import cytoolz as tlz
import nest_asyncio
import orjson as json
from aiohttp import ClientResponseError, ContentTypeError, TCPConnector
from aiohttp.typedefs import StrOrURL
from aiohttp_client_cache import CacheBackend, CachedSession, SQLiteBackend

from .exceptions import InvalidInputType, InvalidInputValue, ServiceError

_EXPIRE = 24 * 60 * 60
__all__ = ["retrieve"]


def create_cachefile(db_name: str = "aiohttp_cache") -> Path:
    """Create a cache folder in the current working directory."""
    Path("cache").mkdir(exist_ok=True)
    return Path("cache", f"{db_name}.sqlite")


async def _retrieve(
    uid: int,
    url: StrOrURL,
    session: CachedSession,
    read_type: str,
    kwds: Dict[str, Optional[Dict[str, Any]]],
) -> Tuple[int, Union[str, Awaitable[Union[str, bytes, Dict[str, Any]]]]]:
    """Create an async request and return the response as binary.

    Parameters
    ----------
    uid : int
        ID of the URL for sorting after returning the results
    url : str
        URL to be retrieved
    session : ClientSession
        A ClientSession for sending the request
    read_type : str
        Return response as text, bytes, or json.
    kwds: dict
        Arguments to be passed to requests

    Returns
    -------
    bytes
        The retrieved response as binary.
    """
    async with session(url, **kwds) as response:
        try:
            response.raise_for_status()
            if read_type == "json":
                resp = await getattr(response, read_type)(content_type=None)
            else:
                resp = await getattr(response, read_type)()
        except (ClientResponseError, ContentTypeError) as ex:
            raise ServiceError(await response.text()) from ex
        else:
            return uid, resp


async def async_session(
    url_kwds: Tuple[Tuple[int, StrOrURL, Dict[StrOrURL, Any]], ...],
    read: str,
    request_method: str,
    cache_name: Union[Path, str],
    family: str = "both",
) -> Callable[[int], Union[str, Awaitable[Union[str, bytes, Dict[str, Any]]]]]:
    """Create an async session for sending requests.

    Parameters
    ----------
    url_kwds : list of tuples of urls and payloads
        A list of URLs or URLs with their payloads to be retrieved.
    read : str
        The method for returning the request; ``binary`` (bytes), ``json``, and ``text``.
    request_method : str
        The request type; GET or POST.
    cache_name : str, optional
        Path to a folder for caching the session, defaults to
        ``cache/aiohttp_cache.sqlite``.
    family : str, optional
        TCP socket family, defaults to both, i.e., IPv4 and IPv6. For IPv4
        or IPv6 only pass ``ipv4`` or ``ipv6``, respectively.

    Returns
    -------
    asyncio.gather
        An async gather function
    """
    cache = SQLiteBackend(
        cache_name=cache_name,
        expire_after=_EXPIRE,
        allowed_methods=("GET", "POST"),
        timeout=5.0,
    )
    valid_family = {"both": 0, "ipv4": socket.AF_INET, "ipv6": socket.AF_INET6}
    if family not in valid_family:
        raise InvalidInputValue("family", list(valid_family.keys()))

    connector = TCPConnector(family=valid_family[family])

    if read == "binary":
        read = "read"

    async with CachedSession(
        json_serialize=json.dumps,
        cache=cache,
        connector=connector,
    ) as session:
        request_func = getattr(session, request_method.lower())
        tasks = (_retrieve(uid, u, request_func, read, kwds) for uid, u, kwds in url_kwds)
        return await asyncio.gather(*tasks)


async def clean_cache(cache_name: Union[Path, str]) -> None:
    """Remove expired responses from the cache file."""
    cache = SQLiteBackend(
        cache_name=cache_name,
        expire_after=_EXPIRE,
        allowed_methods=("GET", "POST"),
        timeout=5.0,
    )
    await cache.delete_expired_responses()


def retrieve(
    urls: Union[StrOrURL, List[StrOrURL], Tuple[StrOrURL, ...]],
    read: str,
    request_kwds: Optional[List[Dict[str, Any]]] = None,
    request_method: str = "GET",
    max_workers: int = 8,
    cache_name: Optional[Union[Path, str]] = None,
    family: str = "both",
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
    family : str, optional
        TCP socket family, defaults to both, i.e., IPv4 and IPv6. For IPv4
        or IPv6 only pass ``ipv4`` or ``ipv6``, respectively.

    Returns
    -------
    list
        List of responses in the order of input URLs.

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
        url_kwds = zip(range(len(urls)), urls, len(urls) * [{"headers": None}])
    else:
        if len(urls) != len(request_kwds):
            msg = "``urls`` and ``request_kwds`` must have the same size."
            raise ValueError(msg)

        session_kwds = inspect.signature(CachedSession._request).parameters.keys()
        not_found = [p for kwds in request_kwds for p in kwds if p not in session_kwds]
        if len(not_found) > 0:
            invalids = ", ".join(not_found)
            raise InvalidInputValue(f"request_kwds ({invalids})", list(session_kwds))

        url_kwds = zip(range(len(urls)), urls, request_kwds)

    loop = asyncio.new_event_loop()
    nest_asyncio.apply(loop)
    asyncio.set_event_loop(loop)

    cache_name = create_cachefile() if cache_name is None else cache_name
    asyncio.run(clean_cache(cache_name))

    chunked_reqs = tlz.partition_all(max_workers, url_kwds)
    results = (
        loop.run_until_complete(
            async_session(c, read, request_method.upper(), cache_name, family),
        )
        for c in chunked_reqs
    )

    return [r for _, r in sorted(tlz.concat(results))]
