"""Core async functions."""
import asyncio
import inspect
import socket
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import cytoolz as tlz
import nest_asyncio
import ujson as json
from aiohttp import ClientResponseError, ContentTypeError, TCPConnector
from aiohttp.typedefs import StrOrURL
from aiohttp_client_cache import CacheBackend, CachedSession, SQLiteBackend

from .exceptions import InvalidInputType, InvalidInputValue, ServiceError

_EXPIRE = 24 * 60 * 60
__all__ = ["retrieve", "clean_cache"]


def create_cachefile(db_name: Union[str, Path, None] = None) -> Path:
    """Create a cache folder in the current working directory."""
    fname = Path("cache", "aiohttp_cache.sqlite") if db_name is None else Path(db_name)
    fname.parent.mkdir(parents=True, exist_ok=True)
    return fname


async def _retrieve(
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
    s_kwds: dict
        Arguments to be passed to requests
    session : ClientSession
        A ClientSession for sending the request
    read_type : str
        Return response as text, bytes, or json.
    r_kwds : dict
        Keywords to pass to the response read function. ``{"content_type": None}`` if read
        is ``json`` else it's empty.

    Returns
    -------
    bytes
        The retrieved response as binary.
    """
    async with session(url, **s_kwds) as response:
        try:
            return uid, await getattr(response, read_type)(**r_kwds)
        except (ClientResponseError, ContentTypeError, ValueError) as ex:
            raise ServiceError(await response.text()) from ex


async def async_session(
    url_kwds: Tuple[Tuple[int, StrOrURL, Dict[StrOrURL, Any]], ...],
    read: str,
    r_kwds: Dict[str, Any],
    request_method: str,
    cache_name: Union[Path, str],
    family: int,
) -> Callable[[int], Union[str, Awaitable[Union[str, bytes, Dict[str, Any]]]]]:
    """Create an async session for sending requests.

    Parameters
    ----------
    url_kwds : list of tuples of urls and payloads
        A list of URLs or URLs with their payloads to be retrieved.
    read : str
        The method for returning the request; ``binary`` (bytes), ``json``, and ``text``.
    r_kwds : dict
        Keywords to pass to the response read function. ``{"content_type": None}`` if read
        is ``json`` else it's empty.
    request_method : str
        The request type; GET or POST.
    cache_name : str
        Path to a file for caching the session, defaults to
        ``./cache/aiohttp_cache.sqlite``.
    family : int
        TCP socket family

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

    connector = TCPConnector(family=family)

    async with CachedSession(
        json_serialize=json.dumps,
        cache=cache,
        connector=connector,
        trust_env=True,
    ) as session:
        request_func = getattr(session, request_method.lower())
        tasks = (
            _retrieve(uid, url, kwds, request_func, read, r_kwds) for uid, url, kwds in url_kwds
        )
        return await asyncio.gather(*tasks)


async def _clean_cache(cache_name: Union[Path, str]) -> None:
    """Remove expired responses from the cache file."""
    cache = CacheBackend(cache_name=cache_name)
    await cache.delete_expired_responses()


def clean_cache(cache_name: Union[Path, str]) -> None:
    """Remove expired responses from the cache file."""
    cache_name = create_cachefile(cache_name)
    asyncio.run(_clean_cache(cache_name))


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
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
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
    inp = ValidateInputs(urls, read, request_kwds, request_method, cache_name, family)

    loop = asyncio.new_event_loop()
    nest_asyncio.apply(loop)
    asyncio.set_event_loop(loop)

    chunked_reqs = tlz.partition_all(max_workers, inp.url_kwds)
    results = (
        loop.run_until_complete(
            async_session(c, inp.read, inp.r_kwds, inp.request_method, inp.cache_name, inp.family),
        )
        for c in chunked_reqs
    )

    return [r for _, r in sorted(tlz.concat(results))]


class ValidateInputs:
    def __init__(
        self,
        urls: Union[StrOrURL, List[StrOrURL], Tuple[StrOrURL, ...]],
        read: str,
        request_kwds: Optional[List[Dict[str, Any]]] = None,
        request_method: str = "GET",
        cache_name: Optional[Union[Path, str]] = None,
        family: str = "both",
    ) -> None:
        """Validate inputs to retrieve function."""
        self.request_method = request_method.upper()
        valid_methods = ["GET", "POST"]
        if self.request_method not in valid_methods:
            raise InvalidInputValue("method", valid_methods)

        valid_reads = ["binary", "json", "text"]
        if read not in valid_reads:
            raise InvalidInputValue("read", valid_reads)
        self.read = "read" if read == "binary" else read
        self.r_kwds = {"content_type": None, "loads": json.loads} if read == "json" else {}

        self.url_kwds = self.generate_requests(urls, request_kwds)

        valid_family = {"both": 0, "ipv4": socket.AF_INET, "ipv6": socket.AF_INET6}
        if family not in valid_family:
            raise InvalidInputValue("family", list(valid_family.keys()))
        self.family = valid_family[family]

        self.cache_name = create_cachefile(cache_name)

    @staticmethod
    def generate_requests(
        urls: Union[StrOrURL, List[StrOrURL], Tuple[StrOrURL, ...]],
        request_kwds: Optional[List[Dict[str, Any]]],
    ):
        """Generate urls and keywords."""
        if not isinstance(urls, (list, tuple)):
            raise InvalidInputType("``urls``", "list of str", "[url1, ...]")

        if request_kwds is None:
            return zip(range(len(urls)), urls, len(urls) * [{"headers": None}])

        if len(urls) != len(request_kwds):
            msg = "``urls`` and ``request_kwds`` must have the same size."
            raise ValueError(msg)

        session_kwds = inspect.signature(CachedSession._request).parameters.keys()
        not_found = [p for kwds in request_kwds for p in kwds if p not in session_kwds]
        if len(not_found) > 0:
            invalids = ", ".join(not_found)
            raise InvalidInputValue(f"request_kwds ({invalids})", list(session_kwds))
        return zip(range(len(urls)), urls, request_kwds)
