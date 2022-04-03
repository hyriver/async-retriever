"""Core async functions."""
import asyncio
import os
from pathlib import Path
from ssl import SSLContext
from typing import Any, Awaitable, Dict, List, Optional, Sequence, Tuple, Union

import cytoolz as tlz
import ujson as json
from aiohttp import TCPConnector
from aiohttp.typedefs import StrOrURL
from aiohttp_client_cache import CachedSession, SQLiteBackend

from . import utils
from .exceptions import InvalidInputValue
from .utils import EXPIRE, BaseRetriever

__all__ = ["retrieve", "delete_url_cache", "retrieve_text", "retrieve_json", "retrieve_binary"]


async def async_session(
    url_kwds: Tuple[Tuple[int, StrOrURL, Dict[StrOrURL, Any]], ...],
    read: str,
    r_kwds: Dict[str, Any],
    request_method: str,
    cache_name: Path,
    timeout: float = 5.0,
    expire_after: int = EXPIRE,
    ssl: Union[SSLContext, bool, None] = None,
    disable: bool = False,
) -> Awaitable[Union[str, bytes, Dict[str, Any]]]:
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
    timeout : float, optional
        Timeout for the request, defaults to 5.0.
    expire_after : int, optional
        Expiration time for the cache in seconds, defaults to -1 (never expire).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to ``False`` to disable
        SSL certification verification.
    disable : bool, optional
        If ``True`` temporarily disable caching requests and get new responses
        from the server, defaults to ``False``.

    Returns
    -------
    asyncio.gather
        An async gather function
    """
    cache = SQLiteBackend(
        cache_name=os.getenv("HYRIVER_CACHE_NAME", cache_name),
        expire_after=int(os.getenv("HYRIVER_CACHE_EXPIRE", expire_after)),
        allowed_methods=("GET", "POST"),
        timeout=timeout,
    )
    connector = TCPConnector(ssl=ssl)
    disable = os.getenv("HYRIVER_CACHE_DISABLE", f"{disable}").lower() == "true"
    async with CachedSession(
        json_serialize=json.dumps,
        cache=cache,
        connector=connector,
        trust_env=True,
    ) as session:
        _session = session.disabled() if disable else session
        async with _session:
            request_func = getattr(session, request_method.lower())
            tasks = (
                utils.retriever(uid, url, kwds, request_func, read, r_kwds)
                for uid, url, kwds in url_kwds
            )
            return await asyncio.gather(*tasks)  # type: ignore


def delete_url_cache(
    url: StrOrURL,
    request_method: str = "GET",
    cache_name: Optional[Union[Path, str]] = None,
    **kwargs: Dict[str, Any],
) -> None:
    """Delete cached response associated with ``url``, along with its history (if applicable).

    Parameters
    ----------
    url : str
        URL to be deleted from the cache
    request_method : str, optional
        HTTP request method to be deleted from the cache, defaults to ``GET``.
    cache_name : str, optional
        Path to a file for caching the session, defaults to
        ``./cache/aiohttp_cache.sqlite``.
    kwargs : dict, optional
        Keywords to pass to the ``cache.delete_url()``.
    """
    loop, new_loop = utils.get_event_loop()

    request_method = request_method.upper()
    valid_methods = ["GET", "POST"]
    if request_method not in valid_methods:
        raise InvalidInputValue("method", valid_methods)

    loop.run_until_complete(
        utils.delete_url(url, request_method, utils.create_cachefile(cache_name), **kwargs)
    )
    if new_loop:
        loop.close()


def retrieve(
    urls: Sequence[StrOrURL],
    read: str,
    request_kwds: Optional[Sequence[Dict[str, Any]]] = None,
    request_method: str = "GET",
    max_workers: int = 8,
    cache_name: Optional[Union[Path, str]] = None,
    timeout: float = 5.0,
    expire_after: float = EXPIRE,
    ssl: Union[SSLContext, bool, None] = None,
    disable: bool = False,
) -> List[Union[str, Dict[str, Any], bytes]]:
    r"""Send async requests.

    Parameters
    ----------
    urls : list of str
        List of URLs.
    read : str
        Method for returning the request; ``binary``, ``json``, and ``text``.
    request_kwds : list of dict, optional
        List of requests keywords corresponding to input URLs (1 on 1 mapping),
        defaults to ``None``. For example, ``[{"params": {...}, "headers": {...}}, ...]``.
    request_method : str, optional
        Request type; ``GET`` (``get``) or ``POST`` (``post``). Defaults to ``GET``.
    max_workers : int, optional
        Maximum number of async processes, defaults to 8.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : float, optional
        Timeout for the request, defaults to 5.0.
    expire_after : int, optional
        Expiration time for response caching in seconds, defaults to -1 (never expire).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to False to disable
        SSL certification verification.
    disable : bool, optional
        If ``True`` temporarily disable caching requests and get new responses
        from the server, defaults to False.

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
    inp = BaseRetriever(urls, read, request_kwds, request_method, cache_name)

    loop, new_loop = utils.get_event_loop()

    session = tlz.partial(
        async_session,
        read=inp.read,
        r_kwds=inp.r_kwds,
        request_method=inp.request_method,
        cache_name=inp.cache_name,
        timeout=timeout,
        expire_after=expire_after,
        ssl=ssl,
        disable=disable,
    )

    chunked_reqs = tlz.partition_all(max_workers, inp.url_kwds)

    results = (loop.run_until_complete(session(url_kwds=c)) for c in chunked_reqs)

    resp = [r for _, r in sorted(tlz.concat(results))]
    if new_loop:
        loop.close()
    return resp


def retrieve_text(
    urls: Sequence[StrOrURL],
    request_kwds: Optional[Sequence[Dict[str, Any]]] = None,
    request_method: str = "GET",
    max_workers: int = 8,
    cache_name: Optional[Union[Path, str]] = None,
    timeout: float = 5.0,
    expire_after: float = EXPIRE,
    ssl: Union[SSLContext, bool, None] = None,
    disable: bool = False,
) -> List[str]:
    r"""Send async requests and get the response as ``text``.

    Parameters
    ----------
    urls : list of str
        List of URLs.
    request_kwds : list of dict, optional
        List of requests keywords corresponding to input URLs (1 on 1 mapping),
        defaults to ``None``. For example, ``[{"params": {...}, "headers": {...}}, ...]``.
    request_method : str, optional
        Request type; ``GET`` (``get``) or ``POST`` (``post``). Defaults to ``GET``.
    max_workers : int, optional
        Maximum number of async processes, defaults to 8.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : float, optional
        Timeout for the request in seconds, defaults to 5.0.
    expire_after : int, optional
        Expiration time for response caching in seconds, defaults to -1 (never expire).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to False to disable
        SSL certification verification.
    disable : bool, optional
        If ``True`` temporarily disable caching requests and get new responses
        from the server, defaults to False.

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
    >>> resp = ar.retrieve_text(urls, kwds)
    >>> resp[0].split('\n')[-2].split('\t')[1]
    '01646500'
    """
    resp: List[str] = retrieve(  # type: ignore
        urls,
        "text",
        request_kwds,
        request_method,
        max_workers,
        cache_name,
        timeout,
        expire_after,
        ssl,
        disable,
    )
    return resp


def retrieve_json(
    urls: Sequence[StrOrURL],
    request_kwds: Optional[Sequence[Dict[str, Any]]] = None,
    request_method: str = "GET",
    max_workers: int = 8,
    cache_name: Optional[Union[Path, str]] = None,
    timeout: float = 5.0,
    expire_after: float = EXPIRE,
    ssl: Union[SSLContext, bool, None] = None,
    disable: bool = False,
) -> List[Dict[str, Any]]:
    r"""Send async requests and get the response as ``json``.

    Parameters
    ----------
    urls : list of str
        List of URLs.
    request_kwds : list of dict, optional
        List of requests keywords corresponding to input URLs (1 on 1 mapping),
        defaults to ``None``. For example, ``[{"params": {...}, "headers": {...}}, ...]``.
    request_method : str, optional
        Request type; ``GET`` (``get``) or ``POST`` (``post``). Defaults to ``GET``.
    max_workers : int, optional
        Maximum number of async processes, defaults to 8.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : float, optional
        Timeout for the request, defaults to 5.0.
    expire_after : int, optional
        Expiration time for response caching in seconds, defaults to -1 (never expire).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to False to disable
        SSL certification verification.
    disable : bool, optional
        If ``True`` temporarily disable caching requests and get new responses
        from the server, defaults to False.

    Returns
    -------
    dict
        List of responses in the order of input URLs.

    Examples
    --------
    >>> import async_retriever as ar
    >>> urls = ["https://labs.waterdata.usgs.gov/api/nldi/linked-data/comid/position"]
    >>> kwds = [
    ...     {
    ...         "params": {
    ...             "f": "json",
    ...             "coords": "POINT(-68.325 45.0369)",
    ...         },
    ...     },
    ... ]
    >>> r = ar.retrieve_json(urls, kwds)
    >>> print(r[0]["features"][0]["properties"]["identifier"])
    2675320
    """
    resp: List[Dict[str, Any]] = retrieve(  # type: ignore
        urls,
        "json",
        request_kwds,
        request_method,
        max_workers,
        cache_name,
        timeout,
        expire_after,
        ssl,
        disable,
    )
    return resp


def retrieve_binary(
    urls: Sequence[StrOrURL],
    request_kwds: Optional[Sequence[Dict[str, Any]]] = None,
    request_method: str = "GET",
    max_workers: int = 8,
    cache_name: Optional[Union[Path, str]] = None,
    timeout: float = 5.0,
    expire_after: float = EXPIRE,
    ssl: Union[SSLContext, bool, None] = None,
    disable: bool = False,
) -> List[bytes]:
    r"""Send async requests and get the response as ``bytes``.

    Parameters
    ----------
    urls : list of str
        List of URLs.
    request_kwds : list of dict, optional
        List of requests keywords corresponding to input URLs (1 on 1 mapping),
        defaults to ``None``. For example, ``[{"params": {...}, "headers": {...}}, ...]``.
    request_method : str, optional
        Request type; ``GET`` (``get``) or ``POST`` (``post``). Defaults to ``GET``.
    max_workers : int, optional
        Maximum number of async processes, defaults to 8.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : float, optional
        Timeout for the request, defaults to 5.0.
    expire_after : int, optional
        Expiration time for response caching in seconds, defaults to -1 (never expire).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to False to disable
        SSL certification verification.
    disable : bool, optional
        If ``True`` temporarily disable caching requests and get new responses
        from the server, defaults to False.

    Returns
    -------
    bytes
        List of responses in the order of input URLs.
    """
    resp: List[bytes] = retrieve(  # type: ignore
        urls,
        "binary",
        request_kwds,
        request_method,
        max_workers,
        cache_name,
        timeout,
        expire_after,
        ssl,
        disable,
    )
    return resp
