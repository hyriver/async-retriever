"""Core async functions."""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any, Literal, Union

import ujson as json
from aiohttp import ClientSession, TCPConnector
from aiohttp_client_cache import SQLiteBackend
from aiohttp_client_cache.session import CachedSession

from async_retriever import _utils as utils
from async_retriever._utils import BaseRetriever
from async_retriever.exceptions import InputValueError

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path
    from ssl import SSLContext

    from aiohttp.typedefs import StrOrURL

    Response = Union[
        "list[str]",
        "list[bytes]",
        "list[dict[str, Any]]",
        "list[list[dict[str, Any]]]",
    ]
EXPIRE_AFTER = 60 * 60 * 24 * 7  # 1 week
__all__ = [
    "delete_url_cache",
    "retrieve",
    "retrieve_binary",
    "retrieve_json",
    "retrieve_text",
]


def delete_url_cache(
    url: StrOrURL,
    request_method: Literal["get", "post"] = "get",
    cache_name: Path | str | None = None,
    **kwargs: dict[str, Any],
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

    valid_methods = ["get", "post"]
    if request_method.lower() not in valid_methods:
        raise InputValueError("method", valid_methods)

    loop.run_until_complete(
        utils.delete_url(url, request_method, utils.create_cachefile(cache_name), **kwargs)
    )
    if new_loop:
        loop.close()


async def _session_with_cache(
    url_kwds: zip[tuple[int, StrOrURL, dict[str, Any]]],
    read: Literal["read", "text", "json"],
    r_kwds: dict[str, Any],
    request_method: Literal["get", "post"],
    cache_name: Path,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool = True,
    raise_status: bool = True,
    limit_per_host: int = 5,
) -> (
    list[tuple[int, str | None]]
    | list[tuple[int, bytes | None]]
    | list[tuple[int, dict[str, Any] | None]]
    | list[tuple[int, list[dict[str, Any]] | None]]
):
    """Create an async session for sending requests.

    Parameters
    ----------
    url_kwds : list of tuples of urls and payloads
        A list of URLs or URLs with their payloads to be retrieved.
    read : str
        The method for returning the request; ``read`` (bytes), ``json``, and ``text``.
    r_kwds : dict
        Keywords to pass to the response read function. ``{"content_type": None}`` if read
        is ``json`` else it's empty.
    request_method : str
        The request type; ``GET`` or ``POST``.
    cache_name : str
        Path to a file for caching the session, defaults to
        ``./cache/aiohttp_cache.sqlite``.
    timeout : int, optional
        Requests timeout in seconds, defaults to 5.
    expire_after : int, optional
        Expiration time for the cache in seconds, defaults to 2592000 (one week).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to ``False`` to disable
        SSL certification verification.
    raise_status : bool, optional
        Raise an exception if the response status is not 200. If
        ``False`` return ``None``. Defaults to ``True``.
    limit_per_host : int, optional
        Maximum number of simultaneous connections per host, defaults to 5.

    Returns
    -------
    asyncio.gather
        An async gather function
    """
    if expire_after == EXPIRE_AFTER:
        expire_after = int(os.getenv("HYRIVER_CACHE_EXPIRE", EXPIRE_AFTER))
    cache = SQLiteBackend(
        cache_name=str(cache_name),
        expire_after=expire_after,
        allowed_methods=("GET", "POST"),
        timeout=timeout,
        fast_save=True,
    )
    async with CachedSession(
        json_serialize=json.dumps,
        cache=cache,
        connector=TCPConnector(ssl=ssl, limit_per_host=limit_per_host),
        trust_env=True,
    ) as session:
        request_func = getattr(session, request_method.lower())
        tasks = [
            utils.retriever(uid, url, kwds, request_func, read, r_kwds, raise_status)
            for uid, url, kwds in url_kwds
        ]
        return await asyncio.gather(*tasks)  # pyright: ignore[reportReturnType]


async def _session_without_cache(
    url_kwds: zip[tuple[int, StrOrURL, dict[str, Any]]],
    read: Literal["read", "text", "json"],
    r_kwds: dict[str, Any],
    request_method: Literal["get", "post"],
    ssl: SSLContext | bool = True,
    raise_status: bool = True,
    limit_per_host: int = 5,
) -> (
    list[tuple[int, str | None]]
    | list[tuple[int, bytes | None]]
    | list[tuple[int, dict[str, Any] | None]]
    | list[tuple[int, list[dict[str, Any]] | None]]
):
    """Create an async session for sending requests.

    Parameters
    ----------
    url_kwds : list of tuples of urls and payloads
        A list of URLs or URLs with their payloads to be retrieved.
    read : str
        The method for returning the request; ``read`` (bytes), ``json``, and ``text``.
    r_kwds : dict
        Keywords to pass to the response read function. ``{"content_type": None}`` if read
        is ``json`` else it's empty.
    request_method : str
        The request type; ``GET`` or ``POST``.
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to ``False`` to disable
        SSL certification verification.
    raise_status : bool, optional
        Raise an exception if the response status is not 200. If
        ``False`` return ``None``. Defaults to ``True``.
    limit_per_host : int, optional
        Maximum number of simultaneous connections per host, defaults to 5.

    Returns
    -------
    asyncio.gather
        An async gather function
    """
    async with ClientSession(
        json_serialize=json.dumps,
        trust_env=True,
        connector=TCPConnector(ssl=ssl, limit_per_host=limit_per_host),
    ) as session:
        request_func = getattr(session, request_method.lower())
        tasks = [
            utils.retriever(uid, url, kwds, request_func, read, r_kwds, raise_status)
            for uid, url, kwds in url_kwds
        ]
        return await asyncio.gather(*tasks)  # pyright: ignore[reportReturnType]


def retrieve(
    urls: Sequence[StrOrURL],
    read_method: Literal["text", "json", "binary"],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "post"] = "get",
    limit_per_host: int = 5,
    cache_name: Path | str | None = None,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool = True,
    disable: bool = False,
    raise_status: bool = True,
) -> list[str | bytes | dict[str, Any] | list[dict[str, Any]] | None]:
    r"""Send async requests.

    Parameters
    ----------
    urls : list of str
        List of URLs.
    read_method : str
        Method for returning the request; ``binary``, ``json``, and ``text``.
    request_kwds : list of dict, optional
        List of requests keywords corresponding to input URLs (1 on 1 mapping),
        defaults to ``None``. For example, ``[{"params": {...}, "headers": {...}}, ...]``.
    request_method : str, optional
        Request type; ``GET`` (``get``) or ``POST`` (``post``). Defaults to ``GET``.
    limit_per_host : int, optional
        Maximum number of simultaneous connections per host, defaults to 5.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : int, optional
        Requests timeout in seconds, defaults to 5.
    expire_after : int, optional
        Expiration time for response caching in seconds, defaults to 2592000 (one week).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to False to disable
        SSL certification verification.
    disable : bool, optional
        If ``True`` temporarily disable caching requests and get new responses
        from the server, defaults to False.
    raise_status : bool, optional
        Raise an exception if the response status is not 200. If
        ``False`` return ``None``. Defaults to ``True``.

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
    >>> resp[0].split("\n")[-2].split("\t")[1]
    '01646500'
    """
    inp = BaseRetriever(
        urls,
        read_method=read_method,
        request_kwds=request_kwds,
        request_method=request_method,
        cache_name=cache_name,
        ssl=ssl,
    )

    if not disable:
        disable = os.getenv("HYRIVER_CACHE_DISABLE", "false").lower() == "true"

    loop, new_loop = utils.get_event_loop()
    if disable:
        results = loop.run_until_complete(
            _session_without_cache(
                inp.url_kwds,
                inp.read_method,
                inp.r_kwds,
                inp.request_method,
                ssl,
                raise_status,
                limit_per_host,
            )
        )
    else:
        results = loop.run_until_complete(
            _session_with_cache(
                inp.url_kwds,
                inp.read_method,
                inp.r_kwds,
                inp.request_method,
                inp.cache_name,
                timeout,
                expire_after,
                ssl,
                raise_status,
                limit_per_host,
            )
        )
    if new_loop:
        loop.close()
    return [r for _, r in sorted(results)]


def retrieve_text(
    urls: Sequence[StrOrURL],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "post"] = "get",
    limit_per_host: int = 5,
    cache_name: Path | str | None = None,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool = True,
    disable: bool = False,
    raise_status: bool = True,
) -> list[str | None]:
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
    limit_per_host : int, optional
        Maximum number of simultaneous connections per host, defaults to 5.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : int, optional
        Requests timeout in seconds in seconds, defaults to 5.
    expire_after : int, optional
        Expiration time for response caching in seconds, defaults to 2592000 (one week).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to False to disable
        SSL certification verification.
    disable : bool, optional
        If ``True`` temporarily disable caching requests and get new responses
        from the server, defaults to False.
    raise_status : bool, optional
        Raise an exception if the response status is not 200. If
        ``False`` return ``None``. Defaults to ``True``.

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
    >>> resp[0].split("\n")[-2].split("\t")[1]
    '01646500'
    """
    return retrieve(  # pyright: ignore[reportReturnType]
        urls,
        "text",
        request_kwds,
        request_method,
        limit_per_host,
        cache_name,
        timeout,
        expire_after,
        ssl,
        disable,
        raise_status,
    )


def retrieve_json(
    urls: Sequence[StrOrURL],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "post"] = "get",
    limit_per_host: int = 5,
    cache_name: Path | str | None = None,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool = True,
    disable: bool = False,
    raise_status: bool = True,
) -> list[dict[str, Any] | None] | list[list[dict[str, Any]] | None]:
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
    limit_per_host : int, optional
        Maximum number of simultaneous connections per host, defaults to 5.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : int, optional
        Requests timeout in seconds, defaults to 5.
    expire_after : int, optional
        Expiration time for response caching in seconds, defaults to 2592000 (one week).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to False to disable
        SSL certification verification.
    disable : bool, optional
        If ``True`` temporarily disable caching requests and get new responses
        from the server, defaults to False.
    raise_status : bool, optional
        Raise an exception if the response status is not 200. If
        ``False`` return ``None``. Defaults to ``True``.

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
    return retrieve(  # pyright: ignore[reportReturnType]
        urls,
        "json",
        request_kwds,
        request_method,
        limit_per_host,
        cache_name,
        timeout,
        expire_after,
        ssl,
        disable,
        raise_status,
    )


def retrieve_binary(
    urls: Sequence[StrOrURL],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "post"] = "get",
    limit_per_host: int = 5,
    cache_name: Path | str | None = None,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool = True,
    disable: bool = False,
    raise_status: bool = True,
) -> list[bytes] | list[bytes | None]:
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
    limit_per_host : int, optional
        Maximum number of simultaneous connections per host, defaults to 5.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : int, optional
        Requests timeout in seconds, defaults to 5.
    expire_after : int, optional
        Expiration time for response caching in seconds, defaults to 2592000 (one week).
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to False to disable
        SSL certification verification.
    disable : bool, optional
        If ``True`` temporarily disable caching requests and get new responses
        from the server, defaults to False.
    raise_status : bool, optional
        Raise an exception if the response status is not 200. If
        ``False`` return ``None``. Defaults to ``True``.

    Returns
    -------
    bytes
        List of responses in the order of input URLs.
    """
    return retrieve(  # pyright: ignore[reportReturnType]
        urls,
        "binary",
        request_kwds,
        request_method,
        limit_per_host,
        cache_name,
        timeout,
        expire_after,
        ssl,
        disable,
        raise_status,
    )
