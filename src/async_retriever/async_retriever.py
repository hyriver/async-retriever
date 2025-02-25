"""Core async functions."""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any, Literal, Union

import orjson as json
from aiohttp import ClientSession, TCPConnector
from aiohttp_client_cache import SQLiteBackend
from aiohttp_client_cache.session import CachedSession

from async_retriever import _utils as utils
from async_retriever._utils import MAX_HOSTS, TIMEOUT, BaseRetriever
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
    valid_methods = ["get", "post"]
    if request_method.lower() not in valid_methods:
        raise InputValueError("method", valid_methods)

    utils.run_in_event_loop(
        utils.delete_url(url, request_method, utils.create_cachefile(cache_name), **kwargs)
    )


async def _session_with_cache(
    url_kwds: zip[tuple[int, StrOrURL, dict[str, Any]]],
    read: Literal["read", "text", "json"],
    r_kwds: dict[str, Any],
    request_method: Literal["get", "post"],
    cache_name: Path,
    expire_after: int,
    ssl: SSLContext | bool,
    raise_status: bool,
    limit_per_host: int,
    timeout: int,
) -> (
    list[tuple[int, str | None]]
    | list[tuple[int, bytes | None]]
    | list[tuple[int, dict[str, Any] | None]]
    | list[tuple[int, list[dict[str, Any]] | None]]
):
    """Create an async session for sending requests."""
    if expire_after == EXPIRE_AFTER:
        expire_after = int(os.getenv("HYRIVER_CACHE_EXPIRE", EXPIRE_AFTER))
    cache = SQLiteBackend(
        cache_name=str(cache_name),
        expire_after=expire_after,
        allowed_methods=("GET", "POST"),
        fast_save=True,
    )
    async with CachedSession(
        json_serialize=lambda r: json.dumps(r).decode(),
        cache=cache,
        connector=TCPConnector(ssl=ssl, limit_per_host=limit_per_host),
        trust_env=True,
    ) as session:
        request_func = getattr(session, request_method.lower())
        tasks = [
            utils.retriever(
                uid, url, {"timeout": timeout, **kwds}, request_func, read, r_kwds, raise_status
            )
            for uid, url, kwds in url_kwds
        ]
        return await asyncio.gather(*tasks)  # pyright: ignore[reportReturnType]


async def _session_without_cache(
    url_kwds: zip[tuple[int, StrOrURL, dict[str, Any]]],
    read: Literal["read", "text", "json"],
    r_kwds: dict[str, Any],
    request_method: Literal["get", "post"],
    ssl: SSLContext | bool,
    raise_status: bool,
    limit_per_host: int,
    timeout: int,
) -> (
    list[tuple[int, str | None]]
    | list[tuple[int, bytes | None]]
    | list[tuple[int, dict[str, Any] | None]]
    | list[tuple[int, list[dict[str, Any]] | None]]
):
    """Create an async session for sending requests."""
    async with ClientSession(
        json_serialize=lambda r: json.dumps(r).decode(),
        trust_env=True,
        connector=TCPConnector(ssl=ssl, limit_per_host=limit_per_host),
    ) as session:
        request_func = getattr(session, request_method.lower())
        tasks = [
            utils.retriever(
                uid, url, {"timeout": timeout, **kwds}, request_func, read, r_kwds, raise_status
            )
            for uid, url, kwds in url_kwds
        ]
        return await asyncio.gather(*tasks)  # pyright: ignore[reportReturnType]


def retrieve(
    urls: Sequence[StrOrURL],
    read_method: Literal["text", "json", "binary"],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "post"] = "get",
    limit_per_host: int = MAX_HOSTS,
    cache_name: Path | str | None = None,
    timeout: int = TIMEOUT,
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
        Maximum number of simultaneous connections per host, defaults to 4.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : int, optional
        Requests timeout in seconds, defaults to 120.
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

    if disable:
        results = utils.run_in_event_loop(
            _session_without_cache(
                inp.url_kwds,
                inp.read_method,
                inp.r_kwds,
                inp.request_method,
                ssl,
                raise_status,
                limit_per_host,
                timeout,
            )
        )
    else:
        results = utils.run_in_event_loop(
            _session_with_cache(
                inp.url_kwds,
                inp.read_method,
                inp.r_kwds,
                inp.request_method,
                inp.cache_name,
                expire_after,
                ssl,
                raise_status,
                limit_per_host,
                timeout,
            )
        )
    return [r for _, r in sorted(results)]


def retrieve_text(
    urls: Sequence[StrOrURL],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "post"] = "get",
    limit_per_host: int = MAX_HOSTS,
    cache_name: Path | str | None = None,
    timeout: int = TIMEOUT,
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
        Maximum number of simultaneous connections per host, defaults to 4.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : int, optional
        Requests timeout in seconds, defaults to 120.
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
    limit_per_host: int = MAX_HOSTS,
    cache_name: Path | str | None = None,
    timeout: int = TIMEOUT,
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
        Maximum number of simultaneous connections per host, defaults to 4.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : int, optional
        Requests timeout in seconds, defaults to 120.
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
    >>> urls = ["https://api.water.usgs.gov/api/nldi/linked-data/comid/position"]
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
    limit_per_host: int = MAX_HOSTS,
    cache_name: Path | str | None = None,
    timeout: int = TIMEOUT,
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
        Maximum number of simultaneous connections per host, defaults to 4.
    cache_name : str, optional
        Path to a file for caching the session, defaults to ``./cache/aiohttp_cache.sqlite``.
    timeout : int, optional
        Requests timeout in seconds, defaults to 120.
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
