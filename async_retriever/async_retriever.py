"""Core async functions."""
from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any, Literal, Sequence, Union, overload

import cytoolz.curried as tlz
import ujson as json
from aiohttp import ClientSession, TCPConnector
from aiohttp_client_cache import SQLiteBackend
from aiohttp_client_cache.session import CachedSession

import async_retriever._utils as utils
from async_retriever._utils import BaseRetriever
from async_retriever.exceptions import InputValueError

if TYPE_CHECKING:
    from pathlib import Path
    from ssl import SSLContext

    from aiohttp.typedefs import StrOrURL

    RESPONSE = Union[
        "list[str]", "list[bytes]", "list[dict[str, Any]]", "list[list[dict[str, Any]]]"
    ]

EXPIRE_AFTER = 60 * 60 * 24 * 7  # 1 week
__all__ = [
    "delete_url_cache",
    "stream_write",
    "retrieve",
    "retrieve_text",
    "retrieve_json",
    "retrieve_binary",
]


def delete_url_cache(
    url: StrOrURL,
    request_method: Literal["get", "GET", "post", "POST"] = "GET",
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

    valid_methods = ["GET", "POST"]
    if request_method.upper() not in valid_methods:
        raise InputValueError("method", valid_methods)

    loop.run_until_complete(
        utils.delete_url(url, request_method, utils.create_cachefile(cache_name), **kwargs)
    )
    if new_loop:
        loop.close()


async def stream_session(
    url_kwds: tuple[tuple[Path, StrOrURL, dict[str, Any]], ...],
    request_method: Literal["get", "GET", "post", "POST"],
    ssl: SSLContext | bool | None = None,
    chunk_size: int | None = None,
) -> None:
    """Create an async session for sending requests.

    Parameters
    ----------
    url_kwds : list of tuples
        A list of file paths and URLs or file paths, URLs, and their payloads
        to be saved as a file.
    request_method : str
        The request type; ``GET`` or ``POST``.
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to ``None``. Set to
        ``False`` to disable SSL certification verification.
    chunk_size : int, optional
        The size of the chunks in bytes to be written to the file, defaults to ``None``,
        which will iterates over data chunks and write them as received from
        the server.
    """
    async with ClientSession(
        json_serialize=json.dumps, trust_env=True, connector=TCPConnector(ssl=ssl)
    ) as session:
        request_func = getattr(session, request_method.lower())
        tasks = (
            utils.stream_session(url, kwds, request_func, filepath, chunk_size)
            for filepath, url, kwds in url_kwds
        )
        await asyncio.gather(*tasks)


def stream_write(
    urls: Sequence[StrOrURL],
    file_paths: list[str | Path],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "GET", "post", "POST"] = "GET",
    max_workers: int = 8,
    ssl: SSLContext | bool | None = None,
    chunk_size: int | None = None,
) -> None:
    r"""Send async requests.

    Parameters
    ----------
    urls : list of str
        List of URLs.
    file_paths : list of str or pathlib.Path
        List of file paths to write the response to.
    request_kwds : list of dict, optional
        List of requests keywords corresponding to input URLs (1 on 1 mapping),
        defaults to ``None``. For example, ``[{"params": {...}, "headers": {...}}, ...]``.
    request_method : str, optional
        Request type; ``GET`` (``get``) or ``POST`` (``post``). Defaults to ``GET``.
    max_workers : int, optional
        Maximum number of async processes, defaults to 8.
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to False to disable
        SSL certification verification.
    chunk_size : int, optional
        The size of the chunks in bytes to be written to the file, defaults to ``None``,
        which will iterates over data chunks and write them as received from
        the server.

    Examples
    --------
    >>> import async_retriever as ar
    >>> import tempfile
    >>> url = "https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_500KB_CSV-1.csv"
    >>> with tempfile.NamedTemporaryFile() as temp:
    ...     ar.stream_write([url], [temp.name])
    """
    inp = BaseRetriever(
        urls,
        file_paths=file_paths,
        request_kwds=request_kwds,
        request_method=request_method,
    )

    loop, new_loop = utils.get_event_loop()

    session = tlz.partial(
        stream_session,
        request_method=inp.request_method,
        ssl=ssl,
        chunk_size=chunk_size,
    )

    chunked_reqs = tlz.partition_all(max_workers, inp.url_kwds)

    _ = [loop.run_until_complete(session(url_kwds=c)) for c in chunked_reqs]
    if new_loop:
        loop.close()


async def async_session_with_cache(
    url_kwds: tuple[tuple[int, StrOrURL, dict[str, Any]], ...],
    read: Literal["text", "json", "binary"],
    r_kwds: dict[str, Any],
    request_method: Literal["get", "GET", "post", "POST"],
    cache_name: Path,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool | None = None,
    raise_status: bool = True,
) -> RESPONSE:
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
        connector=TCPConnector(ssl=ssl),
        trust_env=True,
    ) as session:
        request_func = getattr(session, request_method.lower())
        tasks = (
            utils.retriever(uid, url, kwds, request_func, read, r_kwds, raise_status)
            for uid, url, kwds in url_kwds
        )
        return await asyncio.gather(*tasks)


async def async_session_without_cache(
    url_kwds: tuple[tuple[int, StrOrURL, dict[str, Any]], ...],
    read: Literal["text", "json", "binary"],
    r_kwds: dict[str, Any],
    request_method: Literal["get", "GET", "post", "POST"],
    ssl: SSLContext | bool | None = None,
    raise_status: bool = True,
) -> RESPONSE:
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
        The request type; ``GET`` or ``POST``.
    ssl : bool or SSLContext, optional
        SSLContext to use for the connection, defaults to None. Set to ``False`` to disable
        SSL certification verification.
    raise_status : bool, optional
        Raise an exception if the response status is not 200. If
        ``False`` return ``None``. Defaults to ``True``.

    Returns
    -------
    asyncio.gather
        An async gather function
    """
    async with ClientSession(
        json_serialize=json.dumps, trust_env=True, connector=TCPConnector(ssl=ssl)
    ) as session:
        request_func = getattr(session, request_method.lower())
        tasks = (
            utils.retriever(uid, url, kwds, request_func, read, r_kwds, raise_status)
            for uid, url, kwds in url_kwds
        )
        return await asyncio.gather(*tasks)


@overload
def retrieve(
    urls: Sequence[StrOrURL],
    read_method: Literal["text"],
    request_kwds: Sequence[dict[str, Any]] | None = ...,
    request_method: Literal["get", "GET", "post", "POST"] = ...,
    max_workers: int = ...,
    cache_name: Path | str | None = ...,
    timeout: int = ...,
    expire_after: int = ...,
    ssl: SSLContext | bool | None = ...,
    disable: bool = ...,
    raise_status: bool = ...,
) -> list[str]:
    ...


@overload
def retrieve(
    urls: Sequence[StrOrURL],
    read_method: Literal["json"],
    request_kwds: Sequence[dict[str, Any]] | None = ...,
    request_method: Literal["get", "GET", "post", "POST"] = ...,
    max_workers: int = ...,
    cache_name: Path | str | None = ...,
    timeout: int = ...,
    expire_after: int = ...,
    ssl: SSLContext | bool | None = ...,
    disable: bool = ...,
    raise_status: bool = ...,
) -> list[dict[str, Any]] | list[list[dict[str, Any]]]:
    ...


@overload
def retrieve(
    urls: Sequence[StrOrURL],
    read_method: Literal["binary"],
    request_kwds: Sequence[dict[str, Any]] | None = ...,
    request_method: Literal["get", "GET", "post", "POST"] = ...,
    max_workers: int = ...,
    cache_name: Path | str | None = ...,
    timeout: int = ...,
    expire_after: int = ...,
    ssl: SSLContext | bool | None = ...,
    disable: bool = ...,
    raise_status: bool = ...,
) -> list[bytes]:
    ...


def retrieve(
    urls: Sequence[StrOrURL],
    read_method: Literal["text", "json", "binary"],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "GET", "post", "POST"] = "GET",
    max_workers: int = 8,
    cache_name: Path | str | None = None,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool | None = None,
    disable: bool = False,
    raise_status: bool = True,
) -> RESPONSE:
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
    max_workers : int, optional
        Maximum number of async processes, defaults to 8.
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
    >>> resp[0].split('\n')[-2].split('\t')[1]
    '01646500'
    """
    inp = BaseRetriever(
        urls,
        read_method=read_method,
        request_kwds=request_kwds,
        request_method=request_method,
        cache_name=cache_name,
    )

    if not disable:
        disable = os.getenv("HYRIVER_CACHE_DISABLE", "false").lower() == "true"

    if disable:
        session = tlz.partial(
            async_session_without_cache,
            read=inp.read_method,
            r_kwds=inp.r_kwds,
            request_method=inp.request_method,
            ssl=ssl,
            raise_status=raise_status,
        )
    else:
        session = tlz.partial(
            async_session_with_cache,
            read=inp.read_method,
            r_kwds=inp.r_kwds,
            request_method=inp.request_method,
            cache_name=inp.cache_name,
            timeout=timeout,
            expire_after=expire_after,
            ssl=ssl,
            raise_status=raise_status,
        )

    chunked_reqs = tlz.partition_all(max_workers, inp.url_kwds)
    loop, new_loop = utils.get_event_loop()
    results = (loop.run_until_complete(session(url_kwds=c)) for c in chunked_reqs)

    resp = [r for _, r in sorted(tlz.concat(results))]
    if new_loop:
        loop.close()
    return resp


def retrieve_text(
    urls: Sequence[StrOrURL],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "GET", "post", "POST"] = "GET",
    max_workers: int = 8,
    cache_name: Path | str | None = None,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool | None = None,
    disable: bool = False,
    raise_status: bool = True,
) -> list[str]:
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
    >>> resp[0].split('\n')[-2].split('\t')[1]
    '01646500'
    """
    return retrieve(
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
        raise_status,
    )


def retrieve_json(
    urls: Sequence[StrOrURL],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "GET", "post", "POST"] = "GET",
    max_workers: int = 8,
    cache_name: Path | str | None = None,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool | None = None,
    disable: bool = False,
    raise_status: bool = True,
) -> list[dict[str, Any]] | list[list[dict[str, Any]]]:
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
    return retrieve(
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
        raise_status,
    )


def retrieve_binary(
    urls: Sequence[StrOrURL],
    request_kwds: Sequence[dict[str, Any]] | None = None,
    request_method: Literal["get", "GET", "post", "POST"] = "GET",
    max_workers: int = 8,
    cache_name: Path | str | None = None,
    timeout: int = 5,
    expire_after: int = EXPIRE_AFTER,
    ssl: SSLContext | bool | None = None,
    disable: bool = False,
    raise_status: bool = True,
) -> list[bytes]:
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
    return retrieve(
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
        raise_status,
    )
