"""Download multiple files concurrently by streaming their content to disk."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiofiles
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from multidict import MultiDict
from yarl import URL

from async_retriever import _utils as utils
from async_retriever.exceptions import InputTypeError, ServiceError

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["generate_filename", "stream_write"]
CHUNK_SIZE = 1024 * 1024  # Default chunk size of 1 MB
MAX_HOSTS = 4  # Maximum connections to a single host (rate-limited service)
TIMEOUT = 10 * 60  # Timeout for requests in seconds


async def _stream_file(session: ClientSession, url: str, filepath: Path, chunk_size: int) -> None:
    """Stream the response to a file, skipping if already downloaded."""
    async with session.get(url) as response:
        if response.status != 200:
            raise ServiceError(await response.text(), str(response.url))
        remote_size = int(response.headers.get("Content-Length", -1))
        if filepath.exists() and filepath.stat().st_size == remote_size:
            return

        async with aiofiles.open(filepath, "wb") as file:
            async for chunk in response.content.iter_chunked(chunk_size):
                await file.write(chunk)


async def _stream_session(urls: Sequence[str], files: Sequence[Path], chunk_size: int) -> None:
    """Download files concurrently."""
    async with ClientSession(
        connector=TCPConnector(limit_per_host=MAX_HOSTS),
        timeout=ClientTimeout(TIMEOUT),
    ) as session:
        tasks = [
            asyncio.create_task(_stream_file(session, url, filepath, chunk_size))
            for url, filepath in zip(urls, files)
        ]
        await asyncio.gather(*tasks)


def stream_write(
    urls: Sequence[str], file_paths: Sequence[Path], chunk_size: int = CHUNK_SIZE
) -> None:
    """Download multiple files concurrently by streaming their content to disk.

    Parameters
    ----------
    urls : list of str
        URLs to download.
    file_paths : list of pathlib.Path
        Paths to save the downloaded files.
    chunk_size : int, optional
        Size of the chunks to download, by default 1 MB.
    """
    file_paths = [Path(filepath) for filepath in file_paths]
    if len(urls) != len(file_paths):
        raise InputTypeError("urls/files_paths", "lists of the same size")

    for parent_dir in {f.parent for f in file_paths}:
        parent_dir.mkdir(parents=True, exist_ok=True)

    utils.run_in_event_loop(_stream_session(urls, file_paths, chunk_size))


def generate_filename(
    url: str,
    params: dict[str, Any] | MultiDict[str, Any] | None = None,
    data: dict[str, Any] | str | None = None,
    prefix: str | None = None,
    file_extension: str = "",
) -> str:
    """Generate a unique filename using SHA-256 from a query.

    Parameters
    ----------
    url : str
        The URL for the request.
    params : dict, multidict.MultiDict, optional
        Query parameters for the request, default is ``None``.
    data : dict, str, optional
        Data or JSON to include in the hash, default is ``None``.
    prefix : str, optional
        A custom prefix to attach to the filename, default is ``None``.
    file_extension : str, optional
        The file extension to append to the filename, default is ``""``.

    Returns
    -------
    str
        A unique filename with the SHA-256 hash, optional prefix, and
        the file extension.
    """
    url_obj = URL(url)

    if params is not None and not isinstance(params, (dict, MultiDict)):
        raise InputTypeError("params", "dict or multidict.MultiDict.")

    if data is not None and not isinstance(data, (dict, str)):
        raise InputTypeError("data", "dict or str.")

    if params:
        params_obj = MultiDict(params)
        url_obj = url_obj.with_query(params_obj)
    params_str = str(url_obj.query or "")

    if isinstance(data, dict):
        data_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
    else:
        data_str = str(data or "")

    prefix_part = prefix or ""
    hash_input = f"{url_obj.human_repr()}{params_str}{data_str}"
    hash_digest = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
    file_extension = file_extension.lstrip(".")
    file_extension = f".{file_extension}" if file_extension else ""
    return f"{prefix_part}{hash_digest}{file_extension}"
