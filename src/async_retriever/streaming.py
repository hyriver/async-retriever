"""Download multiple files concurrently by streaming their content to disk."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from aiohttp import ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientResponseError
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Sequence, Iterable
    from ssl import SSLContext

    from aiohttp.typedefs import StrOrURL

__all__ = ["stream_write", "DownloadError"]
CHUNK_SIZE = 1024 * 1024  # Default chunk size of 1 MB

if sys.platform == "win32":  # pragma: no cover
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class DownloadError(Exception):
    """Exception raised when the requested data is not available on the server.

    Parameters
    ----------
    err : str
        Service error message.
    """

    def __init__(self, err: str, url: str | None = None) -> None:
        self.message = "Service returned the following error message:\n"
        if url is None:
            self.message += err
        else:
            self.message += f"URL: {url}\nERROR: {err}\n"
        super().__init__(self.message)

    def __str__(self) -> str:
        """Return the error message."""
        return self.message


async def _stream_file(
    session: ClientSession,
    request_method: Literal["get", "post"],
    url: StrOrURL,
    filepath: Path,
    chunk_size: int,
) -> None:
    """Stream the response to a file."""
    async with session.request(request_method, url) as response:
        try:
            response.raise_for_status()
        except (ClientResponseError, ValueError) as ex:
            raise DownloadError(await response.text(), str(response.url)) from ex
            
        with filepath.open("wb") as file:
            async for chunk in response.content.iter_chunked(chunk_size):
                file.write(chunk)


async def _stream_session(
    url_file_mappings: Iterable[tuple[str, Path]],
    request_method: Literal["get", "post"],
    ssl: bool | SSLContext,
    chunk_size: int,
    limit_per_host: int,
) -> None:
    """Create an async session to download files."""
    if isinstance(ssl, bool):
        verify_ssl = ssl
        ssl_context = None
    elif isinstance(ssl, SSLContext):
        verify_ssl = True
        ssl_context = ssl
    else:
        raise TypeError("`ssl` must be a boolean or SSLContext object.")

    async with ClientSession(
        connector=TCPConnector(verify_ssl=verify_ssl, ssl_context=ssl_context, limit_per_host=limit_per_host)
    ) as session:
        tasks = [
            _stream_file(session, request_method, url, filepath, chunk_size)
            for url, filepath in url_file_mappings
        ]
        await asyncio.gather(*tasks)


def stream_write(
    urls: Sequence[StrOrURL],
    file_paths: Sequence[Path] | Sequence[str],
    request_method: Literal["get", "post"] = "get",
    ssl: bool | SSLContext = True,
    chunk_size: int = CHUNK_SIZE,
    limit_per_host: int = 5,
) -> None:
    """Download multiple files concurrently by streaming their content to disk.

    Parameters
    ----------
    urls : Sequence[str]
        List of URLs to download.
    file_paths : Sequence[Path]
        List of file paths to save the downloaded content.
    request_method : {"get", "post"}, optional
        HTTP method to use (i.e., ``get`` or ``post``), by default ``get``.
    ssl : bool or ssl.SSLContext, optional
        Whether to verify SSL certificates, by default True. Also,
        an SSLContext object can be passed to customize
    chunk_size : int, optional
        Size of each chunk in bytes, by default 1 MB.
    limit_per_host : int, optional
        Maximum simultaneous connections per host, by default 5.

    Examples
    --------
    >>> import tempfile
    >>> url = "https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_500KB_CSV-1.csv"
    >>> with tempfile.NamedTemporaryFile(dir=".") as temp:
    ...     stream_write([url], [temp.name])
    """
    if len(urls) != len(file_paths):
        raise TypeError("`urls` and `file_paths` must be sequences of same length.")

    file_paths = [Path(filepath) for filepath in file_paths]
    parent_dirs = {filepath.parent for filepath in file_paths}
    for parent_dir in parent_dirs:
        parent_dir.mkdir(parents=True, exist_ok=True)

    loop, is_new_loop = _get_event_loop()

    try:
        loop.run_until_complete(
            _stream_session(
                zip(urls, file_paths),
                request_method,
                ssl,
                chunk_size,
                limit_per_host,
            )
        )
    finally:
        if is_new_loop:
            loop.close()

def _get_event_loop() -> tuple[asyncio.AbstractEventLoop, bool]:
    """Get or create an event loop."""
    try:
        return asyncio.get_running_loop(), False
    except RuntimeError:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        return new_loop, True
