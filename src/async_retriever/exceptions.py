"""Customized exceptions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence


class DownloadError(Exception):
    """Exception raised when the requested data is not available on the server.

    Parameters
    ----------
    err : str
        Service error message.
    """

    def __init__(self, err: str, url: str) -> None:
        self.message = f"Service returned the following error message:\nURL: {url}\nERROR: {err}\n"
        super().__init__(self.message)

    def __str__(self) -> str:
        """Return the error message."""
        return self.message


class ServiceError(Exception):
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


class InputValueError(Exception):
    """Exception raised for invalid input.

    Parameters
    ----------
    inp : str
        Name of the input parameter
    valid_inputs : tuple
        List of valid inputs
    given : str, optional
        The given input, defaults to None.
    """

    def __init__(
        self,
        inp: str,
        valid_inputs: Sequence[str | int] | Generator[str | int, None, None],
        given: str | int | None = None,
    ) -> None:
        if given is None:
            self.message = f"Given {inp} is invalid. Valid options are:\n"
        else:
            self.message = f"Given {inp} ({given}) is invalid. Valid options are:\n"
        self.message += "\n".join(str(i) for i in valid_inputs)
        super().__init__(self.message)

    def __str__(self) -> str:
        """Return the error message."""
        return self.message


class InputTypeError(Exception):
    """Exception raised when a function argument type is invalid.

    Parameters
    ----------
    arg : str
        Name of the function argument
    valid_type : str
        The valid type of the argument
    example : str, optional
        An example of a valid form of the argument, defaults to None.
    """

    def __init__(
        self,
        arg: str,
        valid_type: str,
        example: str | None = None,
    ) -> None:
        self.message = f"The {arg} argument should be of type {valid_type}"
        if example is not None:
            self.message += f":\n{example}"
        super().__init__(self.message)

    def __str__(self) -> str:
        """Return the error message."""
        return self.message
