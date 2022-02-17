"""Customized exceptions."""
from typing import Generator, List, Optional, Union


class ServiceError(Exception):
    """Exception raised when the requested data is not available on the server.

    Parameters
    ----------
    err : str
        Service error message.
    """

    def __init__(self, err: str, url: Optional[str] = None) -> None:
        self.message = "Service returned the following error message:\n"
        if url is None:
            self.message += err
        else:
            self.message += f"URL: {url}\nERROR: {err}\n"
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class InvalidInputValue(Exception):
    """Exception raised for invalid input.

    Parameters
    ----------
    inp : str
        Name of the input parameter
    valid_inputs : tuple
        List of valid inputs
    """

    def __init__(
        self,
        inp: str,
        valid_inputs: Union[List[str], Generator[str, None, None]],
    ) -> None:
        self.message = f"Given {inp} is invalid. Valid options are:\n" + "\n".join(
            str(i) for i in valid_inputs
        )
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class InvalidInputType(Exception):
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
        example: Optional[str] = None,
    ) -> None:
        self.message = f"The {arg} argument should be of type {valid_type}"
        if example is not None:
            self.message += f":\n{example}"
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message
