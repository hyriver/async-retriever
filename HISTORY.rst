=======
History
=======

0.15.2 (2023-09-22)
-------------------

Bug Fixes
~~~~~~~~~
- Fix an issue with getting all valid keywords that ``aiohttp`` accepts
  by using ``aiohttp.ClientSession()._request`` directly.

0.15.0 (2023-05-07)
-------------------
From release 0.15 onward, all minor versions of HyRiver packages
will be pinned. This ensures that previous minor versions of HyRiver
packages cannot be installed with later minor releases. For example,
if you have ``py3dep==0.14.x`` installed, you cannot install
``pydaymet==0.15.x``. This is to ensure that the API is
consistent across all minor versions.

Bug Fixes
~~~~~~~~~
- When ``raise_status`` is ``False``, responses for failed requests used to
  return as ``None`` but their requests ID was not returned, so sorting
  would have failed. Now request IDs are returned for all requests regardless
  of whether they were successful or not.
- Give precedence to non-default arguments for caching related arguments
  instead of directly getting them from env variables. This is to avoid
  the case where the user sets the env variables but then passes different
  arguments to the function. In this case, the function should use the
  passed arguments instead of the env variables.

0.14.0 (2023-03-05)
-------------------

New Features
~~~~~~~~~~~~
- Add a new option to all functions called ``raise_status``. If ``False``
  no exception will be raised and instead ``None`` is returned for those
  requests that led to exceptions. This will allow for returning all responses
  that were successful and ignoring the ones that failed. This option defaults
  to ``True`` for retaining backward compatibility.
- Set the cache expiration time to one week from never expire. To ensure all
  users have a smooth transition, cache files that were created before the
  release of this version will be deleted, and a new cache will be created.

Internal Changes
~~~~~~~~~~~~~~~~
- Sync all minor versions of HyRiver packages to 0.14.0.

0.3.12 (2023-02-10)
-------------------

Internal Changes
~~~~~~~~~~~~~~~~
- Rewrite the private ``async_session`` function as two separate functions
  called ``async_session_without_cache`` and ``async_session_with_cache``.
  This makes the code more readable and easier to maintain.
- Fully migrate ``setup.cfg`` and ``setup.py`` to ``pyproject.toml``.
- Convert relative imports to absolute with ``absolufy-imports``.
- Make ``utils`` module private.
- Sync all patch versions of HyRiver packages to x.x.12.

0.3.10 (2023-01-08)
-------------------

New Features
~~~~~~~~~~~~
- Refactor the ``show_versions`` function to improve performance and
  print the output in a nicer table-like format.

Bug Fixes
~~~~~~~~~
- Fix a bug in reading the ``HYRIVER_CACHE_EXPIRE`` environmental variable.
- Bump the minimum version of ``aiohttp-client-cache`` to 0.8.1 to fix a bug
  in reading cache files that were created with previous versions.
  (:issue_async:`41`)

Internal Changes
~~~~~~~~~~~~~~~~
- Enable ``fast_save`` in ``aiohttp-client-cache`` to speed up saving responses
  to the cache file.
- Use ``pyright`` for type checking instead of ``mypy`` and fix all type errors.
- Skip 0.13.8/9 versions so the minor version of all HyRiver packages become
  the same.

0.3.7 (2022-12-09)
------------------

New Features
~~~~~~~~~~~~
- Add support for specifying the chunk size in ``stream_write``. Defaults to
  ``None`` which was the default behavior before, and means iterating over and
  writing the responses as they are received from the server.

Internal Changes
~~~~~~~~~~~~~~~~
- Use ``pyupgrade`` package to update the type hinting annotations
  to Python 3.10 style.
- Modify the codebase based on `Refurb <https://github.com/dosisod/refurb>`__
  suggestions.

0.3.6 (2022-08-30)
------------------

Internal Changes
~~~~~~~~~~~~~~~~
- Add the missing PyPi classifiers for the supported Python versions.
- Release the package as both ``async_retriever`` and ``async-retriever``
  on PyPi and Conda-forge.

0.3.5 (2022-08-29)
------------------

Breaking Changes
~~~~~~~~~~~~~~~~
- Append "Error" to all exception classes for conforming to PEP-8 naming conventions.

Internal Changes
~~~~~~~~~~~~~~~~
- Bump minimum version of ``aiohttp-client-cache`` to 0.7.3 since the ``attrs`` version
  issue has been addressed.


0.3.4 (2022-07-31)
------------------

New Features
~~~~~~~~~~~~
- Add a new function, ``stream_write``, for writing a response to a file as it's being
  retrieved. This could be very useful for downloading large files. This function does
  not use persistent caching.

0.3.3 (2022-06-14)
------------------

Breaking Changes
~~~~~~~~~~~~~~~~
- Set the minimum supported version of Python to 3.8 since many of the
  dependencies such as ``xarray``, ``pandas``, ``rioxarray`` have dropped support
  for Python 3.7.

Internal Changes
~~~~~~~~~~~~~~~~
- Use `micromamba <https://github.com/marketplace/actions/provision-with-micromamba>`__
  for running tests
  and use `nox <https://github.com/marketplace/actions/setup-nox>`__
  for linting in CI.

0.3.2 (2022-04-03)
------------------

New Features
~~~~~~~~~~~~
- Add support for setting caching-related arguments using three environmental variables:

  * ``HYRIVER_CACHE_NAME``: Path to the caching SQLite database.
  * ``HYRIVER_CACHE_EXPIRE``: Expiration time for cached requests in seconds.
  * ``HYRIVER_CACHE_DISABLE``: Disable reading/writing from/to the cache file.

  You can do this like so:

.. code-block:: python

    import os

    os.environ["HYRIVER_CACHE_NAME"] = "path/to/file.sqlite"
    os.environ["HYRIVER_CACHE_EXPIRE"] = "3600"
    os.environ["HYRIVER_CACHE_DISABLE"] = "true"

Internal Changes
~~~~~~~~~~~~~~~~
- Include the URL of a failed request in its exception error message.

0.3.1 (2021-12-31)
------------------

New Features
~~~~~~~~~~~~
- Add three new functions called ``retrieve_text``, ``retrieve_json``, and
  ``retrieve_binary``. These functions are derived from the ``retrieve`` function
  and are used to retrieve the text, JSON, or binary content of a response. They
  are meant to help with type hinting since they have only one return type instead
  of the three different return types that the ``retrieve`` function has.

Internal Changes
~~~~~~~~~~~~~~~~
- Move all private functions to a new module called ``utils``. This makes the code-base
  more readable and easier to maintain.


0.3.0 (2021-12-27)
------------------

Breaking Changes
~~~~~~~~~~~~~~~~
- Set the expiration time to never expire by default.

New Features
~~~~~~~~~~~~
- Add two new arguments to ``retrieve`` for controlling caching. First, ``delete_url_cache``
  for deleting caches for specific requests. Second, ``expire_after`` for setting a
  custom expiration time.
- Expose the ``ssl`` argument for disabling the SSL certification
  verification (:issue_day:`41`).
- Add a new option called ``disable`` that temporarily disables caching
  requests/responses if set to ``True``. It defaults to ``False``.

0.2.5 (2021-11-09)
------------------

New Features
~~~~~~~~~~~~
- Add two new arguments, ``timeout`` and ``expire_after``, to ``retrieve``.
  These two arguments give the user more control in dealing with issues
  related to caching.

Internal Changes
~~~~~~~~~~~~~~~~
- Revert to ``pytest`` as the testing framework.
- Use ``importlib-metadata`` for getting the version instead of ``pkg_resources``
  to decrease import time as discussed in this
  `issue <https://github.com/pydata/xarray/issues/5676>`__.

0.2.4 (2021-09-10)
------------------

Internal Changes
~~~~~~~~~~~~~~~~
- Use ``ujon`` for converting responses to JSON.

Bug Fixes
~~~~~~~~~
- Fix an issue with catching service error messages.

0.2.3 (2021-08-26)
------------------

Internal Changes
~~~~~~~~~~~~~~~~
- Use ``ujson`` for JSON parsing instead of ``orjson`` since ``orjson`` only serializes to
  ``bytes`` which is not compatible with ``aiohttp``.

0.2.2 (2021-08-19)
------------------

New Features
~~~~~~~~~~~~
- Add a new function, ``clean_cache``, for manually removing the expired responses
  from the cache database.

Internal Changes
~~~~~~~~~~~~~~~~
- Handle all cache file-related operations in the ``create_cachefile`` function.


0.2.1 (2021-07-31)
------------------

New Features
~~~~~~~~~~~~
- The responses now are returned to the same order as the input URLs.
- Add support for passing connection type, i.e., IPv4 only, IPv6 only,
  or both via the ``family`` argument. Defaults to ``both``.
- Set ``trust_env=True``, so the session can read the system's ``netrc`` files.
  This can be useful for working with services such as EarthData service
  that read the user authentication info from a ``netrc`` file.

Internal Changes
~~~~~~~~~~~~~~~~
- Replace the ``AsyncRequest`` class with the ``_retrieve`` function to increase
  readability and reduce overhead.
- More robust handling of validating user inputs via a new class called ``ValidateInputs``.
- Move all if-blocks in ``async_session`` to other functions to improve performance.

0.2.0 (2021-06-17)
------------------

Breaking Changes
~~~~~~~~~~~~~~~~
- Make persistent caching dependencies required.
- Rename ``request`` argument to ``request_method`` in ``retrieve`` which now accepts both
  lower and upper cases of ``get`` and ``post``.

Bug Fixes
~~~~~~~~~
- Pass a new loop explicitly to ``nest_asyncio`` (:issue_async:`1`).

Internal Changes
~~~~~~~~~~~~~~~~
- Refactor the entire code-base for more efficient handling of different request methods.
- Check the validity of inputs before sending requests.
- Improve documentation.
- Improve cache handling by removing the expired responses before returning the results.
- Increase testing coverage to 100%.

0.1.0 (2021-05-01)
------------------

- Initial release.
