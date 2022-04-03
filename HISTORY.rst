=======
History
=======

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
