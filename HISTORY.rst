=======
History
=======

0.2.4 (2021-09-10)
------------------

Internal Changes
~~~~~~~~~~~~~~~~
- Use ``usjon`` for converting responses to JSON.

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
- Handle all cache file related operations in the ``create_cachefile`` function.


0.2.1 (2021-07-31)
------------------

New Features
~~~~~~~~~~~~
- The responses now are returned in the same order as the input URLs.
- Add support for passing connection type, i.e., IPv4 only, IPv6, only
  or both via ``family`` argument. Defaults to ``both``.
- Set ``trust_env=True`` so the session can read system's ``netrc`` files.
  This can be useful for working with services such as EarthData service
  that read the user authentication info from a ``netrc`` file.

Internal Changes
~~~~~~~~~~~~~~~~
- Replace ``AsyncRequest`` class with ``_retrieve`` function to increase
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
- Pass a new loop explicitly to ``nest_asyncio`` (:issue:`1`).

Internal Changes
~~~~~~~~~~~~~~~~
- Refactor the entire code-base for more efficient handling of different request methods.
- Check validity of inputs before sending requests.
- Improve documentation.
- Improve cache handling by removing the expired responses before returning the results.
- Increase testing coverage to 100%.

0.1.0 (2021-05-01)
------------------

- Initial release.
