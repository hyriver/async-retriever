=======
History
=======

0.2.0 (2021-05-02)
------------------

Breaking Changes
~~~~~~~~~~~~~~~~
- Make persistent caching dependencies required.
- Rename ``request`` to ``request_method`` in ``retrieve`` which now accpts both lower and
  upper cases of ``get`` and ``post``.

Internal Changes
~~~~~~~~~~~~~~~~
- Refactor the entire code-base for more efficient handling of different request methods.
- Check validity of inputs before sending requests.
- To avoid the issues such as `this <https://github.com/cheginit/HyRiver/issues/1>`__,
  pass a loop explicitly to ``nest_asyncio``.
- Improve documentation.
- Improve cache handling by removing the expired responses before returning the results.
- Increase testing coverage to 100%.

0.1.0 (2021-05-01)
------------------

- Initial release.
