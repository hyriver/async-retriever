"""Configuration for pytest."""

import pytest


@pytest.fixture(autouse=True)
def add_standard_imports(doctest_namespace):
    """Add async_retriever namespace for doctest."""
    import async_retriever as ar

    doctest_namespace["ar"] = ar
