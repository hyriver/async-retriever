from __future__ import annotations

import hashlib
import json

import pytest
from multidict import MultiDict
from yarl import URL

from async_retriever import generate_filename
from async_retriever.exceptions import InputTypeError


def test_generate_filename_basic():
    """Test with minimal valid inputs."""
    url = "https://example.com/api"
    expected_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
    assert generate_filename(url) == expected_hash


def test_generate_filename_with_params():
    """Test with query parameters."""
    url = "https://example.com/api"
    params = {"key1": "value1", "key2": "value2"}
    url_obj = URL(url).with_query(MultiDict(params))
    hash_input = f"{url_obj.human_repr()}{url_obj.query}"
    expected_hash = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
    assert generate_filename(url, params=params) == expected_hash


def test_generate_filename_with_data():
    """Test with data."""
    url = "https://example.com/api"
    data = {"field1": "value1", "field2": "value2"}
    data_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
    hash_input = f"{url}{data_str}"
    expected_hash = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
    assert generate_filename(url, data=data) == expected_hash


def test_generate_filename_with_prefix_and_suffix():
    """Test with custom prefix and suffix."""
    url = "https://example.com/api"
    prefix = "test_prefix_"
    suffix = "json"
    expected_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
    assert (
        generate_filename(url, prefix=prefix, file_extension=suffix)
        == f"{prefix}{expected_hash}.json"
    )


def test_generate_filename_strip_suffix_dot():
    """Test that leading dot in suffix is handled."""
    url = "https://example.com/api"
    suffix = ".json"
    expected_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
    assert generate_filename(url, file_extension=suffix) == f"{expected_hash}.json"


def test_generate_filename_with_invalid_params():
    """Test invalid params type raises exception."""
    url = "https://example.com/api"
    invalid_params = ["invalid", "list"]
    with pytest.raises(InputTypeError) as excinfo:
        generate_filename(url, params=invalid_params)
    assert "dict or multidict.MultiDict." in str(excinfo.value)


def test_generate_filename_with_invalid_data():
    """Test invalid data type raises exception."""
    url = "https://example.com/api"
    invalid_data = ["invalid", "list"]
    with pytest.raises(InputTypeError) as excinfo:
        generate_filename(url, data=invalid_data)
    assert "dict or str" in str(excinfo.value)


def test_generate_filename_with_empty_data_and_params():
    """Test empty data and params."""
    url = "https://example.com/api"
    expected_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
    assert generate_filename(url, params=None, data=None) == expected_hash


def test_generate_filename_with_multidict_params():
    """Test with multidict parameters."""
    url = "https://example.com/api"
    params = MultiDict({"key1": "value1", "key2": "value2"})
    url_obj = URL(url).with_query(params)
    hash_input = f"{url_obj.human_repr()}{url_obj.query}"
    expected_hash = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
    assert generate_filename(url, params=params) == expected_hash


def test_generate_filename_with_empty_prefix():
    """Test with an empty prefix."""
    url = "https://example.com/api"
    prefix = ""
    expected_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
    assert generate_filename(url, prefix=prefix) == expected_hash


def test_generate_filename_with_edge_case_inputs():
    """Test edge cases like empty strings and special characters."""
    url = ""
    params = {"": ""}
    data = {"": ""}
    prefix = ""
    suffix = "."
    url_obj = URL(url).with_query(MultiDict(params))
    data_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
    hash_input = f"{url_obj.human_repr()}{url_obj.query}{data_str}"
    expected_hash = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
    assert (
        generate_filename(url, params=params, data=data, prefix=prefix, file_extension=suffix)
        == f"{expected_hash}"
    )
