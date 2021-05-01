"""Tests for exceptions and requests"""
import pytest

from async_retriever import InvalidInputValue


def invalid_value():
    raise InvalidInputValue("outFormat", ["json", "geojson"])


def test_invalid_value():
    with pytest.raises(InvalidInputValue):
        invalid_value()
