"""Tests for exceptions and requests"""
import pytest

from async_retriever import InvalidInputType, InvalidInputValue


def invalid_value():
    raise InvalidInputValue("outFormat", ["json", "geojson"])


def test_invalid_value():
    with pytest.raises(InvalidInputValue):
        invalid_value()


def invalid_type():
    raise InvalidInputType("``urls``", "iterable of str")


def test_invalid_type():
    with pytest.raises(InvalidInputType):
        invalid_type()
