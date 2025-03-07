"""Tests for exceptions and requests."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlencode

import pytest
from aiohttp import InvalidURL

import async_retriever as ar
from async_retriever.exceptions import InputTypeError, InputValueError, ServiceError


@pytest.fixture
def url_kwds():
    stations = ["01646500", "08072300", "11073495"]
    url = "https://waterservices.usgs.gov/nwis/site"
    return zip(
        *((url, {"params": {"format": "rdb", "sites": s, "siteStatus": "all"}}) for s in stations),
    )


def test_invalid_method(url_kwds):
    urls, kwds = url_kwds
    with pytest.raises(InputValueError) as ex:
        _ = ar.retrieve(urls, "text", request_kwds=kwds, request_method="getter")
    assert "get" in str(ex.value)


def test_delete_invalid_method(url_kwds):
    urls, _ = url_kwds
    with pytest.raises(InputValueError) as ex:
        ar.delete_url_cache(urls[0], request_method="getter")
    assert "get" in str(ex.value)


def test_invalid_read(url_kwds):
    urls, kwds = url_kwds
    with pytest.raises(InputValueError) as ex:
        _ = ar.retrieve(urls, "texts", request_kwds=kwds)
    assert "read" in str(ex.value)


def test_invalid_url(url_kwds):
    urls, kwds = url_kwds
    with pytest.raises(InputTypeError) as ex:
        _ = ar.retrieve(urls[0], "text", request_kwds=kwds)
    assert "list of str" in str(ex.value)


def test_invalid_link():
    urls = ["dead.link.com"]
    with pytest.raises(InvalidURL) as ex:
        _ = ar.retrieve(urls, "text")
    assert "dead.link.com" in str(ex.value)


def test_invalid_length(url_kwds):
    urls, kwds = url_kwds
    with pytest.raises(InputTypeError) as ex:
        _ = ar.retrieve(urls * 2, "text", request_kwds=kwds)
    assert "the same size" in str(ex.value)


def test_invalid_kwds(url_kwds):
    urls, kwds = url_kwds
    kwds = [{"paramss": v} for kw in kwds for _, v in kw.items()]
    with pytest.raises(InputValueError) as ex:
        _ = ar.retrieve(urls, "text", request_kwds=kwds)
    assert "paramss" in str(ex.value)


def test_service_error():
    base_url = "https://api.water.usgs.gov/geoserver/wmadata/ows"
    params = {
        "bbox": "-96.1,28.7,-95.9,28.5,epsg:4326",
        "outputFormat": "application/json",
        "request": "GetFeature",
        "service": "wfs",
        "srsName": "epsg:4269",
        "typeName": "wmadata:nhdflowline_network",
        "version": "2.0.0",
    }
    url = f"{base_url}?{urlencode(params)}"
    with pytest.raises(ServiceError) as ex:
        _ = ar.retrieve([url], "json")
    assert "illegal bbox" in str(ex.value)

    with pytest.raises(ServiceError) as ex:
        _ = ar.stream_write([url], ["temp"])
    assert "Bad Request" in str(ex.value)
    if Path("temp").exists():
        Path("temp").unlink()


def test_wrong_path_number():
    urls = ["https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_500KB_CSV-1.csv"]
    file_paths = ["temp"] * 2
    with pytest.raises(InputTypeError) as ex:
        _ = ar.stream_write(urls, file_paths)
    assert "same size" in str(ex.value)
