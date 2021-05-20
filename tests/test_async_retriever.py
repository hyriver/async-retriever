"""Tests for ar package."""
import io
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

import async_retriever as ar


@pytest.mark.flaky(max_runs=3)
def test_binary():
    west, south, east, north = (-69.77, 45.07, -69.31, 45.45)
    base_url = "https://thredds.daac.ornl.gov/thredds/ncss/ornldaac/1299"
    dates_itr = [(datetime(y, 1, 1), datetime(y, 1, 31)) for y in range(2000, 2005)]
    urls, kwds = zip(
        *[
            (
                f"{base_url}/MCD13.A{s.year}.unaccum.nc4",
                {
                    "params": {
                        "var": "NDVI",
                        "north": f"{north}",
                        "west": f"{west}",
                        "east": f"{east}",
                        "south": f"{south}",
                        "disableProjSubset": "on",
                        "horizStride": "1",
                        "time_start": s.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "time_end": e.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "timeStride": "1",
                        "addLatLon": "true",
                        "accept": "netcdf",
                    }
                },
            )
            for s, e in dates_itr
        ]
    )

    if sys.platform.startswith("win"):
        return Path(tempfile.gettempdir(), "aiohttp_cache.sqlite")

    cache_name = Path("~/.cache/aiohttp_cache.sqlite")
    r_b = ar.retrieve(urls, "binary", request_kwds=kwds, cache_name=cache_name)
    assert sys.getsizeof(r_b[0]) == 986161


@pytest.mark.flaky(max_runs=3)
def test_json():
    urls = ["https://labs.waterdata.usgs.gov/api/nldi/linked-data/comid/position"]
    kwds = [
        {
            "params": {
                "f": "json",
                "coords": "POINT(-68.325 45.0369)",
            }
        }
    ]
    r_j = ar.retrieve(urls, "json", request_kwds=kwds)
    assert r_j[0]["features"][0]["properties"]["identifier"] == "2675320"


@pytest.mark.flaky(max_runs=3)
def test_text():
    base = "https://waterservices.usgs.gov/nwis/site/?"
    urls = ["&".join([base, "format=rdb", "sites=01646500", "siteStatus=all"])]

    r_t = ar.retrieve(urls, "text")

    assert r_t[0].split("\n")[-2].split("\t")[1] == "01646500"


def test_show_versions():
    f = io.StringIO()
    ar.show_versions(file=f)
    assert "INSTALLED VERSIONS" in f.getvalue()
