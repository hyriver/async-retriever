"""Tests for the package."""
import io
import shutil
import sys
from datetime import datetime

import ward

import async_retriever as ar


@ward.test("Binary response")
def test_binary():
    west, south, east, north = (-69.77, 45.07, -69.31, 45.45)
    base_url = "https://thredds.daac.ornl.gov/thredds/ncss/ornldaac/1299"
    dates_itr = [(datetime(y, 1, 1), datetime(y, 1, 31)) for y in range(2000, 2005)]
    urls, kwds = zip(
        *(
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
                    },
                },
            )
            for s, e in dates_itr
        ),
    )

    cache_name = "cache_tmp/aiohttp_cache.sqlite"
    r_b = ar.retrieve(urls, "binary", request_kwds=kwds, cache_name=cache_name)
    ar.clean_cache(cache_name)
    shutil.rmtree("cache_tmp")
    assert sys.getsizeof(r_b[0]) == 986161


@ward.test("JSON response")
def test_json():
    urls = ["https://labs.waterdata.usgs.gov/api/nldi/linked-data/comid/position"]
    kwds = [
        {
            "params": {
                "f": "json",
                "coords": "POINT(-68.325 45.0369)",
            },
        },
    ]
    r_j = ar.retrieve(urls, "json", request_kwds=kwds)
    assert r_j[0]["features"][0]["properties"]["identifier"] == "2675320"


@ward.test("JSON request")
def test_json():
    base_url = "https://labs.waterdata.usgs.gov/api/nldi/pygeoapi/processes"
    operation = "nldi-flowtrace"
    url = f"{base_url}/{operation}/jobs?response=document"
    data = {
        "inputs": [
            {"id": "lat", "type": "text/plain", "value": "43.29139"},
            {"id": "lon", "type": "text/plain", "value": "-73.82705"},
            {"id": "raindroptrace", "type": "text/plain", "value": "True"},
            {"id": "direction", "type": "text/plain", "value": "up"},
        ]
    }
    payload = {"json": data}
    r = ar.retrieve([url], "json", [payload], "POST")
    assert r[0]["outputs"]["features"][0]["properties"]["comid"] == 22294818


@ward.test("Text response")
def test_text():
    base = "https://waterservices.usgs.gov/nwis/site/?"
    urls = ["&".join([base, "format=rdb", "sites=01646500", "siteStatus=all"])]

    r_t = ar.retrieve(urls, "text")

    assert r_t[0].split("\n")[-2].split("\t")[1] == "01646500"


@ward.test("Ordered return")
def test_ordered_return():
    stations = ["11073495", "08072300", "01646500"]
    url = "https://waterservices.usgs.gov/nwis/site"
    urls, kwds = zip(
        *((url, {"params": {"format": "rdb", "sites": s, "siteStatus": "all"}}) for s in stations),
    )
    resp = ar.retrieve(urls, "text", request_kwds=kwds)
    assert [r.split("\n")[-2].split("\t")[1] for r in resp] == stations


@ward.test("Show versions")
def test_show_versions():
    f = io.StringIO()
    ar.show_versions(file=f)
    assert "INSTALLED VERSIONS" in f.getvalue()
