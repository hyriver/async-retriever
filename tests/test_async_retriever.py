"""Tests for the package."""
import asyncio
import io
import sys
from datetime import datetime
from pathlib import Path

from aiohttp_client_cache import SQLiteBackend

import async_retriever as ar

SMALL = 1e-3


async def check_url(url, method="GET", **kwargs):
    cache = SQLiteBackend(cache_name=Path("cache", "aiohttp_cache.sqlite"))
    return await cache.has_url(url, method, **kwargs)


def test_disable_cache():
    url = "https://nationalmap.gov/epqs/pqs.php"
    payload = {"params": {"x": -101, "y": 38, "units": "Meters", "output": "json"}}
    resp = ar.retrieve([url], "json", [payload], disable=True)
    elev = resp[0]["USGS_Elevation_Point_Query_Service"]["Elevation_Query"]["Elevation"]
    assert abs(elev - 880.38) < SMALL and not asyncio.run(check_url(url, params=payload["params"]))


def test_delete_url():
    url = "https://nationalmap.gov/epqs/pqs.php"
    payload = {"params": {"x": -100, "y": 38, "units": "Meters", "output": "json"}}
    resp = ar.retrieve([url], "json", [payload])
    elev = resp[0]["USGS_Elevation_Point_Query_Service"]["Elevation_Query"]["Elevation"]
    ar.delete_url_cache(url, params=payload["params"])
    assert abs(elev - 761.67) < SMALL and not asyncio.run(check_url(url, params=payload["params"]))


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

    cache_name = "cache/tmp/aiohttp_cache.sqlite"
    r = ar.retrieve(urls, "binary", request_kwds=kwds, cache_name=cache_name, ssl=False)
    r_b = ar.retrieve_binary(urls, request_kwds=kwds, cache_name=cache_name, ssl=False)
    assert sys.getsizeof(r[0]) == sys.getsizeof(r_b[0]) == 986161


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
    r = ar.retrieve(urls, "json", kwds)
    r_j = ar.retrieve_json(urls, kwds)
    r_id = r[0]["features"][0]["properties"]["identifier"]
    rj_id = r_j[0]["features"][0]["properties"]["identifier"]
    assert r_id == rj_id == "2675320"


def test_text_post():
    base = "https://waterservices.usgs.gov/nwis/site/?"
    urls = [
        "&".join([base, "format=rdb", f"sites={','.join(['01646500'] * 20)}", "siteStatus=all"])
    ]

    r = ar.retrieve(urls, "text", request_method="POST")
    r_t = ar.retrieve_text(urls, request_method="POST")
    r_id = r[0].split("\n")[-2].split("\t")[1]
    rt_id = r_t[0].split("\n")[-2].split("\t")[1]

    assert r_id == rt_id == "01646500"


def test_ordered_return():
    stations = ["11073495", "08072300", "01646500"]
    url = "https://waterservices.usgs.gov/nwis/site"
    urls, kwds = zip(
        *((url, {"params": {"format": "rdb", "sites": s, "siteStatus": "all"}}) for s in stations),
    )
    resp = ar.retrieve(urls, "text", request_kwds=kwds)
    assert [r.split("\n")[-2].split("\t")[1] for r in resp] == stations


def test_show_versions():
    f = io.StringIO()
    ar.show_versions(file=f)
    assert "INSTALLED VERSIONS" in f.getvalue()
