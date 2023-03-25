#!/usr/bin/env python3

from datetime import datetime
from logging import Logger, getLogger
from typing import Optional

import arrow
from bs4 import BeautifulSoup
from requests import Session

TIMEZONE = "America/Whitehorse"


def fetch_production(
    zone_key: str = "CA-YT",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> dict:
    """Requests the last known production mix (in MW) of a given region."""

    """
    We are using Yukon Energy's data from
    http://www.yukonenergy.ca/energy-in-yukon/electricity-101/current-energy-consumption

    Generation in Yukon is done with hydro, diesel oil, and LNG.

    There are two companies, Yukon Energy and ATCO aka Yukon Electric aka YECL.
    Yukon Energy does most of the generation and feeds into Yukon's grid.
    ATCO does operations, billing, and generation in some of the off-grid communities.

    See schema of the grid at http://www.atcoelectricyukon.com/About-Us/

    Per https://en.wikipedia.org/wiki/Yukon#Municipalities_by_population
    of total population 35874 (2016 census), 28238 are in municipalities
    that are connected to the grid - that is 78.7%.

    Off-grid generation is with diesel generators, this is not reported online as of 2017-06-23
    and is not included in this calculation.

    Yukon Energy reports only "hydro" and "thermal" generation.
    Per http://www.yukonenergy.ca/ask-janet/lng-and-boil-off-gas,
    in 2016 the thermal generation was about 50% diesel and 50% LNG.
    But since Yukon Energy doesn't break it down on their website,
    we return all thermal as "unknown".

    Per https://en.wikipedia.org/wiki/List_of_generating_stations_in_Yukon
    Yukon Energy operates about 98% of Yukon's hydro capacity, the only exception is
    the small 1.3 MW Fish Lake dam operated by ATCO/Yukon Electrical.
    That's small enough to not matter, I think.

    There is also a small 0.81 MW wind farm, its current generation is not available.
    """
    if target_datetime:
        raise NotImplementedError("This parser is not yet able to parse past dates")

    requests_obj = session or Session()

    url = "http://localhost:8000/province/YT"
    data = requests_obj.get(f"{url}/production").json()
    capacity = data["capacity"]
    production = data["production"]

    data = {
        "datetime": get_current_timestamp(),
        "zoneKey": zone_key,
        "production": {
            "unknown": production["unknown"],
            "hydro": production["hydro"],
            # specify some sources that aren't present in Yukon as zero,
            # this allows the analyzer to better estimate CO2eq
            "coal": 0,
            "nuclear": 0,
            "geothermal": 0,
        },
        "capacity": capacity,
        "source": data["source"],
    }

    return data


def get_current_timestamp():
    return arrow.utcnow().to(TIMEZONE).datetime


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    print("fetch_production() ->")
    print(fetch_production())
