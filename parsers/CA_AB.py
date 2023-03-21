#!/usr/bin/env python3

"""Parse the Alberta Electric System Operator's (AESO's) Energy Trading System
(ETS) website.
"""

# Standard library imports
import csv
import logging
import re
import urllib.parse
from datetime import datetime
from logging import Logger, getLogger
from typing import Any, Dict, Optional

# Third-party library imports
import arrow
from requests import Session

# Local library imports
from parsers.lib import validation

DEFAULT_ZONE_KEY = "CA-AB"
MINIMUM_PRODUCTION_THRESHOLD = 10  # MW
TIMEZONE = "Canada/Mountain"
URL = "localhost:8000/province/AB"

EXCHANGE_REGIONS = {
    "CA-AB": "Alberta",
    "CA-BC": "British Columbia",
    "CA-SK": "Saskatchewan",
    "US-NW-NWMT": "Montana",
}


def fetch_exchange(
    zone_key1: str = DEFAULT_ZONE_KEY,
    zone_key2: str = "CA-BC",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> dict:
    """Request the last known power exchange (in MW) between two countries."""
    if target_datetime:
        raise NotImplementedError("Currently unable to scrape historical data")
    session = session or Session()
    data = session.get(f"{URL}/exchange").json()
    flow = data["flow"]
    sorted_zone_keys = "->".join(sorted((zone_key1, zone_key2)))
    if EXCHANGE_REGIONS[zone_key2] not in flow:
        raise NotImplementedError(f"Pair '{sorted_zone_keys}' not implemented")
    return {
        "datetime": get_current_timestamp(),
        "sortedZoneKeys": sorted_zone_keys,
        "netFlow": float(flow[EXCHANGE_REGIONS[zone_key2]]),
        "source": data["source"],
    }


def fetch_price(
    zone_key: str = DEFAULT_ZONE_KEY,
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> list:
    """Request the last known power price of a given country."""
    if target_datetime:
        raise NotImplementedError("Currently unable to scrape historical data")
    session = session or Session()
    data = session.get(f"{URL}/price").json()

    return [
        {
            "currency": data["currency"],
            "datetime": get_current_timestamp(),
            "price": data["price"],
            "source": URL.netloc,
            "zoneKey": zone_key,
        }
    ]


def fetch_production(
    zone_key: str = DEFAULT_ZONE_KEY,
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> Dict[str, Any]:
    """Request the last known production mix (in MW) of a given country."""
    if target_datetime:
        raise NotImplementedError("This parser is not yet able to parse past dates")
    session = session or Session()
    data = session.get(f"{URL}/production").json()
    capacity = data["capacity"]
    production = data["production"]

    return validation.validate(
        {
            "capacity": capacity,
            "datetime": get_current_timestamp(),
            "production": {
                x: production[x] for x in production if x != "battery storage"
            },
            "source": data["source"],
            "storage": {
                "battery": production["battery storage"],
            },
            "zoneKey": zone_key,
        },
        logger,
        floor=MINIMUM_PRODUCTION_THRESHOLD,
        remove_negative=True,
    )


def get_current_timestamp():
    return arrow.to(TIMEZONE).datetime


if __name__ == "__main__":
    # Never used by the electricityMap backend, but handy for testing.
    print("fetch_production() ->")
    print(fetch_production())
    print("fetch_price() ->")
    print(fetch_price())
    print(f"fetch_exchange({DEFAULT_ZONE_KEY}, CA-BC) ->")
    print(fetch_exchange(DEFAULT_ZONE_KEY, "CA-BC"))
    print(f"fetch_exchange({DEFAULT_ZONE_KEY}, CA-SK) ->")
    print(fetch_exchange(DEFAULT_ZONE_KEY, "CA-SK"))
    print(f"fetch_exchange({DEFAULT_ZONE_KEY}, US-MT) ->")
    print(fetch_exchange(DEFAULT_ZONE_KEY, "US-MT"))
    print(f"fetch_exchange({DEFAULT_ZONE_KEY}, US-NW-NWMT) ->")
    print(fetch_exchange(DEFAULT_ZONE_KEY, "US-NW-NWMT"))
