#!/usr/bin/env python3

# The arrow library is used to handle datetimes
from datetime import datetime
from logging import Logger, getLogger
from typing import List, Optional

import arrow
from requests import Session

# Local library imports
from parsers.lib import validation

TIMEZONE = "Canada/Atlantic"
URL = "http://localhost:8000/province/NS"
EXCHANGE_REGIONS = {"CA-NB": "New Brunswick"}


def fetch_production(
    zone_key: str = "CA-NS",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> List[dict]:
    """Requests the last known production mix (in MW) of a given country."""
    if target_datetime:
        raise NotImplementedError(
            "This parser is unable to give information more than 24 hours in the past"
        )
    session = session or Session()
    data = session.get(f"{URL}/production").json()
    return validation.validate(
        {
            "datetime": get_current_timestamp(),
            "production": data["production"],
            "source": data["source"],
            "zoneKey": zone_key,
        },
        expected_range={
            "coal": (0, 1300),
            "gas": (0, 700),
            "biomass": (0, 100),
            "hydro": (0, 500),
            "wind": (0, 700),
        },
        logger=logger
    )


def fetch_exchange(
    zone_key1: str,
    zone_key2: str,
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> List[dict]:
    """
    Requests the last known power exchange (in MW) between two regions.

    Note: As of early 2017, Nova Scotia only has an exchange with New Brunswick (CA-NB).
    (An exchange with Newfoundland, "Maritime Link", is scheduled to open in "late 2017").

    The API for Nova Scotia only specifies imports.
    When NS is exporting energy, the API returns 0.
    """
    if target_datetime:
        raise NotImplementedError(
            "This parser is unable to give information more than 24 hours in the past"
        )

    sorted_zone_keys = "->".join(sorted([zone_key1, zone_key2]))

    if sorted_zone_keys != "CA-NB->CA-NS":
        raise NotImplementedError("This exchange pair is not implemented")

    session = session or Session()
    data = session.get(f"{URL}/exchange").json()
    flow = data["flow"]
    if EXCHANGE_REGIONS[zone_key2] not in flow:
        raise NotImplementedError("This exchange pair is not implemented")

    return {
        "datetime": get_current_timestamp(),
        "sortedZoneKeys": sorted_zone_keys,
        "netFlow": float(flow[EXCHANGE_REGIONS[zone_key2]]),
        "source": data["source"],
    }


def get_current_timestamp():
    return arrow.utcnow().to(TIMEZONE).datetime


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    from pprint import pprint

    test_logger = getLogger()

    print("fetch_production() ->")
    pprint(fetch_production(logger=test_logger))

    print('fetch_exchange("CA-NS", "CA-NB") ->')
    pprint(fetch_exchange("CA-NS", "CA-NB", logger=test_logger))
