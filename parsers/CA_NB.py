#!/usr/bin/env python3

# The arrow library is used to handle datetimes consistently with other parsers
from datetime import datetime
from logging import Logger, getLogger
from typing import Optional

import arrow
from requests import Session

timezone = "Canada/Atlantic"
URL = "http://localhost:8000/province/NB"

EXCHANGE_REGIONS = {
    "CA-QC": "Quebec",
    "US-NE-ISNE": "Maine",
    "CA-NS": "Nova Scotia",
    "CA-PE": "PEI",
}


def fetch_production(
    zone_key: str = "CA-NB",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> dict:
    """Requests the last known production mix (in MW) of a given country."""

    """
    In this case, we are calculating the amount of electricity generated
    in New Brunswick, versus imported and exported elsewhere.
    """

    if target_datetime:
        raise NotImplementedError("This parser is not yet able to parse past dates")

    session = session or Session()
    data = session.get(f"{URL}/production").json()

    result = {
        "datetime": arrow.utcnow().floor("minute").datetime,
        "zoneKey": zone_key,
        "production": data["production"],
        "storage": data.get("storage") if data.get("storage") else {},
        "source": data["source"],
    }

    return result


def fetch_exchange(
    zone_key1: str,
    zone_key2: str,
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> dict:
    """Requests the last known power exchange (in MW) between two regions."""

    if target_datetime:
        raise NotImplementedError("This parser is not yet able to parse past dates")

    sorted_zone_keys = "->".join(sorted([zone_key1, zone_key2]))

    data = session.get(f"{URL}/exchange").json()
    flows = data["flow"]

    if EXCHANGE_REGIONS[zone_key2] not in flows:
        raise NotImplementedError(
            f"This exchange pair '{sorted_zone_keys}' is not implemented"
        )

    result = {
        "datetime": arrow.utcnow().floor("minute").datetime,
        "sortedZoneKeys": sorted_zone_keys,
        "netFlow": flows[EXCHANGE_REGIONS[zone_key2]],
        "source": data["source"],
    }

    return result


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    print("fetch_production() ->")
    print(fetch_production())

    print('fetch_exchange("CA-NB", "CA-PE") ->')
    print(fetch_exchange("CA-NB", "CA-PE"))
