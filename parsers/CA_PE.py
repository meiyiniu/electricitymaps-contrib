#!/usr/bin/env python3

import json
from datetime import datetime
from logging import Logger, getLogger
from typing import Any, Dict, Optional

# The arrow library is used to handle datetimes consistently with other parsers
import arrow
from requests import Session

timezone = "Canada/Atlantic"
URL = "localhost:8000/province/PE"

def fetch_production(
    zone_key: str = "CA-PE",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> Dict[str, Any]:
    """Requests the last known production mix (in MW) of a given country."""
    if target_datetime:
        raise NotImplementedError("This parser is not yet able to parse past dates")

    session = session or Session()
    pei_info = session.get(f"{URL}/production").json()

    if pei_info is None:
        return None

    data = {
        "datetime": pei_info["datetime"],
        "zoneKey": zone_key,
        "production": pei_info["production"],
        "storage": {},
        "source": "princeedwardisland.ca",
    }

    return data


def fetch_exchange(
    zone_key1: str,
    zone_key2: str,
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> Dict[str, Any]:
    """Requests the last known power exchange (in MW) between two regions."""
    if target_datetime:
        raise NotImplementedError("This parser is not yet able to parse past dates")

    sorted_zone_keys = "->".join(sorted([zone_key1, zone_key2]))

    if sorted_zone_keys != "CA-NB->CA-PE":
        raise NotImplementedError("This exchange pair is not implemented")

    session = session or Session()
    pei_info = session.get(f"{URL}/exchange").json()

    if pei_info is None:
        return None

    # In expected result, "net" represents an export.
    # We have sorted_zone_keys 'CA-NB->CA-PE', so it's export *from* NB,
    # and import *to* PEI.
    data = {
        "datetime": pei_info["datetime"],
        "sortedZoneKeys": sorted_zone_keys,
        "netFlow": pei_info["flow"]["New Brunswick"],
        "source": "princeedwardisland.ca",
    }

    return data


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    print("fetch_production() ->")
    print(fetch_production())

    print('fetch_exchange("CA-PE", "CA-NB") ->')
    print(fetch_exchange("CA-PE", "CA-NB"))
