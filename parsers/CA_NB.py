#!/usr/bin/env python3

# The arrow library is used to handle datetimes consistently with other parsers
from datetime import datetime
from logging import Logger, getLogger
from typing import Optional

import arrow

# BeautifulSoup is used to parse HTML to get information
from bs4 import BeautifulSoup
from requests import Session

timezone = "Canada/Atlantic"
URL = "localhost:8000/province/NB"

EXCHANGE_REGIONS = {
    "CA-QC": "Quebec",
    "US-NE-ISNE": "Maine",
    "CA-NS": "Nova Scotia",
    "CA-PE": "PEI"
}


def _get_new_brunswick_flows(requests_obj):
    """
    Gets current electricity flows in and out of New Brunswick.

    There is no reported data timestamp in the page.
    The page returns current time and says "Times at which values are sampled may vary by as much as 5 minutes."
    """

    url = "https://tso.nbpower.com/Public/en/SystemInformation_realtime.asp"
    response = requests_obj.get(url)

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", attrs={"bordercolor": "#191970"})

    rows = table.find_all("tr")

    headers = rows[1].find_all("td")
    values = rows[2].find_all("td")

    flows = {
        headers[i].text.strip(): float(row.text.strip()) for i, row in enumerate(values)
    }

    return flows


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

    data = session.get(f"{URL}/price").json()

    result = {
        "datetime": arrow.utcnow().floor("minute").datetime,
        "zoneKey": zone_key,
        "production": data["production"],
        "storage": data["storage"] if data["storage"] else {},
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
        raise NotImplementedError(f"This exchange pair '{sorted_zone_keys}' is not implemented")

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
