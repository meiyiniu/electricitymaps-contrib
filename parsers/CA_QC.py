from datetime import datetime
from logging import Logger, getLogger
from pprint import pprint
from typing import Optional

# The arrow library is used to handle datetimes
import arrow
from requests import Session

PRODUCTION_URL = "http://localhost:8000/province/QC/production"
CONSUMPTION_URL = "https://www.hydroquebec.com/data/documents-donnees/donnees-ouvertes/json/demande.json"

# Reluctant to call it 'timezone', since we are importing 'timezone' from datetime
TIMEZONE = "America/Montreal"


def fetch_production(
    zone_key: str = "CA-QC",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> list:
    """Requests the last known production mix (in MW) of a given region.
    In this particular case, translated mapping of JSON keys are also required"""

    def if_exists(elem: dict, etype: str):

        english = {
            "hydraulique": "hydro",
            "thermique": "thermal",
            "solaire": "solar",
            "eolien": "wind",
            # autres is all renewable, and mostly biomass.  See Github    #3218
            "autres": "biomass",
            "valeurs": "values",
        }
        english = {v: k for k, v in english.items()}
        try:
            return elem["valeurs"][english[etype]]
        except KeyError:
            return 0.0

    data = _fetch_quebec_production(session)
    res = {
        "zoneKey": zone_key,
        "datetime": get_current_timestamp(),
        "production": data["production"],
        "source": "hydroquebec.com",
    }
    return res


def fetch_consumption(
    zone_key: str = "CA-QC",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
):
    data = _fetch_quebec_consumption(session)
    list_res = []
    for elem in reversed(data["details"]):
        if "demandeTotal" in elem["valeurs"]:
            list_res.append(
                {
                    "zoneKey": zone_key,
                    "datetime": get_current_timestamp(),
                    "consumption": elem["valeurs"]["demandeTotal"],
                    "source": "hydroquebec.com",
                }
            )
    return list_res


def _fetch_quebec_production(
    session: Optional[Session] = None, logger: Logger = getLogger(__name__)
) -> str:
    s = session or Session()
    response = s.get(PRODUCTION_URL)

    if not response.ok:
        logger.info(
            "CA-QC: failed getting requested production data from hydroquebec - URL {}".format(
                PRODUCTION_URL
            )
        )
    return response.json()


def _fetch_quebec_consumption(
    session: Optional[Session] = None, logger: Logger = getLogger(__name__)
) -> str:
    s = session or Session()
    response = s.get(CONSUMPTION_URL)

    if not response.ok:
        logger.info(
            "CA-QC: failed getting requested consumption data from hydroquebec - URL {}".format(
                CONSUMPTION_URL
            )
        )
    return response.json()


def get_current_timestamp():
    return arrow.utcnow().to(TIMEZONE).datetime


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    test_logger = getLogger()

    print("fetch_production() ->")
    pprint(fetch_production(logger=test_logger))

    print("fetch_consumption() ->")
    pprint(fetch_consumption(logger=test_logger))
