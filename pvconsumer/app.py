""" Application for getting live pv data

1. Load Pv system ids from local csv file
2. For each site, find the most recent data in a database
3. Pull data from pvoutput.org, if more data is available. The pv site has a certain refresh rate.
4. Save data to database - extra: check no duplicate data is added to the database
"""

import logging
import os
from datetime import datetime
from typing import List, Optional, Tuple

import click
import pandas as pd
from pvoutput import PVOutput
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import SiteSQL
from sqlalchemy.orm import Session

import pvconsumer
from pvconsumer.pv_systems import filter_pv_systems_which_have_new_data, get_pv_systems
from pvconsumer.save import save_to_pv_site_database
from pvconsumer.solar_sheffield_passiv import get_all_latest_pv_yield_from_solar_sheffield
from pvconsumer.utils import format_pv_data

logging.basicConfig(
    level=getattr(logging, os.getenv("LOGLEVEL", "INFO")),
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--db-url",
    default=None,
    envvar="DB_URL_PV_SITE",
    help="The PV site Database URL where update latest data will be saved",
    type=click.STRING,
)
@click.option(
    "--filename",
    default=None,
    envvar="FILENAME",
    help="Filename of PV systems ids. Default is saved in pvconsumer/data",
    type=click.STRING,
)
@click.option(
    "--provider",
    default="pvoutput.org",
    envvar="PROVIDER",
    help="Name of the PV data provider",
    type=click.STRING,
)
def app(
    db_url: Optional[str] = None,
    filename: Optional[str] = None,
    provider: str = "pvoutput.org",
):
    """
    Run PV consumer app, this collect live PV data and save it to a database.

    :param db_url: the Database url to save the PV system data
    :param db_url_forecast: the Database url to save the Input data last updated
    :param filename: the local file name for the pv systems
    :return:
    """

    logger.info(f"Running PV Consumer app ({pvconsumer.__version__})")

    connection = DatabaseConnection(url=db_url, echo=False)

    with connection.get_session() as session:
        # 1. Read list of PV systems (from local file)
        # and get their refresh times (refresh times can also be stored locally)
        logger.debug("Read list of PV systems (from local file)")
        pv_systems = get_pv_systems(session=session, filename=filename, provider=provider)

        # 2. Find most recent entered data (for each PV system) in OCF database,
        # and filter depending on refresh rate
        logger.debug(
            "Find most recent entered data (for each PV system) in OCF database,"
            "and filter pv systems depending on refresh rate"
        )
        pv_systems = filter_pv_systems_which_have_new_data(pv_systems=pv_systems, session=session)

        # 3. Pull data
        pull_data_and_save(
            pv_systems=pv_systems,
            session=session,
            provider=provider,
        )


def pull_data_and_save(
    pv_systems: List[SiteSQL],
    session: Session,
    provider: str,
    datetime_utc: Optional[None] = None,
):
    """
    Pull the pv ield data and save to database

    :param pv_systems: list of pv systems to save
    :param session: database sessions
    :param provider: provider name
    :param datetime_utc: datetime now, this is optional
    """

    if provider == "pvoutput.org":
        # set up pv output.prg
        pv_output = PVOutput()
    elif provider == "solar_sheffield_passiv":
        # get all pv yields from solar sheffield
        all_pv_yield_df = get_all_latest_pv_yield_from_solar_sheffield()
        logger.debug(f"Found {len(all_pv_yield_df)} PV yields from solar sheffield passiv")
    else:
        raise Exception(f"Can not use provider {provider}")

    if datetime_utc is None:
        datetime_utc = datetime.utcnow()  # add timezone
    date = datetime_utc.date()

    logger.info(f"Pulling data for pv system {len(pv_systems)} pv systems for {date}")

    n_pv_systems_per_batch = 50
    pv_system_chunks = chunks(original_list=pv_systems, n=n_pv_systems_per_batch)

    all_pv_yields_df = []
    for pv_system_chunk in pv_system_chunks:
        if provider == "pvoutput.org":
            # set up pv output.org
            pv_output = PVOutput()

            # get all the pv system ids from a a group of pv systems
            pv_system_ids = [pv_system.client_site_id for pv_system in pv_system_chunk]

            logger.debug(f"Getting data from {provider}")

            # lets take the date of the datetime now.
            # Note that we might miss data from the day before
            # if this is the first data pull after midnight.
            # e.g last data pull was at 2022-01-01 23.57, new data pull at 2022-01-02 00.05,
            # then this will just get data for 2022-01-02, and therefore missing
            # 2022-01-01 23.57 to 2022-01-02
            all_pv_yield_df = pv_output.get_system_status(
                pv_system_ids=pv_system_ids,
                date=date,
                use_data_service=True,
                timezone="Europe/London",
            )
        elif provider == "solar_sheffield_passiv":
            pass
        else:
            raise Exception(f"Can not use provider {provider}")

        for i, pv_system in enumerate(pv_system_chunk):
            logger.debug(
                f"Processing {i}th pv system ({pv_system.client_site_id=}), "
                f"out of {len(pv_systems)}"
            )

            # take only the data we need for system id
            pv_yield_df = all_pv_yield_df[
                all_pv_yield_df["system_id"].astype(int) == pv_system.client_site_id
            ]
            pv_yield_df["site_uuid"] = pv_system.site_uuid

            logger.debug(
                f"Got {len(pv_yield_df)} pv yield for "
                f"pv systems {pv_system.client_site_id} before filtering"
            )

            if len(pv_yield_df) == 0:
                logger.warning(f"Did not find any data for {pv_system.client_site_id} for {date}")
            else:
                # filter out which is in our database and a funny 0 bug
                pv_yield_df = format_pv_data(
                    pv_system=pv_system, pv_yield_df=pv_yield_df, session=session
                )

                if len(all_pv_yields_df) == 0:
                    all_pv_yields_df = pv_yield_df
                else:
                    all_pv_yields_df = pd.concat([all_pv_yields_df, pv_yield_df])

                if len(all_pv_yields_df) > 100:
                    # 4. Save to database - perhaps check no duplicate data. (for each PV system)
                    save_to_pv_site_database(session=session, pv_yield_df=all_pv_yields_df)
                    all_pv_yields_df = []

    # 4. Save to database - perhaps check no duplicate data. (for each PV system)
    logger.debug(all_pv_yields_df)
    save_to_pv_site_database(session=session, pv_yield_df=all_pv_yields_df)


def chunks(original_list: List, n: int) -> Tuple[List]:
    """This chunks up a list into a list of list.

    Each sub list has 'n' elements
    """
    n = max(1, n)
    return (original_list[i : i + n] for i in range(0, len(original_list), n))


if __name__ == "__main__":
    app()
