""" Application for getting live pv data

1. Load Pv system ids from local csv file
2. For each site, find the most recent data in a database
3. Pull data from pvoutput.org, if more data is available. The pv site has a certain refresh rate.
4. Save data to database - extra: check no duplicate data is added to the database
"""

import logging
import os
from datetime import datetime
from typing import List, Optional

import click
from nowcasting_datamodel.connection import Base_PV, DatabaseConnection
from nowcasting_datamodel.models.pv import PVSystemSQL, PVYield
from pvoutput import PVOutput
from sqlalchemy.orm import Session

import pvconsumer
from pvconsumer.pv_systems import filter_pv_systems_which_have_new_data, get_pv_systems

logging.basicConfig(
    level=getattr(logging, os.getenv("LOGLEVEL", "INFO")),
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--db-url",
    default=None,
    envvar="DB_URL",
    help="The Database URL where forecasts will be saved",
    type=click.STRING,
)
@click.option(
    "--filename",
    default=None,
    envvar="FILENAME",
    help="Filename of PV systems ids. Default is saved in pvconsumer/data",
    type=click.STRING,
)
def app(db_url: str, filename: Optional[str] = None):
    """
    Run PV consumer app, this collect live PV data and save it to a database.

    :param db_url: the Database url to save the PV system data
    :param filename: the local file name for the pv systems
    :return:
    """

    logger.info(f"Running PV Consumer app ({pvconsumer.__version__})")

    connection = DatabaseConnection(url=db_url, base=Base_PV, echo=False)
    with connection.get_session() as session:
        # 1. Read list of PV systems (from local file)
        # and get their refresh times (refresh times can also be stored locally)
        logger.debug("Read list of PV systems (from local file)")
        pv_systems = get_pv_systems(session=session, filename=filename)

        # 2. Find most recent entered data (for each PV system) in OCF database,
        # and filter depending on refresh rate
        logger.debug(
            "Find most recent entered data (for each PV system) in OCF database,"
            "and filter pv systems depending on refresh rate"
        )
        pv_systems = filter_pv_systems_which_have_new_data(pv_systems=pv_systems)

        # 3. Pull data
        pv_yields = pull_data(pv_systems=pv_systems, session=session)

        # 4. Save to database - perhaps check no duplicate data. (for each PV system)
        save_to_database(session=session, pv_yields=pv_yields)


def pull_data(pv_systems: List[PVSystemSQL], session: Session, datetime_utc: Optional[None] = None):
    """
    # TODO

    :param pv_systems:
    :param session:
    :param datetime_utc:
    :return:
    """

    pv_output = PVOutput()

    if datetime_utc is None:
        datetime_utc = datetime.utcnow()  # add timezone

    logger.info(f"Pulling data for pv system {len(pv_systems)} pv systems for {datetime_utc}")

    all_pv_yields = []

    n_pv_systems_per_batch = 50
    pv_system_chunks = chunks(l=pv_systems, n=n_pv_systems_per_batch)

    for pv_system_chunk in pv_system_chunks:

        # get all the pv system ids from a a group of pv systems
        pv_system_ids = [pv_system_id.pv_system_id for pv_system_id in pv_system_chunk]

        # lets take the date of the datetime now.
        # Note that we might miss data from the day before
        # if this is the first data pull after midnight.
        # e.g last data pull was at 2022-01-01 23.57, new data pull at 2022-01-02 00.05,
        # then this will just get data for 2022-01-02, and therefore missing
        # 2022-01-01 23.57 to 2022-01-02
        date = datetime_utc.date()
        all_pv_yield_df = pv_output.get_system_status(
            pv_system_ids=pv_system_ids, date=date, use_data_service=True
        )

        for pv_system in pv_system_chunk:

            # TODO filter pv_yield_df on a pv_system
            pv_yield_df = all_pv_yield_df

            logger.debug(
                f"Got {len(pv_yield_df)} pv yield for "
                f"pv systems {pv_system.pv_system_id} before filtering"
            )

            if len(pv_yield_df) == 0:
                logger.warning(f"Did not find any data for {pv_system.pv_system_id} for {date}")
            else:

                # filter by last
                if pv_system.last_pv_yield is not None:
                    last_pv_yield_datetime = pv_system.last_pv_yield.datetime_utc
                    pv_yield_df = pv_yield_df[pv_yield_df["datetime"] > last_pv_yield_datetime]

                    if len(pv_yield_df) == 0:
                        logger.debug(
                            f"No new data available after {last_pv_yield_datetime}. "
                            f"Last data point was {pv_yield_df.index.max()}"
                        )
                        logger.debug(pv_yield_df)
                else:
                    logger.debug(
                        f"This is the first lot pv yield data for pv system {(pv_system.pv_system_id)}"
                    )

                # need columns datetime_utc, solar_generation_kw
                pv_yield_df = pv_yield_df[["instantaneous_power_gen_W", "datetime"]]
                pv_yield_df.rename(
                    columns={
                        "instantaneous_power_gen_W": "solar_generation_kw",
                        "datetime": "datetime_utc",
                    },
                    inplace=True,
                )

                # change to list of pydantic objects
                pv_yields = [PVYield(**row) for row in pv_yield_df.to_dict(orient="records")]

                # change to sqlalamcy objects and add pv systems
                pv_yields_sql = [pv_yield.to_orm() for pv_yield in pv_yields]
                for pv_yield_sql in pv_yields_sql:
                    pv_yield_sql.pv_system = pv_system

                all_pv_yields = all_pv_yields + pv_yields_sql

                logger.debug(
                    f"Found {len(pv_yields_sql)} pv yield for pv systems {pv_system.pv_system_id}"
                )

    return all_pv_yields


def chunks(l: List, n: int) -> Tuple(List):
    """This chunks up a list into a list of list.

    Each sub list has 'n' elements
    """
    n = max(1, n)
    return (l[i : i + n] for i in range(0, len(l), n))


def save_to_database(session: Session, pv_yields: List[PVYield]):
    """
    Save pv data to database

    :param session: database session
    :param pv_yields: list of pv data
    """
    logger.debug(f"Will be adding {len(pv_yields)} pv yield object to database")

    session.add_all(pv_yields)
    session.commit()


if __name__ == "__main__":
    app()
