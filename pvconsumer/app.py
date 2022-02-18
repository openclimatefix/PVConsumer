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

from nowcasting_datamodel.connection import Base_PV, DatabaseConnection
from nowcasting_datamodel.models.pv import PVSystemSQL, PVYield
from pvoutput import PVOutput
from sqlalchemy.orm import Session

from pvconsumer.pv_systems import filter_pv_systems_which_have_new_data, get_pv_systems

logging.basicConfig(
    level=getattr(logging, os.getenv("LOGLEVEL", "INFO")),
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# TODO add click arguements
def app(filename: Optional[str] = None):
    """
    # TODO

    :param filename: the local file name for the pv systems
    :return:
    """
    db_url = os.getenv(DB_URL, "sqlite:///test.db")

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
    for pv_system in pv_systems:

        # lets take the date of the datetime now.
        # Note that we might miss data from the day before
        # if this is the first data pull after midnight.
        # e.g last data pull was at 2022-01-01 23.57, new data pull at 2022-01-02 00.05,
        # then this will just get data for 2022-01-02, and therefore missing
        # 2022-01-01 23.57 to 2022-01-02
        date = datetime_utc.date()
        pv_yield_df = pv_output.get_status(
            pv_system_id=pv_system.pv_system_id, date=date, use_data_service=True
        )

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
                pv_yield_df = pv_yield_df[pv_yield_df.index > last_pv_yield_datetime]

                if len(pv_yield_df) == 0:
                    logger.debug(
                        f"No new data avialble after {last_pv_yield_datetime}. "
                        f"Last data point was {pv_yield_df.index.max()}"
                    )
                    logger.debug(pv_yield_df)
            else:
                logger.debug(
                    f"This is the first lot pv yield data for pv system {(pv_system.pv_system_id)}"
                )

            # need columns datetime_utc, solar_generation_kw
            pv_yield_df = pv_yield_df[["instantaneous_power_gen_W"]]
            pv_yield_df.rename(
                columns={"instantaneous_power_gen_W": "solar_generation_kw"}, inplace=True
            )
            pv_yield_df["datetime_utc"] = pv_yield_df.index

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
