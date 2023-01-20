""" Utils functions """
import logging
from datetime import timezone
from typing import List

import pandas as pd
from nowcasting_datamodel.models import PVSystem, PVSystemSQL, PVYield, PVYieldSQL

logger = logging.getLogger(__name__)


def list_pv_system_to_df(pv_systems: List[PVSystem]) -> pd.DataFrame:
    """
    Change list of pv systems to dataframe

    Args:
        pv_systems: list of pv systems (pdyantic objects)

    Returns: dataframe with columns the same as the pv systems pydantic object

    """
    return pd.DataFrame([pv_system.dict() for pv_system in pv_systems])


def df_to_list_pv_system(pv_systems_df=pd.DataFrame) -> List[PVSystem]:
    """
    Change dataframe to lsit of pv systems

    Args:
        pv_systems_df: dataframe with columns the same as the pv systems pydantic object

    Returns: list of pv systems

    """
    return [PVSystem(**row) for row in pv_systems_df.to_dict(orient="records")]


def format_pv_data(pv_system: PVSystemSQL, pv_yield_df: pd.DataFrame) -> List[PVYieldSQL]:
    """
    Format the pv data

    1. get rid of 0 bug
    2. remove data if already in our database
    3. format in to PVYield objects
    4. convert to SQL objects

    :param pv_system: the pv system this data is about
    :param pv_yield_df: the pv yield data with columns 'instantaneous_power_gen_W' and 'datetime'
    :return: list of pv yield sql objects
    """

    # 0.1 rename
    if "solar_generation_kw" not in pv_yield_df.columns:
        pv_yield_df["solar_generation_kw"] = pv_yield_df["instantaneous_power_gen_W"] / 1000
    if "datetime_utc" not in pv_yield_df.columns:
        pv_yield_df.rename(
            columns={
                "datetime": "datetime_utc",
            },
            inplace=True,
        )

    # 1. We have seen a bug in pvoutput.org where the last value is 0,
    # but then a minute later its gets updated. To solve this,
    # we drop the last row if its zero, but not if there are two zeros.
    # This is beasue if there are two zeros,
    # then the PV system might be actually producing no power
    if len(pv_yield_df) > 1:
        if (
            pv_yield_df.iloc[-1].solar_generation_kw == 0
            and pv_yield_df.iloc[-2].solar_generation_kw != 0
        ):
            logger.debug(
                f"Dropping last row of pv data for "
                f"{pv_system.pv_system_id} "
                f"as last row is 0, but the second to last row is not."
            )
            pv_yield_df.drop(pv_yield_df.tail(1).index, inplace=True)

    # 2. filter by last
    if pv_system.last_pv_yield is not None:
        last_pv_yield_datetime = pv_system.last_pv_yield.datetime_utc.replace(tzinfo=timezone.utc)

        pv_yield_df = pv_yield_df[pv_yield_df["datetime_utc"] > last_pv_yield_datetime]

        if len(pv_yield_df) == 0:
            logger.debug(
                f"No new data available after {last_pv_yield_datetime}. "
                f"Last data point was {pv_yield_df.index.max()}"
            )
            logger.debug(pv_yield_df)
    else:
        logger.debug(
            f"This is the first lot pv yield data for " f"pv system {(pv_system.pv_system_id)}"
        )

    # 3. format in to PVYield objects
    # need columns datetime_utc, solar_generation_kw
    pv_yield_df = pv_yield_df[["solar_generation_kw", "datetime_utc"]]

    # change to list of pydantic objects
    pv_yields = [PVYield(**row) for row in pv_yield_df.to_dict(orient="records")]
    # 4. change to sqlalamcy objects and add pv systems
    pv_yields_sql = [pv_yield.to_orm() for pv_yield in pv_yields]
    for pv_yield_sql in pv_yields_sql:
        pv_yield_sql.pv_system = pv_system
    logger.debug(f"Found {len(pv_yields_sql)} pv yield for pv systems {pv_system.pv_system_id}")

    return pv_yields_sql


class FakeDatabaseConnection:
    """Fake Database connection class"""

    def __init__(self):
        """
        Set up fake database connection, this is so we can still do
        'with connection.get_session() as sessions:'
        bu session is None
        """

        class FakeSession:
            def __init__(self):
                pass

            def __enter__(self):
                return None

            def __exit__(self, type, value, traceback):
                pass

        self.Session = FakeSession

    def get_session(self) -> Session:
        """Get sqlalamcy session"""
        return self.Session()
