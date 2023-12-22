# """ Utils functions """
import logging
from datetime import timezone

import pandas as pd
from sqlalchemy.orm import Session

from pvsite_datamodel.sqlmodels import SiteSQL, GenerationSQL

#
logger = logging.getLogger(__name__)


def format_pv_data(pv_system: SiteSQL, pv_yield_df: pd.DataFrame, session: Session) -> pd.DataFrame:
    """
    Format the pv data

    1. get rid of 0 bug
    2. remove data if already in our database

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
                f"{pv_system.client_site_id} "
                f"as last row is 0, but the second to last row is not."
            )
            pv_yield_df.drop(pv_yield_df.tail(1).index, inplace=True)

    # 2. filter by last
    last_pv_generation = (
        session.query(GenerationSQL)
        .join(SiteSQL)
        .filter(SiteSQL.site_uuid == pv_system.site_uuid)
        .order_by(GenerationSQL.created_utc.desc())
        .first()
    )

    if last_pv_generation is not None:
        last_pv_yield_datetime = last_pv_generation.start_utc.replace(tzinfo=timezone.utc)

        pv_yield_df = pv_yield_df[pv_yield_df["datetime_utc"] > last_pv_yield_datetime]

        if len(pv_yield_df) == 0:
            logger.debug(
                f"No new data available after {last_pv_yield_datetime}. "
                f"Last data point was {pv_yield_df.index.max()}"
            )
            logger.debug(pv_yield_df)
    else:
        logger.debug(
            f"This is the first lot pv yield data for pv system {(pv_system.client_site_id)}"
        )

    return pv_yield_df


class FakeDatabaseConnection:
    """Fake Database connection class"""

    def __init__(self):
        """
        Set up fake database connection

        This is so we can still do
        'with connection.get_session() as sessions:'
        bu session is None
        """

        class FakeSession:
            def __init__(self):  # noqa
                pass

            def __enter__(self):  # noqa
                return None

            def __exit__(self, type, value, traceback):  # noqa
                pass

        self.Session = FakeSession

    def get_session(self) -> Session:
        """Get sqlalamcy session"""
        return self.Session()


pv_output = "pvoutput.org"
solar_sheffield_passiv = "solar_sheffield_passiv"
