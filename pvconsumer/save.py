""" Save to database functions"""
import logging

import pandas as pd
from pvsite_datamodel.write.generation import insert_generation_values
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def save_to_pv_site_database(session: Session, pv_yield_df: pd.DataFrame):
    """

    Save to pv site database

    :param session: pv site databse sessions
    :param pv_system: one pv system
    :param pv_yield_df: pandas datafram of generation values
    :return:
    """

    logger.debug(f"Saving {len(pv_yield_df)} generation values to pv sites database")
    pv_yield_df = pv_yield_df.copy()

    if len(pv_yield_df) == 0:
        return

    # format dataframe
    if "instantaneous_power_gen_W" in pv_yield_df.columns:
        pv_yield_df["solar_generation_kw"] = pv_yield_df["instantaneous_power_gen_W"] / 1000
    if "datetime_utc" not in pv_yield_df.columns:
        pv_yield_df.rename(
            columns={
                "datetime": "datetime_utc",
            },
            inplace=True,
        )

    pv_yield_df["power_kw"] = pv_yield_df["solar_generation_kw"]
    pv_yield_df["end_utc"] = pv_yield_df["datetime_utc"]
    # TODO this is hard coded for Sheffield Solar Passiv
    pv_yield_df["start_utc"] = pv_yield_df["datetime_utc"] - pd.Timedelta("5T")

    # save to database
    logger.debug(f"Inserting {len(pv_yield_df)} records to pv site database")
    insert_generation_values(session, pv_yield_df)
    session.commit()
