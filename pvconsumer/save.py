""" Save to database functions"""
import logging
from typing import List

import pandas as pd
from nowcasting_datamodel.models import PVSystem, PVYield
from pvsite_datamodel.read.site import get_site_by_client_site_id
from pvsite_datamodel.write.generation import insert_generation_values
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def save_to_database(session: Session, pv_yields: List[PVYield]):
    """
    Save pv data to database

    :param session: database session
    :param pv_yields: list of pv data
    """
    logger.debug(f"Will be adding {len(pv_yields)} pv yield object to database")

    session.add_all(pv_yields)
    session.commit()


def save_to_pv_site_database(session: Session, pv_system: PVSystem, pv_yield_df: pd.DataFrame):
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

    # get site from the pv_system
    site = get_site_by_client_site_id(
        session=session,
        client_name=pv_system.provider,
        client_site_id=pv_system.pv_system_id,
    )

    # format dataframe
    pv_yield_df["site_uuid"] = site.site_uuid
    pv_yield_df["power_kw"] = pv_yield_df["solar_generation_kw"]
    pv_yield_df["end_utc"] = pv_yield_df["datetime_utc"]
    # TODO this is hard coded for Sheffield Solar Passiv
    pv_yield_df["start_utc"] = pv_yield_df["datetime_utc"] - pd.Timedelta("5T")

    # save to database
    logger.debug(f"Inserting {len(pv_yield_df)} records to pv site database")
    insert_generation_values(session, pv_yield_df)
