""" PV system functions """
import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from pvoutput import PVOutput
from pvsite_datamodel.read import get_all_sites
from pvsite_datamodel.sqlmodels import GenerationSQL, LocationSQL
from sqlalchemy import func
from sqlalchemy.orm import Session

import pvconsumer
from pvconsumer.solar_sheffield_passiv import get_all_systems_from_solar_sheffield
from pvconsumer.utils import pv_output, solar_sheffield_passiv

# from pvconsumer.utils import df_to_list_pv_system, list_pv_system_to_df

logger = logging.getLogger(__name__)


def load_pv_systems(provider: str = pv_output, filename: Optional[str] = None) -> pd.DataFrame:
    """
    Load pv systems from file

    :param filename: filename to load
    :return: list of pv systems
    """

    if filename is None:
        if provider == pv_output:
            filename = os.path.dirname(pvconsumer.__file__) + "/data/pv_systems.csv"
        elif provider == solar_sheffield_passiv:
            filename = (
                os.path.dirname(pvconsumer.__file__) + "/data/pv_systems_solar_sheffield_passiv.csv"
            )

    logger.debug(f"Loading local pv systems from {filename}")

    pv_systems_df = pd.read_csv(filename, index_col=0)

    return pv_systems_df


def find_missing_pv_systems(
    pv_systems_local: pd.DataFrame,
    pv_systems_db: pd.DataFrame,
    provider: str,
) -> pd.DataFrame:
    """
    Find missing pv systems

    Gte the pv systems that are in local file, but not in the database
    Args:
        pv_systems_local: dataframe with "pv_system_id" from local file
        pv_systems_db: dataframe with "pv_system_id" from local db

    Returns: list of pv systems that are not in the database

    """

    logger.debug("Looking which pv systems are missing")

    if len(pv_systems_db) == 0:
        return pv_systems_local

    # get system ids
    if "pv_system_id" not in pv_systems_db.columns:
        pv_systems_db["pv_system_id"] = pv_systems_db["client_site_id"]
    pv_systems_db = pv_systems_db[["pv_system_id"]]
    pv_systems_local = pv_systems_local[["pv_system_id"]]

    # https://stackoverflow.com/questions/28901683/pandas-get-rows-which-are-not-in-other-dataframe
    # merge together
    df_all = pv_systems_local.merge(
        pv_systems_db.drop_duplicates(), on=["pv_system_id"], how="left", indicator=True
    )

    missing = df_all["_merge"] == "left_only"
    pv_systems_missing = df_all[missing].copy()
    pv_systems_missing["provider"] = provider

    return pv_systems_missing


def get_pv_systems(
    session: Session, provider: str, filename: Optional[str] = None
) -> List[LocationSQL]:
    """
    Get PV systems

    1. Load from database
    2. load from local
    3. add any pv systems not in database, by query pvoutput.org

    :param session: database sessions
    :param provider: provider name
    :param filename: filename for local pv systems
    :return: list of pv systems sqlalchemy objects
    """
    # load all pv systems in database

    pv_systems_sql_db: List[LocationSQL] = get_all_sites(session=session)

    # convert to sql objects to Pandas datafraome
    pv_systems_db_df = pd.DataFrame([pv_system.__dict__ for pv_system in pv_systems_sql_db])

    # load master file
    pv_system_local_df = load_pv_systems(filename=filename, provider=provider)

    # get missing pv systems
    missing_pv_system = find_missing_pv_systems(
        pv_systems_local=pv_system_local_df, pv_systems_db=pv_systems_db_df, provider=provider
    )
    logger.debug(missing_pv_system)

    logger.debug(f"There are {len(missing_pv_system)} pv systems to add to the database")

    if len(missing_pv_system) > 0:
        if provider == pv_output:
            # set up pv output.prg
            pv_output_data = PVOutput()
        elif provider == solar_sheffield_passiv:
            pv_systems = get_all_systems_from_solar_sheffield()
        else:
            raise Exception(f"Can not use provider {provider}")

        logger.debug(missing_pv_system)
        for i, pv_system in missing_pv_system.iterrows():
            logger.debug(pv_system)
            # get metadata
            if provider == pv_output:
                metadata = pv_output_data.get_metadata(
                    pv_system_id=pv_system.pv_system_id, use_data_service=True
                )
                logger.info(
                    f"For py system {pv_system.pv_system_id}, setting "
                    f"latitude {metadata.latitude}, "
                    f"longitude {metadata.longitude}, "
                    f"status_interval_minutes {metadata.status_interval_minutes}, "
                    f"This is the {i}th pv system out of {len(missing_pv_system)}"
                )
                pv_system.latitude = metadata.latitude
                pv_system.longitude = metadata.longitude
                pv_system.status_interval_minutes = int(metadata.status_interval_minutes)
                pv_system.capacity_kw = metadata.system_DC_capacity_W / 1000

            elif provider == solar_sheffield_passiv:
                pv_system = pv_systems[pv_systems["pv_system_id"] == pv_system.pv_system_id].iloc[0]
            else:
                raise Exception(f"Can not use provider {provider}")

            # get the current max ml id, small chance this could lead to a raise condition
            max_ml_id = session.query(func.max(LocationSQL.ml_id)).scalar()
            if max_ml_id is None:
                max_ml_id = 0

            site = LocationSQL(
                client_site_id=str(pv_system.pv_system_id),
                client_site_name=f"{provider}_{pv_system.pv_system_id}",
                latitude=pv_system.latitude,
                longitude=pv_system.longitude,
                capacity_kw=pv_system.capacity_kw,
                ml_id=max_ml_id + 1,
            )

            # add to database
            logger.debug(f"Adding pv system {pv_system.pv_system_id} to database")
            session.add(site)

            # The first time we do this, we might hit a rate limit of 900,
            # therefore its good to save this on the go
            session.commit()

    pv_systems_sql_db: List[LocationSQL] = get_all_sites(session=session)

    return pv_systems_sql_db


def filter_pv_systems_which_have_new_data(
    session: Session, pv_systems: List[LocationSQL], datetime_utc: Optional[datetime] = None
):
    """
    Filter pv systems which have new data available

    This is done by looking at the datestamp of last data pulled,
    add then by looking at the pv system refresh time, we can determine if new data is available

    sudo code:
        if last_datestamp + refresh_interval > datetime_now
            keep = True

    Args:
        pv_systems: list of pv systems
        datetime_utc: the datetime now
        session: database session

    Returns: list of pv systems that have new data.

    """

    logger.info(
        f"Looking at which PV systems might have new data. "
        f"Number of pv systems are {len(pv_systems)}"
    )

    if datetime_utc is None:
        datetime_utc = datetime.utcnow()  # add timezone

    site_uuids = [pv_system.location_uuid for pv_system in pv_systems]

    # pull the latest data from the database
    query = (
        session.query(LocationSQL.location_uuid, GenerationSQL.start_utc)
        .distinct(
            GenerationSQL.location_uuid,
            # GenerationSQL.start_utc,
        )
        .join(LocationSQL)
        .filter(
            GenerationSQL.start_utc <= datetime_utc,
            GenerationSQL.start_utc >= datetime_utc - timedelta(days=1),
            GenerationSQL.location_uuid.in_(site_uuids),
        )
        .order_by(
            GenerationSQL.location_uuid,
            GenerationSQL.start_utc,
            GenerationSQL.created_utc.desc(),
        )
    )
    last_generations = query.all()
    last_generations = {row[0]: row[1] for row in last_generations}

    keep_pv_systems = []
    for i, pv_system in enumerate(pv_systems):
        logger.debug(f"Looking at {i}th pv system, out of {len(pv_systems)} pv systems")

        if pv_system.location_uuid in last_generations:
            last_datetime = last_generations[pv_system.location_uuid]
        else:
            last_datetime = None

        if last_datetime is None:
            # there is no pv yield data for this pv system, so lets keep it
            logger.debug(
                f"There is no pv yield data for pv systems {pv_system.location_uuid}, "
                f"so will be getting data "
            )
            keep_pv_systems.append(pv_system)
        else:
            next_datetime_data_available = timedelta(minutes=5) + last_datetime
            logger.debug(next_datetime_data_available)
            if next_datetime_data_available < datetime_utc:
                logger.debug(
                    f"For pv system {pv_system.location_uuid} as "
                    f"last pv yield datetime is {last_datetime},"
                    f"refresh interval is 5 minutes, "
                    f"so will be getting data, {next_datetime_data_available=}"
                )
                keep_pv_systems.append(pv_system)
            else:
                logger.debug(
                    f"Not keeping pv system {pv_system.location_uuid} as "
                    f"last pv yield datetime is {last_datetime},"
                    f"refresh interval is 5 minutes"
                )

    return keep_pv_systems
