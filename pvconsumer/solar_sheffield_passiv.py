""" Function to get data from Solar Shieffield about Pasiv systems"""


import logging
import os
from typing import List

import pandas as pd
import requests

logger = logging.getLogger(__name__)


url = os.getenv("SS_URL")
user_id = os.getenv("SS_USER_ID")
key = os.getenv("SS_KEY")


def raw_to_dataframe(response):
    """Reformat response data to dataframe"""

    lines = response.text.split("\n")
    columns = lines.pop(0).split(",")
    data = []
    for line in lines:
        data.append(line.split(","))

    return pd.DataFrame(data=data, columns=columns)


def get_all_systems_from_solar_sheffield(pv_system_ids: List[int] = None) -> pd.DataFrame:
    """
    Get the pv systesm from solar sheffield

    :param pv_system_ids: filter on pv system id
    :return:
    """
    logger.debug("Getting all pv systems")

    full_url = f"{url}owner_system_params_rounded?user_id={user_id}&key={key}"
    response = requests.get(full_url)
    assert response.status_code == 200, f"Cant get data from {url}owner_system_params_rounded"

    data_df = raw_to_dataframe(response=response)

    data_df.rename(columns={"system_id": "pv_system_id"}, inplace=True)
    data_df.rename(columns={"kWp": "capacity_kw"}, inplace=True)
    data_df.rename(columns={"longitude_rounded": "longitude"}, inplace=True)
    data_df.rename(columns={"latitude_rounded": "latitude"}, inplace=True)

    data_df["provider"] = "solar_sheffield_passiv"
    data_df["pv_system_id"] = data_df["pv_system_id"].astype(int)

    # format
    none_index = data_df["latitude"] == "None"
    logger.debug(f"Found {sum(none_index)} None values in latitude, going to drop")
    data_df = data_df[~none_index]
    data_df["latitude"] = data_df["latitude"].astype(float)
    data_df["longitude"] = data_df["longitude"].astype(float)

    # change any none strings to Nan, in orientation
    none_index = data_df["orientation"] == "None"
    data_df.loc[none_index, "orientation"] = "Nan"
    data_df["orientation"] = data_df["orientation"].astype(float)

    if pv_system_ids is not None:
        logger.debug(f"Filter for pv system ids {pv_system_ids}")
        data_df = data_df[data_df["pv_system_id"].isin(pv_system_ids)]
    # reformat

    return data_df


def get_all_latest_pv_yield_from_solar_sheffield() -> pd.DataFrame:
    """
    Get latest pv yields from solar sheffield

    This also pulls the pv systems and merges them

    :return:
    """

    logger.debug("Getting all pv yields")
    full_url = f"{url}reading_integrated_5mins?user_id={user_id}&key={key}"
    response = requests.get(full_url)
    assert response.status_code == 200, f"Cant get data from {url}reading_integrated_5mins"

    pv_yield_df = raw_to_dataframe(response=response)
    pv_yield_df["timestamp"] = pd.to_datetime(pv_yield_df["timestamp"])
    pv_yield_df["data"] = pv_yield_df["data"].astype(float)

    logger.debug("Getting all pv systems")
    # could get this from the database instead, but its so very quick here
    full_url = f"{url}owner_system_params_rounded?user_id={user_id}&key={key}"
    response = requests.get(full_url)
    assert response.status_code == 200, f"Cant get data from {url}owner_system_params_rounded"

    pv_system_df = raw_to_dataframe(response=response)

    data_df = pv_yield_df.merge(pv_system_df, left_on="ss_id", right_on="ss_id")

    data_df.rename(columns={"timestamp": "datetime_utc"}, inplace=True)

    # change from Watts hours to W
    data_df.loc[:, "solar_generation_w"] = data_df["data"] * 12
    # change from W hours to KW
    data_df.loc[:, "solar_generation_kw"] = data_df["solar_generation_w"] / 1000

    # add timestamp UTC
    data_df["datetime_utc"] = data_df["datetime_utc"].dt.tz_localize("UTC")

    # only take Passiv data
    data_df = data_df[data_df["owner_name"] == "Passiv"]

    return data_df
