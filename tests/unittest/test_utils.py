from datetime import datetime, timezone

import pandas as pd
from pvsite_datamodel.sqlmodels import GenerationSQL

from pvconsumer.utils import format_pv_data


def test_pv_yield_df_no_data(db_session, sites):
    pv_yield_df = pd.DataFrame(columns=["instantaneous_power_gen_W", "datetime"])

    pv_yields = format_pv_data(pv_system=sites[0], pv_yield_df=pv_yield_df, session=db_session)
    assert len(pv_yields) == 0


def test_pv_yield_df(sites, db_session):
    pv_yield_df = pd.DataFrame(
        columns=["instantaneous_power_gen_W", "datetime"], data=[[1, datetime(2022, 1, 1)]]
    )

    pv_yields = format_pv_data(pv_system=sites[0], pv_yield_df=pv_yield_df, session=db_session)
    assert len(pv_yields) == 1
    assert pv_yields.iloc[0].solar_generation_kw == 1 / 1000


def test_pv_yield_df_last_pv_yield(sites, db_session):
    last_pv_yield = GenerationSQL(
        start_utc=datetime(2022, 1, 1), end_utc=datetime(2022, 1, 1), generation_power_kw=10
    )
    last_pv_yield.site = sites[0]
    db_session.add(last_pv_yield)

    pv_yield_df = pd.DataFrame(
        columns=["instantaneous_power_gen_W", "datetime"],
        data=[
            [1, datetime(2022, 1, 1, tzinfo=timezone.utc)],
            [2, datetime(2022, 1, 2, tzinfo=timezone.utc)],
        ],
    )

    pv_yields = format_pv_data(pv_system=sites[0], pv_yield_df=pv_yield_df, session=db_session)
    assert len(pv_yields) == 1
    assert pv_yields.iloc[0].solar_generation_kw == 2 / 1000


#
def test_pv_yield_df_0_bug(sites, db_session):
    last_pv_yield = GenerationSQL(
        start_utc=datetime(2021, 1, 1), end_utc=datetime(2021, 1, 1), generation_power_kw=10
    )
    last_pv_yield.site = sites[0]
    db_session.add(last_pv_yield)

    pv_yield_df = pd.DataFrame(
        columns=["instantaneous_power_gen_W", "datetime"],
        data=[
            [1, datetime(2022, 1, 1, tzinfo=timezone.utc)],
            [0, datetime(2022, 1, 2, tzinfo=timezone.utc)],
        ],
    )

    pv_yields = format_pv_data(pv_system=sites[0], pv_yield_df=pv_yield_df, session=db_session)
    assert len(pv_yields) == 1
    assert pv_yields.iloc[0].solar_generation_kw == 1 / 1000


def test_pv_yield_df_zeros(sites, db_session):
    pv_yield_df = pd.DataFrame(
        columns=["instantaneous_power_gen_W", "datetime"],
        data=[
            [0, datetime(2022, 1, 1, tzinfo=timezone.utc)],
            [0, datetime(2022, 1, 2, tzinfo=timezone.utc)],
        ],
    )

    pv_yields = format_pv_data(pv_system=sites[0], pv_yield_df=pv_yield_df, session=db_session)
    assert len(pv_yields) == 2
    assert pv_yields.iloc[0].solar_generation_kw == 0
