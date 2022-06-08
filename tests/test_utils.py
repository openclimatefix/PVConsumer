from nowcasting_datamodel.fake import make_fake_pv_system
from nowcasting_datamodel.models.pv import PVSystem, PVYield
import pandas as pd

from datetime import datetime, timezone

from pvconsumer.utils import df_to_list_pv_system, list_pv_system_to_df, format_pv_data


def test_list_pv_system_to_df():
    pv_systems_1 = PVSystem.from_orm(make_fake_pv_system())
    pv_systems_2 = PVSystem.from_orm(make_fake_pv_system())

    _ = list_pv_system_to_df([pv_systems_1, pv_systems_2])


def test_df_to_list_pv_system():
    pv_systems_1 = PVSystem.from_orm(make_fake_pv_system())
    pv_systems_2 = PVSystem.from_orm(make_fake_pv_system())

    df = list_pv_system_to_df([pv_systems_1, pv_systems_2])
    _ = df_to_list_pv_system(df)


def test_pv_yield_df_no_data():

    pv_systems = [
        PVSystem(pv_system_id=10020, provider="pvoutput.org").to_orm(),
    ]
    pv_systems[0].last_pv_yield = None

    pv_yield_df = pd.DataFrame(columns=["instantaneous_power_gen_W", "datetime"])

    pv_yields = format_pv_data(pv_system=pv_systems[0], pv_yield_df=pv_yield_df)
    assert len(pv_yields) == 0


def test_pv_yield_df():
    pv_system = PVSystem(pv_system_id=10020, provider="pvoutput.org").to_orm()
    pv_system.last_pv_yield = None

    pv_yield_df = pd.DataFrame(
        columns=["instantaneous_power_gen_W", "datetime"], data=[[1, datetime(2022, 1, 1)]]
    )

    pv_yields = format_pv_data(pv_system=pv_system, pv_yield_df=pv_yield_df)
    assert len(pv_yields) == 1
    assert pv_yields[0].solar_generation_kw == 1 / 1000


def test_pv_yield_df_last_pv_yield():
    pv_system = PVSystem(pv_system_id=10020, provider="pvoutput.org").to_orm()
    last_pv_yield = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=10)

    pv_system.last_pv_yield = last_pv_yield

    pv_yield_df = pd.DataFrame(
        columns=["instantaneous_power_gen_W", "datetime"],
        data=[
            [1, datetime(2022, 1, 1, tzinfo=timezone.utc)],
            [2, datetime(2022, 1, 2, tzinfo=timezone.utc)],
        ],
    )

    pv_yields = format_pv_data(pv_system=pv_system, pv_yield_df=pv_yield_df)
    assert len(pv_yields) == 1
    assert pv_yields[0].solar_generation_kw == 2 / 1000


def test_pv_yield_df_0_bug():

    pv_system = PVSystem(pv_system_id=10020, provider="pvoutput.org").to_orm()
    pv_system.last_pv_yield = None

    pv_yield_df = pd.DataFrame(
        columns=["instantaneous_power_gen_W", "datetime"],
        data=[
            [1, datetime(2022, 1, 1, tzinfo=timezone.utc)],
            [0, datetime(2022, 1, 2, tzinfo=timezone.utc)],
        ],
    )

    pv_yields = format_pv_data(pv_system=pv_system, pv_yield_df=pv_yield_df)
    assert len(pv_yields) == 1
    assert pv_yields[0].solar_generation_kw == 1 / 1000


def test_pv_yield_df_zeros():

    pv_system = PVSystem(pv_system_id=10020, provider="pvoutput.org").to_orm()
    pv_system.last_pv_yield = None

    pv_yield_df = pd.DataFrame(
        columns=["instantaneous_power_gen_W", "datetime"],
        data=[
            [0, datetime(2022, 1, 1, tzinfo=timezone.utc)],
            [0, datetime(2022, 1, 2, tzinfo=timezone.utc)],
        ],
    )

    pv_yields = format_pv_data(pv_system=pv_system, pv_yield_df=pv_yield_df)
    assert len(pv_yields) == 2
    assert pv_yields[0].solar_generation_kw == 0
