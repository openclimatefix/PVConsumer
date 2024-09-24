from datetime import datetime, timedelta, timezone

import pytest

from pvconsumer.pv_systems import get_pv_systems
from pvconsumer.solar_sheffield_passiv import (
    get_all_latest_pv_yield_from_solar_sheffield,
    get_all_systems_from_solar_sheffield,
)
from pvconsumer.utils import solar_sheffield_passiv


def test_get_pv_systems_ss(db_session, filename):
    pv_systems = get_pv_systems(
        session=db_session, filename=filename, provider=solar_sheffield_passiv
    )

    assert len(pv_systems) > 0


def test_test_get_pv_systems_error(db_session, filename):
    with pytest.raises(Exception):
        _ = get_pv_systems(session=db_session, filename=filename, provider="fake")


def test_get_all_systems():
    pv_systems = get_all_systems_from_solar_sheffield()

    # these numbers seem to change over time
    assert len(pv_systems) >= 56824
    assert len(pv_systems) <= 57300
    assert pv_systems.iloc[0].capacity_kw is not None


def test_get_all_systems_filter():
    pv_systems = get_all_systems_from_solar_sheffield(pv_system_ids=[52, 64, 65])

    assert len(pv_systems) == 3


def test_get_all_latest_pv_yield():
    pv_yields = get_all_latest_pv_yield_from_solar_sheffield()

    assert len(pv_yields) > 0
    assert "datetime_utc" in pv_yields.columns
    assert "solar_generation_kw" in pv_yields.columns
    assert "system_id" in pv_yields.columns

    assert pv_yields["solar_generation_kw"].mean() >= 0
    assert pv_yields.iloc[0].datetime_utc <= datetime.now(tz=timezone.utc)
    assert pv_yields.iloc[0].datetime_utc >= datetime.now(tz=timezone.utc) - timedelta(minutes=10)
