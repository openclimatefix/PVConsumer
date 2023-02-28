from datetime import datetime, timezone
from typing import List

from nowcasting_datamodel.models.pv import PVSystem, PVSystemSQL, PVYield, solar_sheffield_passiv
from nowcasting_datamodel.read.read_pv import get_latest_pv_yield

from pvconsumer.pv_systems import (
    filter_pv_systems_which_have_new_data,
    find_missing_pv_systems,
    load_pv_systems,
)


def test_load_pv_systems():
    _ = load_pv_systems()


def test_load_pv_systems_passiv():
    _ = load_pv_systems(provider=solar_sheffield_passiv)


def test_find_missing_pv_systems():
    pv_systems_local = [
        PVSystem(pv_system_id=1, provider="pvoutput.org"),
        PVSystem(pv_system_id=2, provider="pvoutput.org"),
        PVSystem(pv_system_id=3, provider="pvoutput.org"),
    ]

    pv_systems_db = [
        PVSystem(pv_system_id=1, provider="pvoutput.org"),
    ]

    pv_systems_missing = find_missing_pv_systems(
        pv_systems_local=pv_systems_local,
        pv_systems_db=pv_systems_db,
        provider="pvoutput.org",
    )

    assert len(pv_systems_missing) == 2


def test_filter_pv_systems_which_have_new_data_no_refresh_interval(db_session):
    pv_systems = [
        PVSystem(pv_system_id=1, provider="pvoutput.org").to_orm(),
        PVSystem(pv_system_id=2, provider="pvoutput.org").to_orm(),
        PVSystem(pv_system_id=3, provider="pvoutput.org").to_orm(),
    ]

    pv_systems = get_latest_pv_yield(
        session=db_session, pv_systems=pv_systems, append_to_pv_systems=True
    )

    pv_systems_keep = filter_pv_systems_which_have_new_data(pv_systems=pv_systems)

    assert len(pv_systems_keep) == 3


def test_filter_pv_systems_which_have_new_data_no_data(db_session):
    pv_systems = [
        PVSystem(pv_system_id=1, provider="pvoutput.org", status_interval_minutes=5).to_orm(),
        PVSystem(pv_system_id=2, provider="pvoutput.org", status_interval_minutes=5).to_orm(),
        PVSystem(pv_system_id=3, provider="pvoutput.org", status_interval_minutes=5).to_orm(),
    ]

    pv_systems = get_latest_pv_yield(
        session=db_session, pv_systems=pv_systems, append_to_pv_systems=True
    )

    pv_systems_keep = filter_pv_systems_which_have_new_data(pv_systems=pv_systems)

    assert len(pv_systems_keep) == 3


def test_filter_pv_systems_which_have_new_data(db_session):
    pv_yield_0 = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=1).to_orm()
    pv_yield_1 = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2).to_orm()
    pv_yield_2 = PVYield(datetime_utc=datetime(2022, 1, 1, 0, 4), solar_generation_kw=3).to_orm()

    pv_systems = [
        PVSystem(pv_system_id=1, provider="pvoutput.org", status_interval_minutes=4).to_orm(),
        PVSystem(pv_system_id=2, provider="pvoutput.org", status_interval_minutes=1).to_orm(),
        PVSystem(pv_system_id=3, provider="pvoutput.org", status_interval_minutes=5).to_orm(),
    ]

    pv_yield_0.pv_system = pv_systems[0]
    pv_yield_1.pv_system = pv_systems[1]
    pv_yield_2.pv_system = pv_systems[2]

    db_session.add_all([pv_yield_0, pv_yield_1, pv_yield_2])
    db_session.add_all(pv_systems)

    pv_systems: List[PVSystemSQL] = db_session.query(PVSystemSQL).all()
    pv_systems = get_latest_pv_yield(
        session=db_session, pv_systems=pv_systems, append_to_pv_systems=True
    )

    #
    #   | last data | refresh | keep?
    # 1 | 5 mins    | 4 mins  | True
    # 2 | 5 mins    | 1 mins  | True
    # 3 | 1 mins    | 5 mins  | False

    pv_systems_keep = filter_pv_systems_which_have_new_data(
        pv_systems=pv_systems,
        datetime_utc=datetime(2022, 1, 1, 0, 5, tzinfo=timezone.utc),
    )

    assert len(pv_systems_keep) == 2
    assert pv_systems_keep[0].id == 1
    assert pv_systems_keep[1].id == 2
