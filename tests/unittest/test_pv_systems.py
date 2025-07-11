import uuid
from datetime import datetime

import pandas as pd
from pvsite_datamodel.sqlmodels import GenerationSQL, LocationSQL

from pvconsumer.pv_systems import (
    filter_pv_systems_which_have_new_data,
    find_missing_pv_systems,
    load_pv_systems,
)
from pvconsumer.utils import solar_sheffield_passiv


def test_load_pv_systems():
    _ = load_pv_systems()


def test_load_pv_systems_passiv():
    _ = load_pv_systems(provider=solar_sheffield_passiv)


def test_find_missing_pv_systems():
    pv_systems_local = pd.DataFrame(
        [
            dict(pv_system_id=1, provider="pvoutput.org"),
            dict(pv_system_id=2, provider="pvoutput.org"),
            dict(pv_system_id=3, provider="pvoutput.org"),
        ]
    )

    pv_systems_db = pd.DataFrame(
        [
            dict(client_location_id=1, provider="pvoutput.org"),
        ]
    )

    pv_systems_missing = find_missing_pv_systems(
        pv_systems_local=pv_systems_local,
        pv_systems_db=pv_systems_db,
        provider="pvoutput.org",
    )

    assert len(pv_systems_missing) == 2


def test_filter_pv_systems_which_have_no_datal(db_session):
    pv_systems = [
        LocationSQL(location_uuid=uuid.uuid4()),
        LocationSQL(location_uuid=uuid.uuid4()),
        LocationSQL(location_uuid=uuid.uuid4()),
    ]

    pv_systems_keep = filter_pv_systems_which_have_new_data(
        pv_systems=pv_systems, session=db_session
    )

    assert len(pv_systems_keep) == 3


def test_filter_pv_systems_which_have_new_data(db_session, sites):
    pv_yield_0 = GenerationSQL(
        start_utc=datetime(2022, 1, 1), end_utc=datetime(2022, 1, 1, 0, 5), generation_power_kw=1
    )
    pv_yield_1 = GenerationSQL(
        start_utc=datetime(2022, 1, 1, 0, 4),
        end_utc=datetime(2022, 1, 1, 0, 5),
        generation_power_kw=3,
    )

    pv_yield_0.site = sites[0]
    pv_yield_1.site = sites[1]

    db_session.add_all([pv_yield_0, pv_yield_1])
    db_session.commit()

    assert len(sites) == 30

    #
    #   | last data  | keep?
    # 1 | 6 mins    | True
    # 2 | 2 mins    | False

    pv_systems_keep = filter_pv_systems_which_have_new_data(
        pv_systems=sites,
        session=db_session,
        datetime_utc=datetime(2022, 1, 1, 0, 6),
    )

    assert len(pv_systems_keep) == 29
    assert pv_systems_keep[0].location_uuid == sites[0].location_uuid
    assert pv_systems_keep[1].location_uuid == sites[2].location_uuid
