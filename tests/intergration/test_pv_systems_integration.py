from datetime import datetime, timezone
from typing import List


from nowcasting_datamodel.models.pv import PVSystem, PVSystemSQL, PVYield, solar_sheffield_passiv
from nowcasting_datamodel.read.read_pv import get_latest_pv_yield

from pvconsumer.pv_systems import (
    filter_pv_systems_which_have_new_data,
    find_missing_pv_systems,
    get_pv_systems,
    load_pv_systems,
)


def test_get_pv_systems(db_session, filename):
    pv_systems = get_pv_systems(session=db_session, filename=filename, provider="pvoutput.org")

    assert len(pv_systems)
