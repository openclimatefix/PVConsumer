import os
from datetime import datetime, timezone
from typing import List

from click.testing import CliRunner
from nowcasting_datamodel.models.pv import PVSystem, PVSystemSQL, PVYield, PVYieldSQL

import pvconsumer
from pvconsumer.app import app, pull_data_and_save


def test_pull_data(db_session):

    pv_systems = [
        PVSystem(pv_system_id=10020, provider="pvoutput.org").to_orm(),
    ]

    pull_data_and_save(pv_systems=pv_systems, session=db_session)

    pv_yields = db_session.query(PVYieldSQL).all()
    assert len(pv_yields) > 0


def test_app(db_connection, filename):

    runner = CliRunner()
    response = runner.invoke(app, ["--db-url", db_connection.url, "--filename", filename])
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        pv_systems = session.query(PVSystemSQL).all()
        _ = PVSystem.from_orm(pv_systems[0])
        assert len(pv_systems) == 9

        pv_yields = session.query(PVYieldSQL).all()
        assert len(pv_yields) > 9
        # the app gets multiple values for each pv system.
        # There is a chance this will fail in the early morning when no data is available
