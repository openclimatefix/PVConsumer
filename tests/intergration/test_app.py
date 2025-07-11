import pytest
from click.testing import CliRunner
from pvsite_datamodel.sqlmodels import GenerationSQL, LocationSQL

from pvconsumer.app import app, pull_data_and_save


@pytest.mark.skip("This test uses pvoutput.org which we not longer use")
def test_pull_data(db_session, sites):
    pull_data_and_save(pv_systems=sites, session=db_session, provider="pvoutput.org")

    pv_yields = db_session.query(GenerationSQL).all()
    assert len(pv_yields) > 0


def test_pull_data_solar_sheffield(db_session, sites):
    pull_data_and_save(pv_systems=sites, session=db_session, provider="solar_sheffield_passiv")

    pv_yields = db_session.query(GenerationSQL).all()
    assert len(pv_yields) > 0


@pytest.mark.skip("This test uses pvoutput.org which we not longer use")
def test_app(db_connection, filename, sites):
    runner = CliRunner()
    response = runner.invoke(
        app,
        [
            "--db-url",
            db_connection.url,
            "--filename",
            filename,
        ],
    )
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        pv_systems = session.query(LocationSQL).all()
        assert len(pv_systems) == 30

        pv_yields = session.query(GenerationSQL).all()
        assert len(pv_yields) > 7
        # the app gets multiple values for each pv system.
        # There is a chance this will fail in the early morning when no data is available


def test_app_ss(db_connection, filename_solar_sheffield, sites):
    runner = CliRunner()
    response = runner.invoke(
        app,
        [
            "--db-url",
            db_connection.url,
            "--filename",
            filename_solar_sheffield,
            "--provider",
            "solar_sheffield_passiv",
        ],
    )
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        pv_systems = session.query(LocationSQL).all()
        assert len(pv_systems) == 30
        # the app gets multiple values for each pv system.
        # There is a chance this will fail in the early morning when no data is available

        # make sure there valyes in the generation table too
        pv_yields = session.query(GenerationSQL).all()
        assert len(pv_yields) >= 8
