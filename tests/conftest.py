import inspect
import os
import pickle
from functools import partial

import pytest
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast, Base_PV

import pvconsumer


@pytest.fixture
def db_connection():

    url = os.getenv("DB_URL", "sqlite:///test.db")

    connection = DatabaseConnection(url=url, base=Base_PV, echo=False)
    Base_PV.metadata.create_all(connection.engine)

    yield connection

    Base_PV.metadata.drop_all(connection.engine)


@pytest.fixture
def db_connection_forecast():

    url = os.getenv("DB_URL", "sqlite:///test.db")

    connection = DatabaseConnection(url=url, base=Base_Forecast, echo=False)
    Base_Forecast.metadata.create_all(connection.engine)

    yield connection

    Base_Forecast.metadata.drop_all(connection.engine)


@pytest.fixture(scope="function", autouse=True)
def db_session(db_connection):
    """Creates a new database session for a test."""

    with db_connection.get_session() as s:
        s.begin()
        yield s
        s.rollback()


@pytest.fixture
def filename():
    """Test data filename"""
    return os.path.dirname(pvconsumer.__file__) + "/../tests/data/pv_systems.csv"


@pytest.fixture
def filename_solar_sheffield():
    """Test data filename"""
    return os.path.dirname(pvconsumer.__file__) + "/../tests/data/pv_systems_solar_sheffield.csv"
