import contextlib
import os

import pandas as pd
import pytest
from pvsite_datamodel.connection import DatabaseConnection

# from nowcasting_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import Base, GenerationSQL, LocationSQL
from testcontainers.postgres import PostgresContainer

import pvconsumer


@contextlib.contextmanager
def postgresql_database(bases: list):
    """Reusable context manager to create a database with tables and return a connection.

    Automatically cleans up after itself.
    """
    with PostgresContainer("postgres:14.5") as postgres:
        url = postgres.get_connection_url()
        db_conn = DatabaseConnection(url, echo=False)
        engine = db_conn.engine
        for base in bases:
            base.metadata.create_all(engine)

        yield db_conn

        for base in bases:
            base.metadata.drop_all(db_conn.engine)

        engine.dispose()


@pytest.fixture
def db_connection(scope="session"):
    with postgresql_database([Base]) as db_conn:
        yield db_conn


@pytest.fixture(scope="function", autouse=True)
def db_session(db_connection):
    """Creates a new database session for a test."""

    connection = db_connection.engine.connect()
    # begin the nested transaction
    transaction = connection.begin()
    # use the connection with the already started transaction

    with db_connection.get_session() as s:
        yield s

        s.close()
        # roll back the broader transaction
        transaction.rollback()
        # put back the connection to the connection pool
        connection.close()
        s.flush()

    db_connection.engine.dispose()


@pytest.fixture
def filename():
    """Test data filename"""
    return os.path.dirname(pvconsumer.__file__) + "/../tests/data/pv_systems.csv"


@pytest.fixture
def filename_solar_sheffield():
    """Test data filename"""
    return os.path.dirname(pvconsumer.__file__) + "/../tests/data/pv_systems_solar_sheffield.csv"


@pytest.fixture()
def sites(db_session, filename, filename_solar_sheffield):
    """create some fake sites"""

    db_session.query(GenerationSQL).delete()
    db_session.query(LocationSQL).delete()

    sites = []
    sites_df = pd.read_csv(filename_solar_sheffield, index_col=0)
    client_site_ids = sites_df["pv_system_id"].values
    for i in range(0, len(client_site_ids)):
        site = LocationSQL(
            client_site_id=int(client_site_ids[i]),
            latitude=51,
            longitude=3,
            capacity_kw=4,
            ml_id=i,
        )

        db_session.add(site)
        db_session.commit()

        sites.append(site)

    sites_df = pd.read_csv(filename, index_col=0)
    client_site_ids = sites_df.index
    for i in range(0, len(client_site_ids)):
        site = LocationSQL(
            client_site_id=int(client_site_ids[i]),
            latitude=51,
            longitude=3,
            capacity_kw=4,
            ml_id=i + 100,
        )
        db_session.add(site)
        db_session.commit()

        sites.append(site)

    return sites
