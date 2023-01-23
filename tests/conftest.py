import inspect
import os
import pickle
from functools import partial
import uuid

from datetime import datetime, timezone

import pytest
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast, Base_PV

from pvsite_datamodel.sqlmodels import Base, SiteSQL, ClientSQL, GenerationSQL

import pvconsumer


@pytest.fixture
def db_connection():

    url = os.getenv("DB_URL", "sqlite:///test.db")

    connection = DatabaseConnection(url=url, base=Base_PV, echo=False)
    Base_PV.metadata.create_all(connection.engine)
    Base.metadata.create_all(connection.engine)

    yield connection

    Base_PV.metadata.drop_all(connection.engine)
    Base.metadata.create_all(connection.engine)


@pytest.fixture(scope="session")
def db_connection_forecast():

    url = os.getenv("DB_URL", "sqlite:///test.db")

    connection = DatabaseConnection(url=url, base=Base_Forecast, echo=False)
    Base_Forecast.metadata.create_all(connection.engine)

    yield connection

    Base_Forecast.metadata.drop_all(connection.engine)


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
def sites(db_session):
    """create some fake sites"""

    db_session.query(GenerationSQL).delete()
    db_session.query(SiteSQL).delete()
    db_session.query(ClientSQL).delete()

    sites = []
    client_site_ids = [4383, 4777, 4531, 4569, 4789, 4813, 4834, 4837, 4872]
    for i in range(0, len(client_site_ids)):
        client = ClientSQL(
            client_uuid=uuid.uuid4(),
            client_name="solar_sheffield_passiv",
            created_utc=datetime.now(timezone.utc),
        )
        site = SiteSQL(
            site_uuid=uuid.uuid4(),
            client_uuid=client.client_uuid,
            client_site_id=client_site_ids[i],
            latitude=51,
            longitude=3,
            capacity_kw=4,
            created_utc=datetime.now(timezone.utc),
            updated_utc=datetime.now(timezone.utc),
            ml_id=i,
        )
        db_session.add(client)
        db_session.add(site)
        db_session.commit()

        sites.append(site)

    client_site_ids =[10041,10020,4531, 4532]
    for i in range(0, 4):
        client = ClientSQL(
            client_uuid=uuid.uuid4(),
            client_name="pvoutput.org",
            created_utc=datetime.now(timezone.utc),
        )
        site = SiteSQL(
            site_uuid=uuid.uuid4(),
            client_uuid=client.client_uuid,
            client_site_id=client_site_ids[i],
            latitude=51,
            longitude=3,
            capacity_kw=4,
            created_utc=datetime.now(timezone.utc),
            updated_utc=datetime.now(timezone.utc),
            ml_id=i+100,
        )
        db_session.add(client)
        db_session.add(site)
        db_session.commit()

        sites.append(site)

    return sites
    