"""
Get the pv installed capacoty values and add them to the database

1. load pv data from pv stats
2. get maximum for pv system
3. load and update pv system from database

"""

import json

import boto3
import fsspec
import pandas as pd
import xarray
import xarray as xr
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_PV
from nowcasting_datamodel.models.pv import PVSystemSQL

client = boto3.client("secretsmanager")
response = client.get_secret_value(
    SecretId="development/rds/pv/",
)
secret = json.loads(response["SecretString"])
""" We have used a ssh tunnel to 'localhost' """
db_url = f'postgresql://{secret["username"]}:{secret["password"]}@localhost:5432/{secret["dbname"]}'

# 1, load data

filename = "PV_timeseries_batch.nc"
pv = xr.open_dataset(filename, engine="h5netcdf")

pv_df = pv.to_dataframe()

# 2.Assume is the capacity
m = pv_df.max()

# get pv systems from database and update
connection = DatabaseConnection(url=db_url, base=Base_PV, echo=True)
with connection.get_session() as session:

    pv_systems = session.query(PVSystemSQL).all()

    for pv_system in pv_systems:
        pv_system_id = pv_system.pv_system_id
        print(pv_system_id)

        installed_capacity_kw = float(m.loc[str(pv_system.pv_system_id)]) / 1000
        print(installed_capacity_kw)
        pv_system.installed_capacity_kw = installed_capacity_kw

    session.commit()
