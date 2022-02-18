import inspect
import os
import pickle
from functools import partial

import pytest

from pvoutput import mapscraper as ms

import os

import pytest
from nowcasting_datamodel.connection import Base_PV, DatabaseConnection


@pytest.fixture
def db_connection():

    url = os.getenv("DB_URL_PV", "sqlite:///test.db")

    connection = DatabaseConnection(url=url, base=Base_PV, echo=False)
    Base_PV.metadata.create_all(connection.engine)

    yield connection

    Base_PV.metadata.drop_all(connection.engine)


@pytest.fixture(scope="function", autouse=True)
def db_session(db_connection):
    """Creates a new database session for a test."""

    with db_connection.get_session() as s:
        s.begin()
        yield s
        s.rollback()



@pytest.fixture
def data_dir():
    # Taken from http://stackoverflow.com/a/6098238/732596
    data_dir = os.path.dirname(inspect.getfile(inspect.currentframe()))
    data_dir = os.path.abspath(data_dir)
    assert os.path.isdir(data_dir), data_dir + " does not exist."
    return data_dir


def get_cleaned_test_soup(data_dir):
    test_soup_filepath = os.path.join(data_dir, "data/mapscraper_soup.pickle")
    with open(test_soup_filepath, "rb") as f:
        test_soup = pickle.load(f)
    return ms.clean_soup(test_soup)


@pytest.fixture()
def get_test_dict_of_dfs(data_dir):
    dict_filepath = os.path.join(data_dir, "data/mapscraper_dict_of_dfs.pickle")
    with open(dict_filepath, "rb") as f:
        test_soup = pickle.load(f)
    return test_soup


@pytest.fixture()
def get_function_dict(data_dir):
    # using partials so functions only get executed when needed
    soup = get_cleaned_test_soup(data_dir)
    df = ms._process_system_size_col(soup)
    index = df.index
    keys = get_keys_for_dict()
    functions = (
        partial(ms._process_system_size_col, soup),
        partial(ms._process_output_col, soup, index),
        partial(ms._process_generation_and_average_cols, soup, index),
        partial(ms._process_efficiency_col, soup, index),
        partial(ms._process_metadata, soup),
    )
    function_dict = dict(zip(keys, functions))
    return function_dict


def get_keys_for_dict():
    keys = (
        "pv_system_size_metadata",
        "process_output_col",
        "process_generation_and_average_cols",
        "process_efficiency_col",
        "process_metadata",
    )
    return keys
