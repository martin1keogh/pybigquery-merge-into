import logging
import random
import string

import pytest
from sqlalchemy import create_engine
from sqlalchemy.sql.ddl import CreateSchema, DropSchema


@pytest.fixture(scope="session")
def engine():
    return create_engine("bigquery://", echo=True)


@pytest.fixture(scope="session")
def test_dataset(engine):
    dataset = "".join(random.choices(string.ascii_lowercase, k=10))

    with engine.connect() as conn:
        logging.info(f"Creating dataset {dataset}")
        conn.execute(CreateSchema(dataset))

        yield dataset

        logging.info(f"Dropping dataset {dataset}")
        conn.execute(DropSchema(dataset))


@pytest.fixture(scope="module")
def connection(engine):
    with engine.connect() as conn:
        yield conn
