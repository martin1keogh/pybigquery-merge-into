import logging

from sqlalchemy import Column, Date, MetaData, String, Table

logger = logging.getLogger()
logger.setLevel(logging.INFO)

metadata = MetaData()

target = Table(
    "target",
    metadata,
    Column("t1", String),
    Column("t2", Date)
)

source = Table(
    "source",
    metadata,
    Column("s1", String),
    Column("s2", Date)
)
