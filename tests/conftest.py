import logging

from sqlalchemy import ARRAY, Boolean, Column, Date, Integer, JSON, MetaData, String, Table

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

# From https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_examples
inventory = Table(
    "dataset.Inventory",
    metadata,
    Column("product", String),
    Column("quantity", Integer),
    Column("supply_constrained", Boolean),
)

new_arrivals = Table(
    "dataset.NewArrivals",
    metadata,
    Column("product", String),
    Column("quantity", Integer),
    Column("warehouse", String),
)

warehouse = Table(
    "dataset.Warehouse",
    metadata,
    Column("warehouse", String),
    Column("state", String),
)

detailed_inventory = Table(
    "dataset.DetailedInventory",
    metadata,
    Column("product", String),
    Column("quantity", Integer),
    Column("supply_constrained", Boolean),
    Column("comments", ARRAY(JSON)),
    Column("specifications", JSON),
)
