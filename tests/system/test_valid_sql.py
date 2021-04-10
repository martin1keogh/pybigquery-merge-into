from datetime import date, timedelta

import pytest
from sqlalchemy import Column, Date, MetaData, String, Table, delete, insert, literal, select, update
from sqlalchemy.sql.ddl import CreateTable, DropTable

from pybigquery_merge_into.merge_clause import MergeInto, WhenMatched, WhenNotMatched, WhenNotMatchedBySource
from tests.conftest import logger

metadata = MetaData()


@pytest.fixture(scope="module")
def target(test_dataset, connection):
    table = Table(
        f"{test_dataset}.target",
        metadata,
        Column("t1", String),
        Column("t2", Date)
    )

    logger.info(f"Creating table {table}")
    connection.execute(CreateTable(table))

    yield table

    logger.info(f"Dropping table {table}")
    connection.execute(DropTable(table))


@pytest.fixture(scope="module")
def source(test_dataset, connection):
    table = Table(
        f"{test_dataset}.source",
        metadata,
        Column("s1", String),
        Column("s2", Date)
    )

    logger.info(f"Creating table {table}")
    connection.execute(CreateTable(table))

    yield table

    logger.info(f"Dropping table {table}")
    connection.execute(DropTable(table))


def test_when_matched(connection, target, source):
    query = MergeInto(
        target=target,
        source=source,
        onclause=target.c.t1 == source.c.s1,
        when_clauses=[
            WhenMatched(update(target).values({
                target.c.t2: source.c.s2 + timedelta(days=1).days
            })),
        ]
    )

    connection.execute(query)


def test_when_not_matched(connection, target, source):
    query = MergeInto(
        target=target,
        source=source,
        onclause=target.c.t1 == source.c.s1,
        when_clauses=[
            WhenNotMatched(insert(target).values({
                target.c.t2: source.c.s2 + timedelta(days=1).days
            })),
        ]
    )

    connection.execute(query)


def test_when_not_matched_by_source(connection, target, source):
    query = MergeInto(
        target=target,
        source=source,
        onclause=target.c.t1 == source.c.s1,
        when_clauses=[
            WhenNotMatchedBySource(delete(target)),
        ]
    )

    connection.execute(query)


def test_conditions(connection, target, source):
    query = MergeInto(
        target=target,
        source=source,
        onclause=target.c.t1 == source.c.s1,
        when_clauses=[
            WhenNotMatchedBySource(delete(target), condition=target.c.t2 > date.today()),
        ]
    )

    connection.execute(query)


def test_multiple_when_clauses(connection, target, source):
    query = MergeInto(
        target=target,
        source=source,
        onclause=target.c.t1 == source.c.s1,
        when_clauses=[
            WhenMatched(update(target).values({
                target.c.t2: source.c.s2 + timedelta(days=1).days
            })),
            WhenNotMatchedBySource(delete(target), condition=target.c.t2 > date.today()),
        ]
    )

    connection.execute(query)


def test_subquery_in_source(connection, target, source):
    sub = select([(source.c.s1 + "_sub").label("s3")]).alias("sub")

    query = MergeInto(
        target=target,
        source=sub,
        onclause=target.c.t1 == sub.c.s3,
        when_clauses=[
            WhenMatched(update(target).values({
                target.c.t1: sub.c.s3
            })),
            WhenNotMatchedBySource(delete(target)),
        ]
    )

    connection.execute(query)


def test_constant_onclause(connection, target, source):
    query = MergeInto(
        target=target,
        source=source,
        onclause=literal(False),
        when_clauses=[
            WhenNotMatchedBySource(delete(target)),
        ]
    )

    connection.execute(query)


def test_alias_on_target(connection, target, source):
    alias = target.alias("alias_t")
    query = MergeInto(
        target=alias,
        source=source,
        onclause=alias.c.t1 == source.c.s1,
        when_clauses=[
            WhenNotMatchedBySource(delete(alias), condition=alias.c.t2 > date.today()),
            WhenNotMatched(insert(alias).values(t1="dummy")),
            WhenMatched(update(alias).values(t2=date.today())),
        ]
    )

    connection.execute(query)


def test_alias_on_source(connection, target, source):
    alias = source.alias("alias_s")
    query = MergeInto(
        target=target,
        source=alias,
        onclause=target.c.t1 == alias.c.s1,
        when_clauses=[
            WhenNotMatchedBySource(delete(target)),
            WhenNotMatched(insert(target).values(t1=alias.c.s1)),
            WhenMatched(update(target).values(t2=alias.c.s2)),
        ]
    )

    connection.execute(query)


def test_cte_in_source(connection, target, source):
    cte = select([source.c.s1]).cte("cte")
    sub = select([cte.c.s1]).select_from(cte)

    query = MergeInto(
        target=target,
        source=sub,
        onclause=target.c.t1 == sub.c.s1,
        when_clauses=[
            WhenMatched(update(target).values({
                target.c.t1: sub.c.s1
            })),
        ]
    )

    connection.execute(query)


def test_update_shared_columns(connection, target, source):
    sub = select([source.c.s1.label("t1")]).alias("sub")

    query = MergeInto(
        target=target,
        source=sub,
        onclause=target.c.t1 == sub.c.t1,
        when_clauses=[
            WhenMatched(update(target).values({
                target.c.t1: sub.c.t1
            })),
        ]
    )

    connection.execute(query)
