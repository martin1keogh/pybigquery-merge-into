from textwrap import dedent

import pytest
import sqlparse
from pybigquery.sqlalchemy_bigquery import BigQueryDialect
from sqlalchemy import delete, func, insert, join, literal, select, text, update

from pybigquery_merge_into.merge_clause import MergeInto, WhenMatched, WhenNotMatched, WhenNotMatchedBySource
from tests.conftest import detailed_inventory, inventory, new_arrivals, warehouse


# Check we can make the examples from https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_examples work
# Note that the "expected" value might have be slightly reworked (indentation, extra AS, etc) but should still be semantically the same.
# Known differences are:
#   - always using fully qualified table/column names (added to the expected SQL query)
#   - extra AS before aliases (added to the expected SQL query)
#   - extra backticks (`) around identifiers (removed in this function)
#   - slightly different indentation (removed by sqlparse.format()-ing both queries)
#   - adds the INTO in "MERGE [INTO]" (added to the expected SQL query)
#   - always specifies "WHEN NOT MATCH [BY TARGET]" (added to the expected SQL query)
#   - boolean constants are written in lowercase (changed in the expected SQL query)
@pytest.fixture(autouse=True)
def _(request):
    # run the test, get the SQLAlchemy query & the expected SQL output
    query, expected = request.function()

    # Reformat the SQL to get rid of some of the differences between formats
    expected = sqlparse.format(dedent(expected), reindent=True)

    # Get the actual string-output of the SQLAlchemy query & reformat it to
    actual = str(query.compile(dialect=BigQueryDialect(), compile_kwargs={'literal_binds': True}))
    actual = actual.replace("`", "")
    actual = sqlparse.format(actual, reindent=True)

    assert actual == expected


def test_example_1():
    expected = """\
        MERGE INTO dataset.DetailedInventory AS T
        USING dataset.Inventory AS S
        ON T.product = S.product
        WHEN NOT MATCHED BY TARGET AND T.quantity < 20 THEN 
            INSERT (product, quantity, supply_constrained, comments)
            VALUES (S.product, S.quantity, true, ARRAY<STRUCT<created DATE, comment STRING>>[(DATE('2016-01-01'), 'comment1')])
        WHEN NOT MATCHED BY TARGET THEN 
            INSERT (product, quantity, supply_constrained)
            VALUES (S.product, S.quantity, false)
        """

    T = detailed_inventory.alias("T")
    S = inventory.alias("S")

    # without a meaningful variable name this is more confusing than anything really,
    # but it lets me check this sort of factoring out works too.
    default_insert = insert(T).values({
        T.c.product: S.c.product,
        T.c.quantity: S.c.quantity,
        T.c.supply_constrained: literal(False),
    })

    query = MergeInto(
        target=T,
        source=S,
        onclause=T.c.product == S.c.product,
        when_clauses=[
            WhenNotMatched(
                default_insert.values({
                    T.c.supply_constrained: literal(True),
                    # can't figure this one out without cheating a bit
                    T.c.comments: text("ARRAY<STRUCT<created DATE, comment STRING>>[(DATE('2016-01-01'), 'comment1')]")
                }),
                condition=T.c.quantity < 20
            ),
            WhenNotMatched(default_insert)
        ]
    )

    return query, expected


def test_example_2():
    expected = """\
        MERGE INTO dataset.Inventory AS T
        USING dataset.NewArrivals AS S
        ON T.product = S.product
        WHEN MATCHED THEN 
            UPDATE  SET quantity=(T.quantity + S.quantity)
        WHEN NOT MATCHED BY TARGET THEN 
            INSERT (product, quantity) VALUES (S.product, S.quantity)
        """

    T = inventory.alias("T")
    S = new_arrivals.alias("S")

    query = MergeInto(
        target=T,
        source=S,
        onclause=T.c.product == S.c.product,
        when_clauses=[
            WhenMatched(
                update(T).values({
                    T.c.quantity: T.c.quantity + S.c.quantity,
                }),
            ),
            WhenNotMatched(
                insert(T).values({
                    T.c.product: S.c.product,
                    T.c.quantity: S.c.quantity,
                }),
            )
        ]
    )

    return query, expected


# Note that the subquery is different because querying `*` is annoying to do using SQLAlchemy
def test_example_3():
    expected = """\
        MERGE INTO dataset.NewArrivals AS T
        USING (SELECT dataset.NewArrivals.product AS product FROM dataset.NewArrivals WHERE dataset.NewArrivals.warehouse != 'warehouse #2') AS S
        ON T.product = S.product
        WHEN MATCHED AND T.warehouse = 'warehouse #1' THEN 
            UPDATE  SET quantity=(T.quantity + 20)
        WHEN MATCHED THEN 
            DELETE
        """

    T = new_arrivals.alias("T")
    S = select([new_arrivals.c.product]).where(new_arrivals.c.warehouse != "warehouse #2").alias("S")

    query = MergeInto(
        target=T,
        source=S,
        onclause=T.c.product == S.c.product,
        when_clauses=[
            WhenMatched(
                update(T).values({
                    T.c.quantity: T.c.quantity + 20,
                }),
                condition=T.c.warehouse == "warehouse #1"
            ),
            WhenMatched(
                delete(T)
            )
        ]
    )

    return query, expected


def test_example_6():
    expected = """\
        MERGE INTO dataset.Inventory AS T
        USING (SELECT t1.product AS product, t1.quantity AS quantity, t2.state AS state FROM dataset.NewArrivals AS t1 JOIN dataset.Warehouse AS t2 ON t1.warehouse = t2.warehouse) AS S
        ON T.product = S.product
        WHEN MATCHED AND S.state = 'CA' THEN 
            UPDATE SET quantity=(T.quantity + S.quantity)
        WHEN MATCHED THEN 
            DELETE
        """

    T = inventory.alias("T")
    NA = new_arrivals.alias("t1")
    W = warehouse.alias("t2")
    S = select([NA.c.product, NA.c.quantity, W.c.state]).select_from(join(NA, W, NA.c.warehouse == W.c.warehouse)).alias("S")

    query = MergeInto(
        target=T,
        source=S,
        onclause=T.c.product == S.c.product,
        when_clauses=[
            WhenMatched(
                update(T).values({
                    T.c.quantity: T.c.quantity + S.c.quantity,
                }),
                condition=S.c.state == "CA"
            ),
            WhenMatched(
                delete(T)
            )
        ]
    )

    return query, expected


def test_example_8():
    expected = """\
        MERGE INTO dataset.NewArrivals
        USING (SELECT * FROM UNNEST([('microwave', 10, 'warehouse #1'), ('dryer', 30, 'warehouse #1'), ('oven', 20, 'warehouse #2')])) AS anon_1
        ON false
        WHEN NOT MATCHED BY TARGET THEN
          INSERT ROW
        WHEN NOT MATCHED BY SOURCE THEN
          DELETE
        """

    values = [
        str(("microwave", 10, "warehouse #1")),
        str(("dryer", 30, "warehouse #1")),
        str(("oven", 20, "warehouse #2")),
    ]
    values = ", ".join(values)

    T = new_arrivals
    S = select(["*"]).select_from(func.UNNEST(text(f"[{values}]"))).subquery()  # can't get `func.UNNEST(values)` to work, renders as `UNNEST(NULL)`?

    query = MergeInto(
        target=T,
        source=S,
        onclause=literal(False),
        when_clauses=[
            WhenNotMatched(insert(T)),
            WhenNotMatchedBySource(delete(T))
        ]
    )

    return query, expected
