from datetime import timedelta
from textwrap import dedent

import sqlparse
from pybigquery.sqlalchemy_bigquery import BigQueryDialect
from sqlalchemy import delete, insert, join, literal, select, text, update

from pybigquery_merge_into.merge_clause import MergeInto, WhenMatched, WhenNotMatched
from tests.conftest import detailed_inventory, inventory, new_arrivals, source, target, warehouse


def test_when_matched():
    query = MergeInto(
        target=target,
        source=source,
        onclause=target.c.t1 == source.c.s1,
        when_clauses=[
            WhenMatched(update(target).values({
                target.c.t2: source.c.s2 + timedelta(days=1).days
            }))
        ]
    )

    # sneaky double space in the last line (between UPDATE & SET)
    # due to how the UPDATE clause is transformed (direct string manipulation)
    expected = """\
        MERGE INTO `target`
        USING `source`
        ON `target`.`t1` = `source`.`s1`
        WHEN MATCHED THEN 
        \tUPDATE  SET `t2`=(`source`.`s2` + :s2_1)
        """

    assert str(query.compile(dialect=BigQueryDialect())) == dedent(expected)


def test_when_not_matched():
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

    expected = """\
        MERGE INTO `target`
        USING `source`
        ON `target`.`t1` = `source`.`s1`
        WHEN NOT MATCHED BY TARGET THEN 
        \tINSERT (`t2`) VALUES ((`source`.`s2` + :s2_1))
        """

    assert str(query.compile(dialect=BigQueryDialect())) == dedent(expected)


def test_cte_in_source():
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

    expected = """\
        MERGE INTO `target`
        USING (WITH `cte` AS 
        (SELECT `source`.`s1` AS `s1` 
        FROM `source`)
         SELECT `cte`.`s1` AS `s1` 
        FROM `cte`)
        ON `target`.`t1` = `s1`
        WHEN MATCHED THEN 
        \tUPDATE  SET `t1`=`s1`
        """

    assert str(query.compile(dialect=BigQueryDialect())) == dedent(expected)


# Check we can make the examples from https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_examples work
# Note that the "expected" value might have be slightly reworked (indentation, extra AS, etc) but should still be semantically the same.
# Known differences are:
#   - extra AS before aliases
#   - extra backticks (`) around identifiers
#   - slightly different indentation
#   - adds the INTO in "MERGE [INTO]"
#   - always specifies "WHEN NOT MATCH [BY TARGET]"
def test_compiles_example_1():
    expected = """\
        MERGE INTO `dataset.DetailedInventory` AS `T`
        USING `dataset.Inventory` AS `S`
        ON `T`.`product` = `S`.`product`
        WHEN NOT MATCHED BY TARGET AND `T`.`quantity` < 20 THEN 
        \tINSERT (`product`, `quantity`, `supply_constrained`, `comments`) VALUES (`S`.`product`, `S`.`quantity`, true, ARRAY<STRUCT<created DATE, comment STRING>>[(DATE('2016-01-01'), 'comment1')])
        WHEN NOT MATCHED BY TARGET THEN 
        \tINSERT (`product`, `quantity`, `supply_constrained`) VALUES (`S`.`product`, `S`.`quantity`, false)
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
                    T.c.comments: text("ARRAY<STRUCT<created DATE, comment STRING>>[(DATE('2016-01-01'), 'comment1')]")  # can't figure this one out without cheating a bit
                }),
                condition=T.c.quantity < 20
            ),
            WhenNotMatched(default_insert)
        ]
    )

    assert str(query.compile(dialect=BigQueryDialect(), compile_kwargs={'literal_binds': True})) == dedent(expected)


def test_example_2():
    expected = """\
        MERGE INTO `dataset.Inventory` AS `T`
        USING `dataset.NewArrivals` AS `S`
        ON `T`.`product` = `S`.`product`
        WHEN MATCHED THEN 
        \tUPDATE  SET `quantity`=(`T`.`quantity` + `S`.`quantity`)
        WHEN NOT MATCHED BY TARGET THEN 
        \tINSERT (`product`, `quantity`) VALUES (`S`.`product`, `S`.`quantity`)
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

    assert str(query.compile(dialect=BigQueryDialect(), compile_kwargs={'literal_binds': True})) == dedent(expected)


# Note that the subquery is different because querying `*` is annoying to do using SQLAlchemy
def test_example_3():
    expected = """\
        MERGE INTO `dataset.NewArrivals` AS `T`
        USING (SELECT `dataset.NewArrivals`.`product` AS `product` 
        FROM `dataset.NewArrivals` 
        WHERE `dataset.NewArrivals`.`warehouse` != 'warehouse #2') AS `S`
        ON `T`.`product` = `S`.`product`
        WHEN MATCHED AND `T`.`warehouse` = 'warehouse #1' THEN 
        \tUPDATE  SET `quantity`=(`T`.`quantity` + 20)
        WHEN MATCHED THEN 
        \tDELETE
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

    assert str(query.compile(dialect=BigQueryDialect(), compile_kwargs={'literal_binds': True})) == dedent(expected)


def test_example_6():
    expected = """\
        MERGE INTO dataset.Inventory AS T
        USING (SELECT t1.product AS product, t1.quantity AS quantity, t2.state AS state FROM dataset.NewArrivals AS t1 JOIN dataset.Warehouse AS t2 ON t1.warehouse = t2.warehouse) AS S
        ON T.product = S.product
        WHEN MATCHED AND S.state = 'CA' THEN 
        \tUPDATE SET quantity=(T.quantity + S.quantity)
        WHEN MATCHED THEN 
        \tDELETE
        """
    expected = sqlparse.format(dedent(expected), reindent=True)

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

    actual = str(query.compile(dialect=BigQueryDialect(), compile_kwargs={'literal_binds': True}))
    # no option to strip quotes from indentifers in sqlparse?
    actual = actual.replace("`", "")
    actual = sqlparse.format(actual, reindent=True)

    assert actual == expected
