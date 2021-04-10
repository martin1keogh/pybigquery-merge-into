from datetime import timedelta
from textwrap import dedent

from pybigquery.sqlalchemy_bigquery import BigQueryDialect
from sqlalchemy import insert, update

from pybigquery_merge_into.merge_clause import MergeInto, WhenMatched, WhenNotMatched
from tests.conftest import source, target


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
        \tINSERT  (`t2`) VALUES ((`source`.`s2` + :s2_1))
        """

    assert str(query.compile(dialect=BigQueryDialect())) == dedent(expected)
