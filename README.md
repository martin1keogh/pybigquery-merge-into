# Pybigquery MERGE INTO support

Disclaimer: this is just me playing around with SQLAlchemy's custom SQL feature.  
Use at your own risk, etc.

## Presentation

This repository (aims to) add the [MERGE INTO](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
feature to [SQLAlchemy](https://www.sqlalchemy.org)/[pybigquery](https://github.com/googleapis/python-bigquery-sqlalchemy).

## Usage

The main class is `pybigquery_merge_into.merge_clause.MergeInto()`.

### Example

```python
>>> query = MergeInto(
        target=target,
        source=source,
        onclause=target.c.t1 == source.c.s1,
        when_clauses=[
            WhenMatched(update(target).values({
                target.c.t2: source.c.s2 + timedelta(days=1).days
            })),
            WhenNotMatched(insert(target).values({
                target.c.t2: source.c.s2 + timedelta(days=1).days
            })),
            WhenNotMatchedBySource(
                delete(target), condition=target.c.t2 > date.today()
            ),
        ]
    )

>>> print(str(query.compile(dialect=BigQueryDialect())))
MERGE INTO `target`
USING `source`
ON `target`.`t1` = `source`.`s1`
WHEN MATCHED THEN 
	UPDATE  SET `t2`=(`source`.`s2` + :s2_1)
WHEN NOT MATCHED BY TARGET THEN 
	INSERT  (`t2`) VALUES ((`source`.`s2` + :s2_2))
WHEN NOT MATCHED BY SOURCE AND `target`.`t2` > :t2_1 THEN 
	DELETE
```

See the [tests](tests/unit/test_compile_examples.py#L37) for more examples.
 
## TODO

Write some documentation.
