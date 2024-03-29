from abc import abstractmethod
from textwrap import dedent
from typing import Generic, List, NoReturn, Optional, TypeVar, Union

from sqlalchemy import Table
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ClauseElement, ColumnElement, Subquery
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.dml import Delete, Insert, Update
from sqlalchemy.sql.selectable import SelectBase

_Ops = Union[Insert, Update, Delete]
T = TypeVar("T", bound=_Ops)


class _WhenClause(ClauseElement, Generic[T]):
    def __init__(
            self,
            action: T,
            condition: Optional[ColumnElement] = None
    ):
        self.action = action
        self.condition = condition

    @classmethod
    @abstractmethod
    # shitty name, FIXME
    def when_type(cls) -> str:
        pass


@compiles(_WhenClause, "bigquery")
def compile_when_clause(element: _WhenClause[_Ops], compiler: SQLCompiler, **kwargs):
    text = element.when_type()

    if element.condition is not None:
        text += " AND {}".format(compiler.process(element.condition, **kwargs))

    # The when_clause specs are ever so slightly different from the classic UPDATE/INSERT/DELETE clauses, so this sucks
    if isinstance(element.action, Delete):
        action_text = "DELETE"  # this one's alright though

    elif isinstance(element.action, Update):
        action_text = compiler.process(element.action, **kwargs)
        # remove the `<table>` from `UPDATE <table> SET`
        action_text = action_text.replace(compiler.process(element.action.table, asfrom=True), "", 1)

    elif isinstance(element.action, Insert):
        if not (element.action._values or element.action._ordered_values):  # type: ignore
            action_text = "INSERT ROW"
        else:
            action_text = compiler.process(element.action, **kwargs)
            # remove the `INTO <table>` from `INSERT INTO <table> (...) VALUES`, handling the potential aliasing
            action_text = action_text.replace(f"INTO `{element.action.table.name}` ", "", 1)

    else:
        _: NoReturn = element.action
        raise AssertionError(f"Invalid value: {element.action !r}")

    text += " THEN \n\t{}\n".format(action_text)

    return text


class WhenMatched(_WhenClause[Union[Update, Delete]]):
    @classmethod
    def when_type(cls) -> str:
        return "WHEN MATCHED"


class WhenNotMatched(_WhenClause[Insert]):
    @classmethod
    def when_type(cls) -> str:
        return "WHEN NOT MATCHED BY TARGET"


class WhenNotMatchedBySource(_WhenClause[Union[Update, Delete]]):
    @classmethod
    def when_type(cls) -> str:
        return "WHEN NOT MATCHED BY SOURCE"


class MergeInto(Executable, ClauseElement):
    def __init__(
            self,
            target: Table,
            source: Union[Table, Subquery],
            onclause: ColumnElement,
            when_clauses: List[_WhenClause]
    ):
        """
        :param target: Table to be updated
        :param source: Origin of the new data. Must be either a table or a subquery
        :param onclause: SQLAlchemy condition, will be used to match the data between tables
        :param when_clauses: List of [WhenMatched, WhenNotMatched, WhenNotMatchedBySource] instances
        """
        assert when_clauses, "An MERGE INTO statement requires at least one `when_clause`"
        assert not isinstance(source, SelectBase), "A source should not be a Selectable. If you intend to pass a subquery " \
                                                   "please call .subquery() before calling this method. See " \
                                                   "https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#a-select-statement-is-no-longer-implicitly-considered-to-be-a-from-clause " \
                                                   "for more information."

        super().__init__()
        self.target = target
        self.source = source
        self.onclause = onclause
        self.when_clauses = when_clauses


@compiles(MergeInto, "bigquery")
def compile_merge_into(element: MergeInto, compiler: SQLCompiler, **kwargs):
    base_template = dedent("""\
        MERGE INTO {target}
        USING {source}
        ON {cond}
    """)

    # Compile the INSERT/UPDATE/DELETE parts first.
    # This is because CTEs in the `source` value (and elsewhere, but there really shouldn't
    # ever be a CTE in `target` and/or `cond`) would appear in the INSERT/UPDATE (SQLAlchemy
    # sees a CTE in the context, so it pushes it to the top of the INSERT/UPDATE part).
    # This feels pretty hackish, but it works well for now.
    # Another solution might be to process the `when_clauses` in a copy() of the compiler
    # with the .ctes emptied, ie in a "blank" state.
    actions = []
    for when_clause in element.when_clauses:
        actions.append(compiler.process(when_clause, **kwargs))

    query = base_template.format(
        target=compiler.process(element.target, asfrom=True, **kwargs),
        source=compiler.process(element.source, asfrom=True, **kwargs),
        cond=compiler.process(element.onclause, **kwargs),
    )

    query += "".join(actions)

    # deactivate all "fetch PK" or "implicit-returning" features
    # XXX should we set isdelete = False too?
    compiler.isinsert = compiler.isupdate = False

    return dedent(query)
