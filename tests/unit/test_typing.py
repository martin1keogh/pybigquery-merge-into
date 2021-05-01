from textwrap import dedent
from typing import Tuple

import pytest
from mypy import api
from sqlalchemy.sql import Delete, Insert, Update

from pybigquery_merge_into.merge_clause import WhenMatched, WhenNotMatched, WhenNotMatchedBySource


@pytest.mark.parametrize(["mypy_when_clause", "expected_error"], indirect=["mypy_when_clause"], argvalues=[
    ((WhenMatched, Insert), 'Argument 1 to "WhenMatched" has incompatible type "Insert"; expected "Union[Update, Delete]"'),
    ((WhenNotMatched, Update), 'Argument 1 to "WhenNotMatched" has incompatible type "Update"; expected "Insert"'),
    ((WhenNotMatched, Delete), 'Argument 1 to "WhenNotMatched" has incompatible type "Delete"; expected "Insert"'),
    ((WhenNotMatchedBySource, Insert), 'Argument 1 to "WhenNotMatchedBySource" has incompatible type "Insert"; expected "Union[Update, Delete]"'),
])
def test_type_error_on_when_clauses(mypy_when_clause, expected_error):
    assert expected_error in mypy_when_clause[0]


@pytest.mark.parametrize("mypy_when_clause", indirect=True, argvalues=[
    (WhenMatched, Update),
    (WhenMatched, Delete),
    (WhenNotMatched, Insert),
    (WhenNotMatchedBySource, Update),
    (WhenNotMatchedBySource, Delete),
])
def test_type_ok_on_when_clauses(mypy_when_clause: Tuple[str, str, int]):
    assert mypy_when_clause[2] == 0


@pytest.fixture
def mypy_when_clause(tmp_path, request) -> Tuple[str, str, int]:
    when_clause, action = request.param

    code = dedent(f"""\
        from sqlalchemy.sql.dml import {action.__name__}
        from pybigquery_merge_into.merge_clause import {when_clause.__name__}
        
        _: {action.__name__}
        {when_clause.__name__}(_)
    """)

    file = tmp_path / "t.py"
    file.write_text(code)

    return api.run([str(file.absolute())])
