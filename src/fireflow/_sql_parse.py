"""A simple SELECT statement parser

Adapted from:
https://github.com/pyparsing/pyparsing/blob/14def7d7f454bfe891589a67801854b12e0b5ed2/examples/select_parser.py
Taken from SQLite's SELECT statement
definition at https://www.sqlite.org/lang_select.html
"""
from functools import lru_cache
import typing as t

import pyparsing as pp

if t.TYPE_CHECKING:
    from sqlalchemy import ColumnElement

    from .orm import Base

pp.ParserElement.enablePackrat()

KWD_TYPE = t.Literal[
    "UNION",
    "ALL",
    "AND",
    "INTERSECT",
    "EXCEPT",
    "COLLATE",
    "ASC",
    "DESC",
    "ON",
    "USING",
    "NATURAL",
    "INNER",
    "CROSS",
    "LEFT",
    "OUTER",
    "JOIN",
    "AS",
    "INDEXED",
    "NOT",
    "SELECT",
    "DISTINCT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "LIMIT",
    "OFFSET",
    "OR",
    "CAST",
    "ISNULL",
    "NOTNULL",
    "NULL",
    "IS",
    "BETWEEN",
    "ELSE",
    "END",
    "CASE",
    "WHEN",
    "THEN",
    "EXISTS",
    "IN",
    "LIKE",
    "GLOB",
    "REGEXP",
    "MATCH",
    "ESCAPE",
    "CURRENT_TIME",
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "TRUE",
    "FALSE",
]


@lru_cache
def _get_grammar(expr_only: bool = False) -> pp.ParserElement:
    """Return the grammar for a SELECT statement"""
    LPAR, RPAR, COMMA = map(pp.Suppress, "(),")
    DOT, STAR = map(pp.Literal, ".*")
    select_stmt = pp.Forward().setName("select statement")

    # keywords
    @lru_cache
    def kwd(k: KWD_TYPE) -> pp.CaselessKeyword:
        return pp.CaselessKeyword(k)

    any_keyword = pp.MatchFirst([kwd(k) for k in t.get_args(KWD_TYPE)])

    quoted_identifier = pp.QuotedString('"', escQuote='""')
    identifier = (~any_keyword + pp.Word(pp.alphas, pp.alphanums + "_")).setParseAction(
        pp.pyparsing_common.downcaseTokens
    ) | quoted_identifier
    collation_name = identifier.copy()
    column_name = identifier.copy()
    column_alias = identifier.copy()
    table_name = identifier.copy()
    table_alias = identifier.copy()
    index_name = identifier.copy()
    function_name = identifier.copy()
    parameter_name = identifier.copy()
    database_name = identifier.copy()

    comment = "--" + pp.restOfLine

    # expression
    expr = pp.Forward().setName("expression")

    numeric_literal = pp.pyparsing_common.number
    string_literal = pp.QuotedString("'", escQuote="''")
    blob_literal = pp.Regex(r"[xX]'[0-9A-Fa-f]+'")
    literal_value = (
        numeric_literal
        | string_literal
        | blob_literal
        | kwd("TRUE")
        | kwd("FALSE")
        | kwd("NULL")
        | kwd("CURRENT_TIME")
        | kwd("CURRENT_DATE")
        | kwd("CURRENT_TIMESTAMP")
    )
    bind_parameter = pp.Word("?", pp.nums) | pp.Combine(
        pp.oneOf(": @ $") + parameter_name
    )
    type_name = pp.oneOf("TEXT REAL INTEGER BLOB NULL")

    expr_term = (
        kwd("CAST") + LPAR + expr + kwd("AS") + type_name + RPAR
        | kwd("EXISTS") + LPAR + select_stmt + RPAR
        | function_name.setName("function_name")
        + LPAR
        + pp.Optional(STAR | pp.delimitedList(expr))
        + RPAR
        | literal_value
        | bind_parameter
        | pp.Group(
            identifier("col_db") + DOT + identifier("col_tab") + DOT + identifier("col")
        )
        | pp.Group(identifier("col_tab") + DOT + identifier("col"))
        | pp.Group(identifier("col"))
    )

    NOT_NULL = pp.Group(kwd("NOT") + kwd("NULL"))
    NOT_BETWEEN = pp.Group(kwd("NOT") + kwd("BETWEEN"))
    NOT_IN = pp.Group(kwd("NOT") + kwd("IN"))
    NOT_LIKE = pp.Group(kwd("NOT") + kwd("LIKE"))
    NOT_MATCH = pp.Group(kwd("NOT") + kwd("MATCH"))
    NOT_GLOB = pp.Group(kwd("NOT") + kwd("GLOB"))
    NOT_REGEXP = pp.Group(kwd("NOT") + kwd("REGEXP"))

    UNARY, BINARY, TERNARY = 1, 2, 3
    expr << pp.infixNotation(  # type: ignore[operator]
        expr_term,
        [
            (pp.oneOf("- + ~") | kwd("NOT"), UNARY, pp.opAssoc.RIGHT),
            (kwd("ISNULL") | kwd("NOTNULL") | NOT_NULL, UNARY, pp.opAssoc.LEFT),
            ("||", BINARY, pp.opAssoc.LEFT),
            (pp.oneOf("* / %"), BINARY, pp.opAssoc.LEFT),
            (pp.oneOf("+ -"), BINARY, pp.opAssoc.LEFT),
            (pp.oneOf("<< >> & |"), BINARY, pp.opAssoc.LEFT),
            (pp.oneOf("< <= > >="), BINARY, pp.opAssoc.LEFT),
            (
                pp.oneOf("= == != <>")
                | kwd("IS")
                | kwd("IN")
                | kwd("LIKE")
                | kwd("GLOB")
                | kwd("MATCH")
                | kwd("REGEXP")
                | NOT_IN
                | NOT_LIKE
                | NOT_GLOB
                | NOT_MATCH
                | NOT_REGEXP,
                BINARY,
                pp.opAssoc.LEFT,
            ),
            ((kwd("BETWEEN") | NOT_BETWEEN, kwd("AND")), TERNARY, pp.opAssoc.LEFT),
            (
                (kwd("IN") | NOT_IN)
                + LPAR
                + pp.Group(select_stmt | pp.delimitedList(expr))
                + RPAR,
                UNARY,
                pp.opAssoc.LEFT,
            ),
            (kwd("AND"), BINARY, pp.opAssoc.LEFT),
            (kwd("OR"), BINARY, pp.opAssoc.LEFT),
        ],
    )

    if expr_only:
        expr.ignore(comment)
        return expr("expr")

    compound_operator = (
        kwd("UNION") + pp.Optional(kwd("ALL")) | kwd("INTERSECT") | kwd("EXCEPT")
    )

    ordering_term = pp.Group(
        expr("order_key")
        + pp.Optional(kwd("COLLATE") + collation_name("collate"))
        + pp.Optional(kwd("ASC") | kwd("DESC"))("direction")
    )

    join_constraint = pp.Group(
        pp.Optional(
            kwd("ON") + expr
            | kwd("USING") + LPAR + pp.Group(pp.delimitedList(column_name)) + RPAR
        )
    )

    join_op = COMMA | pp.Group(
        pp.Optional(kwd("NATURAL"))
        + pp.Optional(
            kwd("INNER")
            | kwd("CROSS")
            | kwd("LEFT") + kwd("OUTER")
            | kwd("LEFT")
            | kwd("OUTER")
        )
        + kwd("JOIN")
    )

    join_source = pp.Forward()
    single_source = (
        pp.Group(
            database_name("database") + DOT + table_name("table*")
            | table_name("table*")
        )
        + pp.Optional(pp.Optional(kwd("AS")) + table_alias("table_alias*"))
        + pp.Optional(
            kwd("INDEXED") + kwd("BY") + index_name("name")
            | kwd("NOT") + kwd("INDEXED")
        )("index")
        | (
            LPAR
            + select_stmt
            + RPAR
            + pp.Optional(pp.Optional(kwd("AS")) + table_alias)
        )
        | (LPAR + join_source + RPAR)
    )

    join_source <<= (
        pp.Group(
            single_source + pp.OneOrMore(join_op + single_source + join_constraint)
        )
        | single_source
    )

    result_column = pp.Group(
        STAR("col")
        | table_name("col_table") + DOT + STAR("col")
        | expr("col") + pp.Optional(pp.Optional(kwd("AS")) + column_alias("alias"))
    )

    select_core = (
        kwd("SELECT")
        + pp.Optional(kwd("DISTINCT") | kwd("ALL"))
        + pp.Group(pp.delimitedList(result_column))("columns")
        + pp.Optional(kwd("FROM") + join_source("from*"))
        + pp.Optional(kwd("WHERE") + expr("where_expr"))
        + pp.Optional(
            kwd("GROUP")
            + kwd("BY")
            + pp.Group(pp.delimitedList(ordering_term))("group_by_terms")
            + pp.Optional(kwd("HAVING") + expr("having_expr"))
        )
    )

    select_stmt << (  # type: ignore[operator]
        select_core
        + pp.ZeroOrMore(compound_operator + select_core)
        + pp.Optional(
            kwd("ORDER")
            + kwd("BY")
            + pp.Group(pp.delimitedList(ordering_term))("order_by_terms")
        )
        + pp.Optional(
            kwd("LIMIT")
            + (
                pp.Group(expr + kwd("OFFSET") + expr)
                | pp.Group(expr + COMMA + expr)
                | expr
            )("limit")
        )
    )

    select_stmt.ignore(comment)

    return select_stmt


def get_grammar_select() -> pp.ParserElement:
    """Return the grammar for SELECT statements."""
    return _get_grammar()


def get_grammar_expr() -> pp.ParserElement:
    """Return the grammar for expressions."""
    return _get_grammar(expr_only=True)


class FilterStringError(NotImplementedError):
    """Raised when a filter string cannot be parsed."""

    def __init__(
        self, filter_string: str, *, user: str = "Could not be read", detail: str = ""
    ) -> None:
        super().__init__(f"{user}: {filter_string!r}")
        self.user = user
        self.filter_string = filter_string
        self.detail = detail


def filter_from_string(
    obj_cls: t.Type["Base"], filter_string: str
) -> t.Union[None, "ColumnElement[bool]"]:
    """Create a filter from a string."""
    from sqlalchemy import and_, or_
    from sqlalchemy.ext.associationproxy import ColumnAssociationProxyInstance
    from sqlalchemy.orm import InstrumentedAttribute

    if not isinstance(filter_string, str):
        raise TypeError(f"Expected a string, got {type(filter_string)}")
    if not filter_string:
        return None
    try:
        parsed = (
            get_grammar_expr()
            .parse_string(filter_string, parse_all=True)
            .as_dict()["expr"]
        )
    except pp.ParseException as exc:
        raise FilterStringError(
            filter_string,
            detail=f"Invalid SQL at column {exc.column} ({exc.msg}): {exc.line!r}",
        ) from exc

    if isinstance(parsed, dict):
        # e.g. "a" => {"col": "a"}
        return None
    elif not isinstance(parsed, list):
        raise FilterStringError(filter_string, detail=f"Expected a list: {parsed}")
    len_parsed = len(parsed)
    if len_parsed == 0:
        return None
    if isinstance(parsed[0], dict) and len(parsed) == 3:
        # e.g. "a == 1" => [{'col': 'a'}, '==', 1]
        parsed = [parsed]
        len_parsed = 1
    index = 0
    last_op = None
    comp = None
    while index < len_parsed:
        if not (isinstance(parsed[index], list) and len(parsed[index]) == 3):
            raise FilterStringError(
                filter_string, detail=f"Expected a list of length 3: {parsed[index]}"
            )
        assign, comparator, value = parsed[index]
        if not isinstance(assign, dict):
            raise FilterStringError(filter_string, detail=f"Expected a dict: {assign}")
        col_str = assign.get("col")
        if col_str is None:
            raise FilterStringError(
                filter_string,
                user="Left comparators must be columns",
                detail=f"Expected a 'col' key: {assign}",
            )
        tbl_str = assign.get("col_tab")
        if tbl_str is not None:
            # TODO how to do join filters? like "status.state == 'created'" for calcjob
            # see: https://docs.sqlalchemy.org/en/20/orm/queryguide/select.html#orm-queryguide-relationship-operators
            raise FilterStringError(filter_string, user=f"Unknown table: {tbl_str}")
        tbl_obj = obj_cls
        col = getattr(tbl_obj, col_str, None)
        if col is None:
            raise FilterStringError(
                filter_string,
                user=f"Unknown column {col_str!r}",
                detail=f"Attribute {col_str!r} not found",
            )
        if not isinstance(col, (InstrumentedAttribute, ColumnAssociationProxyInstance)):
            raise FilterStringError(
                filter_string,
                user=f"Unknown column {col_str!r} {col}",
                detail=f"Attribute {col_str!r} is not a InstrumentedAttribute",
            )
        if isinstance(value, dict):
            if "col" in value:
                raise FilterStringError(
                    filter_string,
                    user="unknown right comparison",
                    detail=f"Got a column for right comparison: {value}",
                )
            raise FilterStringError(
                filter_string,
                user="unknown right comparison",
                detail=f"Got a dict for a value comparison: {value}",
            )
        if comparator == "==":
            next_comp = col == value
        elif comparator == "!=":
            next_comp = col != value
        elif comparator == ">":
            next_comp = col > value
        elif comparator == ">=":
            next_comp = col >= value
        elif comparator == "<":
            next_comp = col < value
        elif comparator == "<=":
            next_comp = col <= value
        elif comparator == "IN":
            next_comp = col.in_(value)
        elif comparator == ["NOT", "IN"]:
            next_comp = ~col.in_(value)
        elif comparator == "LIKE":
            next_comp = col.like(value)
        elif comparator == ["NOT", "LIKE"]:
            next_comp = ~col.like(value)
        else:
            raise FilterStringError(
                filter_string, user=f"Unknown comparator: {comparator}"
            )

        if comp is not None and last_op == "AND":
            comp = and_(comp, next_comp)
        elif comp is not None and last_op == "OR":
            comp = or_(comp, next_comp)
        else:
            comp = next_comp

        if (index + 1) < len_parsed:
            if parsed[index + 1] == "AND":
                last_op = "AND"
                index += 1
            elif parsed[index + 1] == "OR":
                last_op = "OR"
                index += 1
            else:
                raise FilterStringError(
                    filter_string, user=f"Unknown operator: {parsed[index + 1]}"
                )

        index += 1

    return comp
