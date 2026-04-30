"""SQL validator and safe SQL builder for CKAN plugin.

Two concerns live here:

- ``SQLValidator`` hardens the ``execute_sql`` path. It rejects anything that
  isn't a single SELECT against a UUID-quoted CKAN resource (or a CTE alias
  thereof). Comments are stripped before keyword/function scanning so that
  block-comment obfuscation cannot slip forbidden tokens past the check.

- ``SafeSQLBuilder`` powers ``aggregate_data``. It validates every identifier,
  metric expression, filter value, and LIMIT against an allowlist so that
  caller-supplied strings can never reach the generated SQL unescaped.
"""

import re
from typing import Any, List, Optional, Set, Tuple

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Parenthesis, TokenList
from sqlparse.tokens import Keyword


_UUID_RE = re.compile(
    r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$",
    re.IGNORECASE,
)
_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_METRIC_RE = re.compile(
    r"^\s*(count|sum|avg|min|max|stddev)\s*"
    r"\(\s*(?:(distinct)\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$",
    re.IGNORECASE,
)
_COUNT_STAR_RE = re.compile(r"^\s*count\s*\(\s*\*\s*\)\s*$", re.IGNORECASE)
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")


def _strip_comments(sql: str) -> str:
    """Remove SQL comments so content-based scans can't be hidden behind them."""
    without_block = _BLOCK_COMMENT_RE.sub(" ", sql)
    return _LINE_COMMENT_RE.sub(" ", without_block)


class SQLValidator:
    """Validates SQL queries for security before execution."""

    MAX_SQL_LENGTH = 8192
    DEFAULT_ROW_LIMIT = 10000
    FORBIDDEN_KEYWORDS = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "GRANT",
        "REVOKE",
        "TRUNCATE",
        "EXECUTE",
        "EXEC",
        "CALL",
        "DECLARE",
        "SET",
        "PREPARE",
        "COPY",
        "LISTEN",
        "NOTIFY",
        "VACUUM",
        "ANALYZE",
        "CLUSTER",
        "REINDEX",
        "LOAD",
        "DO",
    ]
    FORBIDDEN_FUNCTIONS = [
        "xp_cmdshell",
        "pg_sleep",
        "pg_read_file",
        "pg_read_binary_file",
        "pg_ls_dir",
        "pg_stat_file",
        "lo_import",
        "lo_export",
        "current_setting",
        "set_config",
        "dblink",
    ]

    @staticmethod
    def validate_query(sql: Any) -> Tuple[bool, Optional[str]]:
        """Validate SQL security. Returns (is_valid, error_message)."""
        if not sql or not isinstance(sql, str):
            return False, "SQL must be non-empty string"
        sql = sql.strip()
        if not sql:
            return False, "SQL must be non-empty string"
        if len(sql) > SQLValidator.MAX_SQL_LENGTH:
            return False, f"SQL too long (max {SQLValidator.MAX_SQL_LENGTH})"

        # Strip comments so keyword/function scans can't be bypassed by hiding
        # payloads inside /* ... */ or -- comments.
        sql_scan = _strip_comments(sql)

        for keyword in SQLValidator.FORBIDDEN_KEYWORDS:
            if re.search(rf"\b{keyword}\b", sql_scan, re.IGNORECASE):
                return False, f"Forbidden keyword: {keyword}"

        for fn in SQLValidator.FORBIDDEN_FUNCTIONS:
            if re.search(rf"\b{re.escape(fn)}\b", sql_scan, re.IGNORECASE):
                return False, f"Forbidden function: {fn}"

        sql_upper = sql_scan.lstrip().upper()
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            return False, "Only SELECT queries allowed"

        for pattern, msg in [
            (r"into\s+outfile", "File write detected"),
            (r"into\s+dumpfile", "File write detected"),
        ]:
            if re.search(pattern, sql_scan, re.IGNORECASE):
                return False, msg

        try:
            parsed = sqlparse.parse(sql)
        except Exception as e:
            return False, f"SQL parsing error: {e}"
        if len(parsed) != 1:
            return False, "Multiple statements not allowed"
        statement = parsed[0]
        statement_type = statement.get_type()
        if statement_type is not None and statement_type not in ("SELECT", "UNKNOWN"):
            return False, "Only SELECT statements allowed"

        try:
            cte_aliases = _extract_cte_aliases(statement)
            targets = _extract_from_join_targets(statement)
        except Exception as e:
            return False, f"Could not parse FROM/JOIN clause: {e}"

        if not targets:
            return False, (
                "No FROM/JOIN target found (query must reference a resource)"
            )

        for name, parent in targets:
            if parent is not None:
                return False, (
                    f"Schema-qualified FROM/JOIN target not allowed: "
                    f"{parent}.{name}"
                )
            if _UUID_RE.match(name):
                continue
            if name in cte_aliases:
                continue
            return False, (
                f"FROM/JOIN target must be a UUID-quoted resource or CTE alias: "
                f"{name}"
            )

        return True, None

    @classmethod
    def enforce_row_limit(cls, sql: str) -> str:
        """Append ``LIMIT`` to an already-validated query if it lacks one.

        Bounds the upstream scan cost on CKAN: every ``execute_sql`` path
        resolves to a query capped at ``DEFAULT_ROW_LIMIT`` rows even if the
        caller forgot to set one. A user-supplied top-level ``LIMIT`` is
        preserved as-is; a ``LIMIT`` buried inside a subquery or CTE does not
        count — we only treat the outermost statement.
        """
        try:
            parsed = sqlparse.parse(sql)
        except Exception:
            return sql
        if not parsed:
            return sql
        statement = parsed[0]
        for tok in statement.tokens:
            if isinstance(tok, Parenthesis):
                continue
            if tok.ttype in Keyword and tok.normalized.upper() == "LIMIT":
                return sql
        stripped = sql.rstrip().rstrip(";").rstrip()
        return f"{stripped} LIMIT {cls.DEFAULT_ROW_LIMIT}"


def _extract_cte_aliases(statement: TokenList) -> Set[str]:
    """If statement is a CTE (``WITH ...``), collect the alias names."""
    aliases: Set[str] = set()
    for tok in statement.tokens:
        if tok.is_whitespace:
            continue
        if tok.ttype in Keyword and tok.normalized.upper() == "WITH":
            # Next non-whitespace token should be the alias declarations.
            idx = statement.tokens.index(tok)
            for nxt in statement.tokens[idx + 1 :]:
                if nxt.is_whitespace:
                    continue
                if isinstance(nxt, IdentifierList):
                    for ident in nxt.get_identifiers():
                        if isinstance(ident, Identifier):
                            name = ident.get_real_name()
                            if name:
                                aliases.add(name)
                elif isinstance(nxt, Identifier):
                    name = nxt.get_real_name()
                    if name:
                        aliases.add(name)
                break
            break
        # Not a CTE.
        return aliases
    return aliases


def _extract_from_join_targets(
    statement: TokenList,
) -> List[Tuple[str, Optional[str]]]:
    """Walk sqlparse tokens to extract every FROM/JOIN table reference.

    Returns a list of ``(name, parent)`` tuples. ``parent`` is the schema
    qualifier (e.g. ``pg_catalog``) if present, otherwise ``None``.
    Subqueries are recursed into rather than recorded. Aliases attached to
    CTEs and subqueries are skipped because the subquery's inner FROM is
    what we care about.
    """
    results: List[Tuple[str, Optional[str]]] = []

    def record(ident: Identifier) -> None:
        name = ident.get_real_name()
        parent = ident.get_parent_name()
        if name is None:
            # Couldn't parse — be conservative and reject.
            results.append((str(ident).strip(), "?"))
            return
        results.append((name, parent))

    def walk(token_list: TokenList) -> None:
        expecting = False
        for tok in token_list.tokens:
            if tok.is_whitespace:
                continue

            if tok.ttype in Keyword:
                upper = tok.normalized.upper()
                if upper == "FROM" or "JOIN" in upper:
                    expecting = True
                else:
                    expecting = False
                continue

            if isinstance(tok, Parenthesis):
                walk(tok)
                expecting = False
                continue

            if expecting:
                if isinstance(tok, IdentifierList):
                    for ident in tok.get_identifiers():
                        if isinstance(ident, Identifier):
                            first = ident.token_first(skip_ws=True, skip_cm=True)
                            if isinstance(first, Parenthesis):
                                walk(first)
                            else:
                                record(ident)
                        elif isinstance(ident, Parenthesis):
                            walk(ident)
                    expecting = False
                    continue
                if isinstance(tok, Identifier):
                    first = tok.token_first(skip_ws=True, skip_cm=True)
                    if isinstance(first, Parenthesis):
                        walk(first)
                    else:
                        record(tok)
                    expecting = False
                    continue
                if isinstance(tok, TokenList):
                    walk(tok)
                expecting = False
                continue

            if isinstance(tok, TokenList):
                walk(tok)

    walk(statement)
    return results


class SafeSQLBuilder:
    """Build safe SQL fragments for ``aggregate_data``.

    Every method either returns a validated, quoted SQL fragment or raises
    ``ValueError``. Callers should surface ``ValueError`` as a user-visible
    error without executing anything against CKAN.
    """

    MAX_LIMIT = 10000
    ALLOWED_AGG_FUNCTIONS = {"count", "sum", "avg", "min", "max", "stddev"}

    @staticmethod
    def validate_resource_id(resource_id: Any) -> str:
        if not isinstance(resource_id, str) or not _UUID_RE.match(resource_id):
            raise ValueError(
                f"resource_id must be a valid UUID (got: {resource_id!r})"
            )
        return resource_id

    @staticmethod
    def quote_identifier(name: Any) -> str:
        """Validate a column/alias name and return its double-quoted form."""
        if not isinstance(name, str) or not _SIMPLE_IDENT_RE.match(name):
            raise ValueError(
                f"Invalid identifier (must match [A-Za-z_][A-Za-z0-9_]*): "
                f"{name!r}"
            )
        return f'"{name}"'

    @staticmethod
    def validate_metric_expr(expr: Any) -> str:
        """Validate an aggregate expression against an allowlist.

        Accepted forms:
          - ``count(*)``
          - ``{count|sum|avg|min|max|stddev}(<ident>)``
          - ``{count|sum|avg|min|max|stddev}(DISTINCT <ident>)``

        Returns the canonicalized form with identifiers double-quoted.
        """
        if not isinstance(expr, str):
            raise ValueError(f"metric expression must be a string: {expr!r}")
        if _COUNT_STAR_RE.match(expr):
            return "count(*)"
        m = _METRIC_RE.match(expr)
        if not m:
            raise ValueError(
                "Invalid metric expression (allowed: count(*), "
                "{count|sum|avg|min|max|stddev}([DISTINCT] <ident>)): "
                f"{expr!r}"
            )
        func = m.group(1).lower()
        distinct = "DISTINCT " if m.group(2) else ""
        ident = m.group(3)
        return f'{func}({distinct}"{ident}")'

    @staticmethod
    def build_filter_condition(field: Any, value: Any) -> str:
        """Build a safe WHERE condition.

        Field names are validated as identifiers. Values are coerced:
        ``None`` → ``IS NULL``, booleans → ``TRUE``/``FALSE``, numbers are
        formatted, and strings are single-quoted with embedded quotes
        escaped.
        """
        quoted = SafeSQLBuilder.quote_identifier(field)
        if value is None:
            return f"{quoted} IS NULL"
        if isinstance(value, bool):
            return f"{quoted} = {'TRUE' if value else 'FALSE'}"
        if isinstance(value, (int, float)):
            return f"{quoted} = {value}"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"{quoted} = '{escaped}'"
        raise ValueError(
            f"Unsupported filter value type for {field!r}: "
            f"{type(value).__name__}"
        )

    # Operator → SQL fragment for build_where_clause. Restricted to a known-
    # safe set; arbitrary operators are rejected.
    _COMPARISON_OPS = {
        "eq": "=",
        "ne": "!=",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
    }
    _MAX_STRING_VALUE_LEN = 256
    _MAX_IN_LIST_LEN = 100
    ALLOWED_WHERE_OPS = (
        "eq",
        "ne",
        "gt",
        "gte",
        "lt",
        "lte",
        "in",
        "not_in",
        "like",
        "ilike",
        "is_null",
    )

    @classmethod
    def _format_scalar(cls, value: Any, op_label: str) -> str:
        """Format a scalar as a SQL literal. Strings are single-quoted with
        embedded quotes escaped; numbers and bools are inlined."""
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            if len(value) > cls._MAX_STRING_VALUE_LEN:
                raise ValueError(
                    f"value too long for {op_label!r}: "
                    f"{len(value)} > {cls._MAX_STRING_VALUE_LEN}"
                )
            return "'" + value.replace("'", "''") + "'"
        raise ValueError(
            f"unsupported value type for {op_label!r}: "
            f"{type(value).__name__}"
        )

    @classmethod
    def build_where_clause(cls, where: Any) -> str:
        """Build a parameter-validated SQL WHERE fragment from a structured
        spec.

        Accepted shapes per field:
          - ``"col": <scalar>`` — equality (or ``IS NULL`` if ``None``).
          - ``"col": {"op": value, ...}`` — one or more comparison clauses
            ANDed together. Allowed ops:

              ``eq``, ``ne``, ``gt``, ``gte``, ``lt``, ``lte``  — scalar value.
              ``in``, ``not_in``                                — list of scalars.
              ``like``, ``ilike``                               — string pattern.
              ``is_null``                                       — bool.

        Returns the WHERE fragment WITHOUT the leading ``WHERE`` (or ``""``
        if ``where`` is empty/None). Raises ``ValueError`` on any disallowed
        operator, identifier, or value.
        """
        if where in (None, {}):
            return ""
        if not isinstance(where, dict):
            raise ValueError(
                f"where must be a dict, got: {type(where).__name__}"
            )

        parts: List[str] = []
        for field, spec in where.items():
            quoted = SafeSQLBuilder.quote_identifier(field)
            if not isinstance(spec, dict):
                # Scalar shorthand → equality / IS NULL.
                parts.append(SafeSQLBuilder.build_filter_condition(field, spec))
                continue
            if not spec:
                raise ValueError(
                    f"empty operator dict for field {field!r}"
                )
            for op, val in spec.items():
                if not isinstance(op, str):
                    raise ValueError(
                        f"operator must be a string for {field!r}: {op!r}"
                    )
                op_lower = op.lower()
                if op_lower in cls._COMPARISON_OPS:
                    sql_op = cls._COMPARISON_OPS[op_lower]
                    parts.append(
                        f"{quoted} {sql_op} {cls._format_scalar(val, op_lower)}"
                    )
                elif op_lower in ("in", "not_in"):
                    if not isinstance(val, list) or not val:
                        raise ValueError(
                            f"{op_lower!r} requires a non-empty list for "
                            f"{field!r}, got: {val!r}"
                        )
                    if len(val) > cls._MAX_IN_LIST_LEN:
                        raise ValueError(
                            f"{op_lower!r} list too long for {field!r}: "
                            f"{len(val)} > {cls._MAX_IN_LIST_LEN}"
                        )
                    items = ", ".join(
                        cls._format_scalar(v, op_lower) for v in val
                    )
                    sql_kw = "IN" if op_lower == "in" else "NOT IN"
                    parts.append(f"{quoted} {sql_kw} ({items})")
                elif op_lower in ("like", "ilike"):
                    if not isinstance(val, str):
                        raise ValueError(
                            f"{op_lower!r} requires a string pattern for "
                            f"{field!r}, got: {val!r}"
                        )
                    parts.append(
                        f"{quoted} {op_lower.upper()} "
                        f"{cls._format_scalar(val, op_lower)}"
                    )
                elif op_lower == "is_null":
                    if not isinstance(val, bool):
                        raise ValueError(
                            f"'is_null' requires a bool for {field!r}, "
                            f"got: {val!r}"
                        )
                    parts.append(
                        f"{quoted} {'IS NULL' if val else 'IS NOT NULL'}"
                    )
                else:
                    raise ValueError(
                        f"Unknown operator {op!r} for field {field!r}. "
                        f"Allowed: {', '.join(cls.ALLOWED_WHERE_OPS)}"
                    )

        return " AND ".join(parts)

    @staticmethod
    def validate_order_by(order_by: Any) -> str:
        """Validate an ``ORDER BY`` clause: ``<identifier> [ASC|DESC]``."""
        if not isinstance(order_by, str):
            raise ValueError(f"order_by must be a string: {order_by!r}")
        m = re.match(
            r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(ASC|DESC)?\s*$",
            order_by,
            re.IGNORECASE,
        )
        if not m:
            raise ValueError(
                f"Invalid order_by (expected identifier [ASC|DESC]): "
                f"{order_by!r}"
            )
        ident = f'"{m.group(1)}"'
        direction = f" {m.group(2).upper()}" if m.group(2) else ""
        return f"{ident}{direction}"

    @staticmethod
    def clamp_limit(limit: Any) -> int:
        """Accept a positive int and clamp to ``MAX_LIMIT``."""
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError(f"limit must be an integer: {limit!r}")
        if limit < 1:
            raise ValueError(f"limit must be >= 1: {limit}")
        return min(limit, SafeSQLBuilder.MAX_LIMIT)
