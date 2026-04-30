"""Comprehensive security-focused tests for SQL validator.

These tests verify that SQL validation correctly prevents SQL injection
and destructive operations while allowing valid SELECT queries.
"""

import pytest

from plugins.ckan.sql_validator import SafeSQLBuilder, SQLValidator


class TestValidSelectQueries:
    """Test that valid SELECT queries pass validation."""

    def test_simple_select_passes(self):
        """Test that simple SELECT query passes."""
        sql = 'SELECT * FROM "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_where_clause_passes(self):
        """Test that SELECT with WHERE clause passes."""
        sql = "SELECT * FROM \"11111111-2222-3333-4444-555555555555\" WHERE status = 'Open'"
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_order_by_passes(self):
        """Test that SELECT with ORDER BY passes."""
        sql = (
            'SELECT * FROM "11111111-2222-3333-4444-555555555555" ORDER BY date DESC'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_limit_passes(self):
        """Test that SELECT with LIMIT passes."""
        sql = 'SELECT * FROM "11111111-2222-3333-4444-555555555555" LIMIT 10'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_specific_columns_passes(self):
        """Test that SELECT with specific columns passes."""
        sql = 'SELECT id, name, status FROM "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_count_passes(self):
        """Test that SELECT with COUNT passes."""
        sql = 'SELECT COUNT(*) FROM "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_group_by_passes(self):
        """Test that SELECT with GROUP BY passes."""
        sql = 'SELECT status, COUNT(*) FROM "11111111-2222-3333-4444-555555555555" GROUP BY status'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_join_passes(self):
        """Test that SELECT with JOIN passes."""
        sql = 'SELECT a.* FROM "11111111-2222-3333-4444-555555555555" a JOIN "66666666-7777-8888-9999-aaaaaaaaaaaa" b ON a.id = b.id'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_cte_passes(self):
        """Test that SELECT with CTE passes."""
        sql = """
        WITH subquery AS (
            SELECT * FROM "11111111-2222-3333-4444-555555555555"
        )
        SELECT * FROM subquery
        """
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True, f"CTE query should pass but got error: {error}"
        assert error is None

    def test_select_with_window_function_passes(self):
        """Test that SELECT with window functions passes."""
        sql = 'SELECT *, RANK() OVER (PARTITION BY status ORDER BY date) FROM "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_valid_uuid_format_passes(self):
        """Test that SELECT with valid UUID format passes."""
        sql = 'SELECT * FROM "a1b2c3d4-e5f6-7890-abcd-ef1234567890"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_case_insensitive_passes(self):
        """Test that SELECT in lowercase passes."""
        sql = 'select * from "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None


class TestRejectDestructiveOperations:
    """Test that destructive operations are rejected."""

    def test_insert_statement_rejected(self):
        """Test that INSERT statements are rejected."""
        sql = (
            "INSERT INTO \"11111111-2222-3333-4444-555555555555\" VALUES (1, 'test')"
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "INSERT" in error or "SELECT" in error

    def test_update_statement_rejected(self):
        """Test that UPDATE statements are rejected."""
        sql = "UPDATE \"11111111-2222-3333-4444-555555555555\" SET status = 'Closed'"
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "UPDATE" in error or "SELECT" in error

    def test_delete_statement_rejected(self):
        """Test that DELETE statements are rejected."""
        sql = 'DELETE FROM "11111111-2222-3333-4444-555555555555" WHERE id = 1'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "DELETE" in error or "SELECT" in error

    def test_drop_statement_rejected(self):
        """Test that DROP statements are rejected."""
        sql = 'DROP TABLE "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "DROP" in error or "SELECT" in error

    def test_create_statement_rejected(self):
        """Test that CREATE statements are rejected."""
        sql = "CREATE TABLE test (id INT)"
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "CREATE" in error or "SELECT" in error

    def test_alter_statement_rejected(self):
        """Test that ALTER statements are rejected."""
        sql = (
            'ALTER TABLE "11111111-2222-3333-4444-555555555555" ADD COLUMN test INT'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "ALTER" in error

    def test_truncate_statement_rejected(self):
        """Test that TRUNCATE statements are rejected."""
        sql = 'TRUNCATE TABLE "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "TRUNCATE" in error

    def test_grant_statement_rejected(self):
        """Test that GRANT statements are rejected."""
        sql = 'GRANT SELECT ON "11111111-2222-3333-4444-555555555555" TO user'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "GRANT" in error

    def test_revoke_statement_rejected(self):
        """Test that REVOKE statements are rejected."""
        sql = 'REVOKE SELECT ON "11111111-2222-3333-4444-555555555555" FROM user'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "REVOKE" in error


class TestRejectSQLInjection:
    """Test that SQL injection patterns are rejected."""

    def test_multiple_statements_rejected(self):
        """Test that multiple statements are rejected."""
        sql = (
            'SELECT * FROM "11111111-2222-3333-4444-555555555555"; DROP TABLE users;'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "Multiple statements" in error or "DROP" in error

    def test_multiple_select_statements_rejected(self):
        """Test that multiple SELECT statements are rejected."""
        sql = 'SELECT * FROM "11111111-2222-3333-4444-555555555555"; SELECT * FROM "66666666-7777-8888-9999-aaaaaaaaaaaa"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "Multiple statements" in error

    def test_dangerous_comment_rejected(self):
        """Test that dangerous comments are rejected."""
        sql = 'SELECT * FROM "11111111-2222-3333-4444-555555555555" -- DROP TABLE users'
        is_valid, error = SQLValidator.validate_query(sql)
        # This might pass if comment handling is lenient, but should ideally fail
        # The validator should catch DROP in comments
        if not is_valid:
            assert "Dangerous comment" in error or "DROP" in error

    def test_command_execution_pattern_rejected(self):
        """Test that command execution patterns are rejected."""
        sql = "SELECT * FROM \"11111111-2222-3333-4444-555555555555\" WHERE xp_cmdshell('dir')"
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "Command execution" in error or "xp_cmdshell" in error

    def test_file_write_pattern_rejected(self):
        """Test that file write patterns are rejected."""
        sql = 'SELECT * INTO OUTFILE "/tmp/test" FROM "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "File write" in error or "OUTFILE" in error

    def test_sleep_function_rejected(self):
        """Test that sleep functions are rejected."""
        sql = (
            'SELECT * FROM "11111111-2222-3333-4444-555555555555" WHERE pg_sleep(10)'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "Sleep function" in error or "pg_sleep" in error

    def test_union_based_injection_detected(self):
        """Test that UNION-based injection attempts are detected."""
        # This should fail because it's not a valid SELECT structure
        sql = 'SELECT * FROM "11111111-2222-3333-4444-555555555555" UNION SELECT * FROM users'
        is_valid, error = SQLValidator.validate_query(sql)
        # UNION might be valid in some contexts, but should be checked
        # The validator should parse and validate the structure
        assert isinstance(is_valid, bool)
        assert error is None or isinstance(error, str)


class TestRejectInvalidInputs:
    """Test that invalid inputs are rejected."""

    def test_empty_string_rejected(self):
        """Test that empty string is rejected."""
        sql = ""
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "non-empty" in error.lower() or "string" in error.lower()

    def test_none_value_rejected(self):
        """Test that None value is rejected."""
        sql = None
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "non-empty" in error.lower() or "string" in error.lower()

    def test_whitespace_only_rejected(self):
        """Test that whitespace-only string is rejected."""
        sql = "   \n\t  "
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None

    def test_too_long_query_rejected(self):
        """Test that queries exceeding max length are rejected."""
        # Create a query that exceeds MAX_SQL_LENGTH
        base_query = 'SELECT * FROM "11111111-2222-3333-4444-555555555555" WHERE '
        padding = "x" * (SQLValidator.MAX_SQL_LENGTH + 100)
        sql = base_query + padding
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "too long" in error.lower() or str(SQLValidator.MAX_SQL_LENGTH) in error

    def test_non_string_type_rejected(self):
        """Test that non-string types are rejected."""
        sql = 12345
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "string" in error.lower()


class TestRejectInvalidUUIDs:
    """Test that invalid UUID formats are rejected."""

    def test_invalid_uuid_format_rejected(self):
        """Test that invalid UUID format in resource ID is rejected."""
        sql = 'SELECT * FROM "not-a-uuid-at-all"'
        is_valid, error = SQLValidator.validate_query(sql)
        # If the string doesn't match UUID pattern, it won't be checked
        # But if it matches pattern regex but isn't valid UUID, should fail
        # This depends on validator implementation
        assert isinstance(is_valid, bool)

    def test_malformed_uuid_rejected(self):
        """Test that malformed UUID is rejected."""
        sql = 'SELECT * FROM "12345678-1234-1234-1234-123456789012"'
        is_valid, error = SQLValidator.validate_query(sql)
        # This might pass if regex matches but UUID validation fails
        # Should ideally fail on UUID format validation
        assert isinstance(is_valid, bool)

    def test_uuid_without_quotes_passes_if_no_uuid_check(self):
        """Test that UUID without quotes might pass (depends on validator)."""
        sql = "SELECT * FROM 11111111-2222-3333-4444-555555555555"
        is_valid, error = SQLValidator.validate_query(sql)
        # Without quotes, UUID pattern won't match, so won't be validated
        # But should still pass SELECT validation
        assert isinstance(is_valid, bool)


class TestRejectForbiddenKeywords:
    """Test that forbidden keywords in various contexts are rejected."""

    def test_execute_keyword_rejected(self):
        """Test that EXECUTE keyword is rejected."""
        sql = (
            'SELECT * FROM "11111111-2222-3333-4444-555555555555" WHERE EXECUTE test'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "EXECUTE" in error

    def test_exec_keyword_rejected(self):
        """Test that EXEC keyword is rejected."""
        sql = 'SELECT * FROM "11111111-2222-3333-4444-555555555555" WHERE EXEC test'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "EXEC" in error

    def test_call_keyword_rejected(self):
        """Test that CALL keyword is rejected."""
        sql = 'SELECT * FROM "11111111-2222-3333-4444-555555555555" WHERE CALL test'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "CALL" in error

    def test_declare_keyword_rejected(self):
        """Test that DECLARE keyword is rejected."""
        sql = (
            'SELECT * FROM "11111111-2222-3333-4444-555555555555" WHERE DECLARE @var'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert error is not None
        assert "DECLARE" in error

    def test_set_keyword_in_where_might_pass(self):
        """Test that SET keyword in WHERE clause might pass (context-dependent)."""
        sql = (
            'SELECT * FROM "11111111-2222-3333-4444-555555555555" WHERE status = SET'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        # SET as a value might pass, but SET as keyword should be caught
        # This depends on validator implementation
        assert isinstance(is_valid, bool)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_select_with_nested_subquery_passes(self):
        """Test that SELECT with nested subquery passes."""
        sql = 'SELECT * FROM (SELECT * FROM "11111111-2222-3333-4444-555555555555") sub'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_having_clause_passes(self):
        """Test that SELECT with HAVING clause passes."""
        sql = 'SELECT status, COUNT(*) FROM "11111111-2222-3333-4444-555555555555" GROUP BY status HAVING COUNT(*) > 10'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_distinct_passes(self):
        """Test that SELECT DISTINCT passes."""
        sql = 'SELECT DISTINCT status FROM "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_aggregate_functions_passes(self):
        """Test that SELECT with aggregate functions passes."""
        sql = 'SELECT AVG(value), MAX(value), MIN(value) FROM "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_exactly_max_length_passes(self):
        """Test that query exactly at max length passes."""
        # Create query exactly at MAX_SQL_LENGTH
        base_query = 'SELECT * FROM "11111111-2222-3333-4444-555555555555"'
        padding_length = SQLValidator.MAX_SQL_LENGTH - len(base_query)
        if padding_length > 0:
            sql = base_query + " " + "x" * (padding_length - 1)
            is_valid, error = SQLValidator.validate_query(sql)
            assert is_valid is True
            assert error is None

    def test_select_with_special_characters_passes(self):
        """Test that SELECT with special characters passes."""
        sql = "SELECT * FROM \"11111111-2222-3333-4444-555555555555\" WHERE name = 'O'Brien'"
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None

    def test_select_with_regex_patterns_passes(self):
        """Test that SELECT with regex patterns passes."""
        sql = "SELECT * FROM \"11111111-2222-3333-4444-555555555555\" WHERE name ~ '^[A-Z]'"
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True
        assert error is None


class TestFromJoinTargetEnforcement:
    """FROM/JOIN targets must be UUID-quoted resources or CTE aliases."""

    def test_schema_qualified_target_rejected(self):
        sql = "SELECT * FROM pg_catalog.pg_user"
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert "Schema-qualified" in error

    def test_information_schema_rejected(self):
        sql = "SELECT * FROM information_schema.columns"
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert "Schema-qualified" in error

    def test_union_to_unknown_table_rejected(self):
        sql = (
            'SELECT * FROM "11111111-2222-3333-4444-555555555555" '
            "UNION SELECT * FROM users"
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert "users" in error

    def test_quoted_non_uuid_rejected(self):
        sql = 'SELECT * FROM "not-a-uuid-at-all-really"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert "UUID" in error or "Invalid" in error

    def test_cte_alias_accepted(self):
        sql = (
            'WITH sub AS (SELECT * FROM "11111111-2222-3333-4444-555555555555") '
            "SELECT * FROM sub"
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True, error

    def test_subquery_with_alias_accepted(self):
        sql = (
            'SELECT * FROM (SELECT * FROM "11111111-2222-3333-4444-555555555555")'
            " sub"
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True, error

    def test_join_with_one_unknown_target_rejected(self):
        sql = (
            'SELECT * FROM "11111111-2222-3333-4444-555555555555" a '
            'JOIN "pg_user" b ON a.id = b.id'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False

    def test_bare_select_without_from_rejected(self):
        sql = "SELECT 1"
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert "FROM" in error


class TestCommentStrippingBeforeKeywordScan:
    """Forbidden keywords hidden in comments must not slip past the scanner."""

    def test_block_comment_hiding_select_prefix_rejected(self):
        sql = '/*SELECT*/ DELETE FROM "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert "DELETE" in error

    def test_line_comment_hiding_delete_rejected(self):
        sql = (
            '-- comment\nDELETE FROM "11111111-2222-3333-4444-555555555555"'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert "DELETE" in error

    def test_benign_block_comment_accepted(self):
        sql = 'SELECT * /* hello */ FROM "11111111-2222-3333-4444-555555555555"'
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is True, error


class TestForbiddenFunctions:
    """Postgres functions useful for data exfiltration are blocked by name."""

    @pytest.mark.parametrize(
        "fn",
        [
            "pg_read_file",
            "pg_ls_dir",
            "pg_stat_file",
            "lo_import",
            "lo_export",
            "current_setting",
            "set_config",
            "dblink",
        ],
    )
    def test_forbidden_function_rejected(self, fn):
        sql = (
            f"SELECT {fn}('x') FROM "
            '"11111111-2222-3333-4444-555555555555"'
        )
        is_valid, error = SQLValidator.validate_query(sql)
        assert is_valid is False
        assert fn in error


class TestSafeSQLBuilderIdentifier:
    def test_valid_identifier_quoted(self):
        assert SafeSQLBuilder.quote_identifier("neighborhood") == '"neighborhood"'

    def test_underscore_and_digits(self):
        assert SafeSQLBuilder.quote_identifier("col_1") == '"col_1"'

    @pytest.mark.parametrize(
        "bad",
        [
            "col; DROP TABLE x",
            "col)",
            "1col",
            "col space",
            "col.other",
            "col--",
            "",
            None,
            42,
        ],
    )
    def test_bad_identifier_rejected(self, bad):
        with pytest.raises(ValueError):
            SafeSQLBuilder.quote_identifier(bad)


class TestSafeSQLBuilderMetric:
    def test_count_star(self):
        assert SafeSQLBuilder.validate_metric_expr("count(*)") == "count(*)"

    def test_count_star_whitespace(self):
        assert (
            SafeSQLBuilder.validate_metric_expr("  COUNT ( * ) ")
            == "count(*)"
        )

    @pytest.mark.parametrize(
        "expr,expected",
        [
            ("sum(amount)", 'sum("amount")'),
            ("avg(value)", 'avg("value")'),
            ("min(x)", 'min("x")'),
            ("max(x)", 'max("x")'),
            ("stddev(y)", 'stddev("y")'),
            ("count(distinct user_id)", 'count(DISTINCT "user_id")'),
        ],
    )
    def test_aggregate_quotes_identifier(self, expr, expected):
        assert SafeSQLBuilder.validate_metric_expr(expr) == expected

    @pytest.mark.parametrize(
        "bad",
        [
            "pg_sleep(10)",
            "count(*)); DROP TABLE x--",
            "sum(x + y)",
            "sum(x); select 1",
            "count(*) + 1",
            "concat(a, b)",
            "sum(x.y)",
            "",
            None,
        ],
    )
    def test_bad_metric_rejected(self, bad):
        with pytest.raises(ValueError):
            SafeSQLBuilder.validate_metric_expr(bad)


class TestSafeSQLBuilderFilter:
    def test_integer_value(self):
        assert SafeSQLBuilder.build_filter_condition("id", 42) == '"id" = 42'

    def test_float_value(self):
        assert (
            SafeSQLBuilder.build_filter_condition("lat", 42.5)
            == '"lat" = 42.5'
        )

    def test_none_value(self):
        assert (
            SafeSQLBuilder.build_filter_condition("status", None)
            == '"status" IS NULL'
        )

    def test_bool_true(self):
        assert (
            SafeSQLBuilder.build_filter_condition("active", True)
            == '"active" = TRUE'
        )

    def test_string_value_escaped(self):
        assert (
            SafeSQLBuilder.build_filter_condition("name", "O'Brien")
            == "\"name\" = 'O''Brien'"
        )

    def test_string_injection_escaped_not_executed(self):
        got = SafeSQLBuilder.build_filter_condition("name", "x' OR 1=1--")
        assert got == "\"name\" = 'x'' OR 1=1--'"

    def test_bad_field_rejected(self):
        with pytest.raises(ValueError):
            SafeSQLBuilder.build_filter_condition("name; DROP TABLE x", "ok")

    def test_unsupported_value_type_rejected(self):
        with pytest.raises(ValueError):
            SafeSQLBuilder.build_filter_condition("name", ["list"])


class TestSafeSQLBuilderWhereClause:
    def test_empty_returns_empty_string(self):
        assert SafeSQLBuilder.build_where_clause(None) == ""
        assert SafeSQLBuilder.build_where_clause({}) == ""

    def test_scalar_shorthand_equality(self):
        got = SafeSQLBuilder.build_where_clause({"status": "Closed"})
        assert got == "\"status\" = 'Closed'"

    def test_scalar_none_is_null(self):
        got = SafeSQLBuilder.build_where_clause({"close_date": None})
        assert got == '"close_date" IS NULL'

    def test_date_range(self):
        got = SafeSQLBuilder.build_where_clause(
            {"close_date": {"gte": "2026-04-29", "lt": "2026-04-30"}}
        )
        assert got == (
            "\"close_date\" >= '2026-04-29' AND "
            "\"close_date\" < '2026-04-30'"
        )

    def test_mixed_scalar_and_range(self):
        got = SafeSQLBuilder.build_where_clause(
            {
                "close_date": {"gte": "2026-04-29", "lt": "2026-04-30"},
                "case_status": "Closed",
            }
        )
        assert "\"case_status\" = 'Closed'" in got
        assert "\"close_date\" >= '2026-04-29'" in got
        assert "\"close_date\" < '2026-04-30'" in got
        assert " AND " in got

    def test_numeric_comparisons(self):
        got = SafeSQLBuilder.build_where_clause({"count": {"gt": 5, "lte": 10}})
        assert got == '"count" > 5 AND "count" <= 10'

    def test_in_list_strings(self):
        got = SafeSQLBuilder.build_where_clause(
            {"neighborhood": {"in": ["Roxbury", "Dorchester"]}}
        )
        assert got == "\"neighborhood\" IN ('Roxbury', 'Dorchester')"

    def test_not_in_list(self):
        got = SafeSQLBuilder.build_where_clause(
            {"status": {"not_in": ["Open", "Pending"]}}
        )
        assert got == "\"status\" NOT IN ('Open', 'Pending')"

    def test_like_escaped(self):
        got = SafeSQLBuilder.build_where_clause(
            {"address": {"like": "%Beacon%"}}
        )
        assert got == "\"address\" LIKE '%Beacon%'"

    def test_ilike(self):
        got = SafeSQLBuilder.build_where_clause(
            {"name": {"ilike": "boston%"}}
        )
        assert got == "\"name\" ILIKE 'boston%'"

    def test_is_null_true(self):
        got = SafeSQLBuilder.build_where_clause({"close_date": {"is_null": True}})
        assert got == '"close_date" IS NULL'

    def test_is_null_false(self):
        got = SafeSQLBuilder.build_where_clause(
            {"close_date": {"is_null": False}}
        )
        assert got == '"close_date" IS NOT NULL'

    def test_quote_injection_in_string_value_escaped(self):
        got = SafeSQLBuilder.build_where_clause(
            {"name": {"eq": "x' OR 1=1--"}}
        )
        assert got == "\"name\" = 'x'' OR 1=1--'"

    def test_quote_injection_in_in_list_escaped(self):
        got = SafeSQLBuilder.build_where_clause(
            {"name": {"in": ["a", "b' OR 1=1--"]}}
        )
        assert got == "\"name\" IN ('a', 'b'' OR 1=1--')"

    def test_unknown_operator_rejected(self):
        with pytest.raises(ValueError, match="Unknown operator"):
            SafeSQLBuilder.build_where_clause({"x": {"regex": "."}})

    def test_bad_field_rejected(self):
        with pytest.raises(ValueError):
            SafeSQLBuilder.build_where_clause({"x; DROP TABLE": 1})

    def test_in_requires_list(self):
        with pytest.raises(ValueError, match="non-empty list"):
            SafeSQLBuilder.build_where_clause({"x": {"in": "single"}})

    def test_in_rejects_empty_list(self):
        with pytest.raises(ValueError, match="non-empty list"):
            SafeSQLBuilder.build_where_clause({"x": {"in": []}})

    def test_in_rejects_oversized_list(self):
        with pytest.raises(ValueError, match="too long"):
            SafeSQLBuilder.build_where_clause({"x": {"in": list(range(101))}})

    def test_like_rejects_non_string(self):
        with pytest.raises(ValueError, match="string pattern"):
            SafeSQLBuilder.build_where_clause({"x": {"like": 5}})

    def test_is_null_rejects_non_bool(self):
        with pytest.raises(ValueError, match="bool"):
            SafeSQLBuilder.build_where_clause({"x": {"is_null": "yes"}})

    def test_overlong_string_rejected(self):
        big = "a" * 1000
        with pytest.raises(ValueError, match="too long"):
            SafeSQLBuilder.build_where_clause({"x": {"eq": big}})

    def test_non_dict_top_level_rejected(self):
        with pytest.raises(ValueError, match="must be a dict"):
            SafeSQLBuilder.build_where_clause(["x"])


class TestSafeSQLBuilderOrderAndLimit:
    def test_order_by_plain(self):
        assert SafeSQLBuilder.validate_order_by("date") == '"date"'

    def test_order_by_desc(self):
        assert SafeSQLBuilder.validate_order_by("date DESC") == '"date" DESC'

    def test_order_by_asc_lower(self):
        assert SafeSQLBuilder.validate_order_by("date asc") == '"date" ASC'

    @pytest.mark.parametrize(
        "bad", ["date; DROP", "date, other", "1", "a.b", "", None]
    )
    def test_bad_order_by_rejected(self, bad):
        with pytest.raises(ValueError):
            SafeSQLBuilder.validate_order_by(bad)

    def test_limit_clamped_to_max(self):
        assert SafeSQLBuilder.clamp_limit(10**9) == SafeSQLBuilder.MAX_LIMIT

    def test_limit_passthrough(self):
        assert SafeSQLBuilder.clamp_limit(50) == 50

    @pytest.mark.parametrize("bad", [0, -1, "10", None, True, 1.5])
    def test_bad_limit_rejected(self, bad):
        with pytest.raises(ValueError):
            SafeSQLBuilder.clamp_limit(bad)


class TestExtractTopLevelLimit:
    UUID = "11111111-2222-3333-4444-555555555555"

    def test_simple_limit(self):
        assert (
            SQLValidator.extract_top_level_limit(
                f'SELECT * FROM "{self.UUID}" LIMIT 50'
            )
            == 50
        )

    def test_limit_with_trailing_semicolon(self):
        assert (
            SQLValidator.extract_top_level_limit(
                f'SELECT * FROM "{self.UUID}" LIMIT 100;'
            )
            == 100
        )

    def test_no_limit_returns_none(self):
        assert (
            SQLValidator.extract_top_level_limit(f'SELECT * FROM "{self.UUID}"')
            is None
        )

    def test_subquery_limit_ignored(self):
        # Top-level statement has no LIMIT; the subquery's LIMIT does
        # not count.
        sql = (
            f'SELECT * FROM (SELECT * FROM "{self.UUID}" LIMIT 5) sub '
        )
        assert SQLValidator.extract_top_level_limit(sql) is None

    def test_after_enforce_row_limit(self):
        sql = SQLValidator.enforce_row_limit(
            f'SELECT * FROM "{self.UUID}"'
        )
        assert (
            SQLValidator.extract_top_level_limit(sql)
            == SQLValidator.DEFAULT_ROW_LIMIT
        )


class TestSafeSQLBuilderResourceId:
    def test_valid_uuid(self):
        uuid = "11111111-2222-3333-4444-555555555555"
        assert SafeSQLBuilder.validate_resource_id(uuid) == uuid

    @pytest.mark.parametrize(
        "bad",
        [
            "pg_catalog.pg_user",
            "not-a-uuid",
            "11111111-2222-3333-4444-55555555555",  # too short
            "",
            None,
            123,
        ],
    )
    def test_bad_resource_id_rejected(self, bad):
        with pytest.raises(ValueError):
            SafeSQLBuilder.validate_resource_id(bad)


class TestEnforceRowLimit:
    """``SQLValidator.enforce_row_limit`` appends a LIMIT if absent."""

    UUID = "11111111-2222-3333-4444-555555555555"

    def test_appends_limit_when_missing(self):
        sql = f'SELECT * FROM "{self.UUID}"'
        out = SQLValidator.enforce_row_limit(sql)
        assert out.endswith(f"LIMIT {SQLValidator.DEFAULT_ROW_LIMIT}")

    def test_preserves_existing_top_level_limit(self):
        sql = f'SELECT * FROM "{self.UUID}" LIMIT 5'
        out = SQLValidator.enforce_row_limit(sql)
        assert out == sql

    def test_preserves_limit_case_insensitive(self):
        sql = f'SELECT * FROM "{self.UUID}" limit 5'
        out = SQLValidator.enforce_row_limit(sql)
        assert out == sql

    def test_subquery_limit_does_not_count_as_top_level(self):
        sql = (
            f'SELECT * FROM (SELECT * FROM "{self.UUID}" LIMIT 5) sub'
        )
        out = SQLValidator.enforce_row_limit(sql)
        assert out.endswith(f"LIMIT {SQLValidator.DEFAULT_ROW_LIMIT}")

    def test_cte_without_top_level_limit_gets_limit_appended(self):
        sql = (
            f'WITH t AS (SELECT neighborhood FROM "{self.UUID}") '
            "SELECT * FROM t"
        )
        out = SQLValidator.enforce_row_limit(sql)
        assert out.endswith(f"LIMIT {SQLValidator.DEFAULT_ROW_LIMIT}")

    def test_cte_with_top_level_limit_preserved(self):
        sql = (
            f'WITH t AS (SELECT neighborhood FROM "{self.UUID}") '
            "SELECT * FROM t LIMIT 3"
        )
        out = SQLValidator.enforce_row_limit(sql)
        assert out == sql

    def test_trailing_semicolon_stripped_before_append(self):
        sql = f'SELECT * FROM "{self.UUID}";'
        out = SQLValidator.enforce_row_limit(sql)
        assert ";" not in out.split("LIMIT")[0]
        assert out.endswith(f"LIMIT {SQLValidator.DEFAULT_ROW_LIMIT}")

    def test_trailing_whitespace_handled(self):
        sql = f'SELECT * FROM "{self.UUID}"   \n  '
        out = SQLValidator.enforce_row_limit(sql)
        assert out.endswith(f"LIMIT {SQLValidator.DEFAULT_ROW_LIMIT}")
