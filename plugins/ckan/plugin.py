"""CKAN plugin implementation for OpenContext.

This plugin provides access to CKAN-based open data portals.
"""

import difflib
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.interfaces import DataPlugin, PluginType, ToolDefinition, ToolResult
from plugins.ckan.config_schema import CKANPluginConfig
from plugins.ckan.sql_validator import SafeSQLBuilder, SQLValidator


# Datasets edited longer ago than this are flagged with a DATA FRESHNESS
# caveat -- useful when the model is about to call a 4-year-old snapshot
# "current".
_STALE_DATASET_DAYS = 365

# Tunes when the SMALL SAMPLE banner fires: any total in (1, _SMALL_SAMPLE_MAX]
# triggers it. Single-record (N=1) gets its own, stronger banner.
_SMALL_SAMPLE_MAX = 10

# Used by _params_repr to keep the echoed-Query line from being dominated
# by a multi-line SQL string. Long values are tail-truncated.
_PARAMS_REPR_MAX = 200

# Copilot C8: default response size should be small. Copilot truncates
# long tool responses or streams them slowly; the model summarizes 20
# records well more reliably than 100 records badly. Caller can always
# pass a higher `limit` explicitly.
DEFAULT_QUERY_LIMIT = 20
DEFAULT_SEARCH_LIMIT = 10

# A field whose values are mostly NULL-like (real null, "", "N/A",
# "Unknown", "None", or the string "NULL") at or above this share triggers
# a DATA QUALITY caveat (civic-AI #11). Tuned so a column with the odd
# missing value stays silent but a column that's mostly empty fires.
_NULL_LIKE_FREQ_THRESHOLD = 0.20

# Values that look semantically null even though they're real strings.
# CKAN datasets put any of these in a column to mean "missing" -- the
# model otherwise treats "Unknown" as a meaningful category (civic-AI #10).
_NULL_LIKE_STRINGS = frozenset({"", "n/a", "na", "unknown", "none", "null", "-", "--"})

# Resource is considered "old" when its frequency is unset and last
# modified more than this many days ago (civic-AI #6).
_NO_FREQUENCY_OLD_DAYS = 730  # 2 years

# Multiplier for the abandonment detector (civic-AI #5): a resource is
# flagged if its last_modified is older than this many times the stated
# update_frequency interval (e.g. weekly cadence + 4x stale = >28d).
_ABANDONMENT_INTERVAL_MULTIPLIER = 4

# Approximate day-count for each declared update frequency. Keys are
# lowercased, normalized substrings; the first matching substring of the
# dataset's stated frequency wins. See _frequency_days().
_FREQUENCY_DAYS: Dict[str, int] = {
    "real-time": 1,
    "realtime": 1,
    "daily": 1,
    "weekly": 7,
    "biweekly": 14,
    "bi-weekly": 14,
    "fortnight": 14,
    "semi-month": 15,
    "semimonth": 15,
    "monthly": 30,
    "bimonth": 60,
    "bi-month": 60,
    "quarter": 91,
    "semi-annual": 182,
    "semiannual": 182,
    "biannual": 182,
    "annual": 365,
    "yearly": 365,
}

logger = logging.getLogger(__name__)


class CKANPlugin(DataPlugin):
    """Plugin for accessing CKAN-based open data portals.

    This plugin implements the DataPlugin interface and provides tools for
    searching datasets, retrieving dataset metadata, and querying data.
    """

    plugin_name = "ckan"
    plugin_type = PluginType.OPEN_DATA
    plugin_version = "1.0.0"

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize CKAN plugin with configuration.

        Args:
            config: Plugin configuration dictionary
        """
        super().__init__(config)
        self.plugin_config = CKANPluginConfig(**config)
        self.client: Optional[httpx.AsyncClient] = None

    async def initialize(self) -> bool:
        """Initialize CKAN plugin and test connection.

        Returns:
            True if initialization succeeded
        """
        try:
            # Create HTTP client
            headers = {}
            if self.plugin_config.api_key:
                headers["Authorization"] = self.plugin_config.api_key

            self.client = httpx.AsyncClient(
                base_url=self.plugin_config.base_url,
                headers=headers,
                timeout=self.plugin_config.timeout,
            )

            # Test connection
            response = await self._call_ckan_api("status_show", {})
            if response.get("success"):
                self._initialized = True
                logger.info(
                    f"CKAN plugin initialized successfully for {self.plugin_config.city_name}"
                )
                return True
            else:
                logger.error("CKAN API connection test failed")
                return False

        except Exception as e:
            logger.error(f"Failed to initialize CKAN plugin: {e}", exc_info=True)
            return False

    async def shutdown(self) -> None:
        """Shutdown plugin and close HTTP client."""
        if self.client:
            await self.client.aclose()
            self.client = None
        self._initialized = False
        logger.info("CKAN plugin shut down")

    @staticmethod
    def _is_queryable(resource: Dict[str, Any]) -> bool:
        """A CKAN resource is queryable via datastore_search only if it has
        been loaded into CKAN's Postgres datastore. Boston attaches each
        dataset as 5-7 download-only resources (GeoJSON, KML, SHP, ...) plus
        a single CSV that's actually loaded; only that one returns rows."""
        return bool(resource.get("datastore_active"))

    @classmethod
    def _first_queryable_resource(
        cls, dataset: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Return the first resource of a dataset that is loaded into the
        datastore (i.e. answers datastore_search), or None if none are."""
        for res in dataset.get("resources") or []:
            if cls._is_queryable(res):
                return res
        return None

    def _parse_ckan_error(
        self, response_body: Dict[str, Any], context: str = ""
    ) -> str:
        """Extract human-readable error from CKAN API response body."""
        if response_body.get("success") is True:
            return ""
        err = response_body.get("error", {})
        msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
        portal = f" on {self.plugin_config.city_name} OpenData portal"
        base = f"{msg}{portal}" if msg else f"Unknown error{portal}"
        return f"{context}: {base}" if context else base

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_not_exception_type((RuntimeError, httpx.HTTPStatusError)),
    )
    async def _call_ckan_api(self, action: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call CKAN API action.

        Args:
            action: CKAN action name (e.g., "package_search")
            data: Action parameters

        Returns:
            CKAN API response

        Raises:
            RuntimeError: On HTTP errors or when CKAN returns success: false
        """
        if not self.client:
            raise RuntimeError("Plugin not initialized")

        url = f"/api/3/action/{action}"
        portal = f"{self.plugin_config.city_name} OpenData portal"

        try:
            response = await self.client.post(url, json=data)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            try:
                body = e.response.json()
                ckan_msg = self._parse_ckan_error(body, "")
                if ckan_msg:
                    raise RuntimeError(f"Error: {ckan_msg} (HTTP {status_code})") from e
            except ValueError:
                pass
            param_hint = ""
            if "resource_id" in data:
                param_hint = f" Resource '{data.get('resource_id')}'"
            elif "id" in data:
                param_hint = f" Dataset '{data.get('id')}'"
            raise RuntimeError(
                f"Error:{param_hint} not found on {portal} (HTTP {status_code})"
            ) from e

        result = response.json()

        if result.get("success") is False:
            msg = self._parse_ckan_error(result, "")
            raise RuntimeError(f"Error: {msg}" if msg else f"API error on {portal}")

        return result

    def get_tools(self) -> List[ToolDefinition]:
        """Get list of tools provided by CKAN plugin.

        Returns:
            List of tool definitions

        Note on Copilot B3 (enum schemas): every free-text string
        parameter on these tools is either an opaque CKAN identifier
        (resource UUID, dataset slug) or free user input (search
        keyword, SQL). The fixed-vocabulary surfaces that exist on this
        plugin -- WHERE operators, aggregate function names, ORDER BY
        direction -- are all nested inside structured `where` / `metrics`
        / `order_by` strings rather than top-level params, so JSON
        Schema enum doesn't apply cleanly. The hallucination guard
        that enum would have provided is delivered instead by the
        pre-flight field-name validator and the SafeSQLBuilder
        allowlist (see plugins/ckan/sql_validator.py).
        """
        city = self.plugin_config.city_name
        return [
            ToolDefinition(
                name="search_datasets",
                description=(
                    f"Find datasets in {city}'s open data portal by keyword. "
                    "If you want data rows, prefer ckan__search_and_query "
                    "(combines search + query in one call). Use this tool "
                    "when you need to list or compare candidate datasets. "
                    "Returns dataset metadata; the response highlights the "
                    "best queryable resource_id."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": (
                                "Free-text keywords matched against "
                                "DATASET METADATA (title/tags/desc), NOT "
                                "row content. Use the row-returning "
                                "tools' `where` argument to filter ROWS. "
                                "Examples: '311', 'parks'."
                            ),
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                f"Maximum number of datasets to return "
                                f"(default: {DEFAULT_SEARCH_LIMIT})."
                            ),
                            "default": DEFAULT_SEARCH_LIMIT,
                        },
                    },
                    "required": ["query"],
                },
            ),
            ToolDefinition(
                name="get_dataset",
                description=(
                    f"Get full metadata for one {city} dataset, including "
                    "its resources (each with a queryable resource_id "
                    "UUID). Use this to discover resource UUIDs needed by "
                    "ckan__query_data / ckan__aggregate_data / "
                    "ckan__execute_sql, and to see the data freshness / "
                    "update-cadence caveats for the dataset."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "dataset_id": {
                            "type": "string",
                            "description": (
                                "Dataset ID or slug. Provenance: the `id` "
                                "(or `name`) field of a dataset returned by "
                                "`ckan__search_datasets`. NOT a resource UUID."
                            ),
                        },
                    },
                    "required": ["dataset_id"],
                },
            ),
            ToolDefinition(
                name="query_data",
                description=(
                    f"Query rows from a {city} resource. CASE-SENSITIVE "
                    "field names -- typos are rejected with a 'did you "
                    "mean' suggestion. `resource_id` is the UUID from "
                    "ckan__search_datasets / ckan__get_dataset, NOT a "
                    "dataset ID. Use `filters` for equality (status='Open') "
                    "and `where` for ranges/dates/IN/LIKE. If you only "
                    "have a keyword and no resource_id, use "
                    "ckan__search_and_query instead."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "resource_id": {
                            "type": "string",
                            "description": (
                                "CKAN resource UUID (36-char "
                                "hex+hyphen format). Get one from "
                                "ckan__search_datasets or "
                                "ckan__get_dataset; this is NOT a "
                                "dataset ID. Do NOT invent or guess "
                                "this value."
                            ),
                        },
                        "filters": {
                            "type": "object",
                            "description": (
                                "Equality-only row filters as a JSON "
                                "object of field -> value. For range, "
                                "date, IN, or LIKE comparisons use "
                                "`where` instead; `filters` cannot "
                                "express anything other than exact "
                                "equality."
                            ),
                        },
                        "where": {
                            "type": "object",
                            "description": (
                                "Structured WHERE clause as a JSON "
                                "object of field -> spec. A spec is "
                                "either a scalar (equality) or "
                                "{op: value} where `op` is one of: "
                                "eq, ne, gt, gte, lt, lte, in, "
                                "not_in, like, ilike, is_null. Field "
                                "names are case-sensitive; get them "
                                "from ckan__get_schema or from the "
                                "'Filterable columns' footer of a "
                                "prior query."
                            ),
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                f"Maximum number of records (default: "
                                f"{DEFAULT_QUERY_LIMIT}). Ask for a higher "
                                "limit when the user explicitly wants more "
                                "than ~20 records or you need a larger "
                                "sample to summarize."
                            ),
                            "default": DEFAULT_QUERY_LIMIT,
                        },
                    },
                    "required": ["resource_id"],
                },
            ),
            ToolDefinition(
                name="get_schema",
                description=(
                    f"Get the case-sensitive field names + types for a "
                    f"{city} resource. Call before ckan__aggregate_data "
                    "or ckan__execute_sql so you reference real columns. "
                    "Note: most CKAN columns are TEXT even when the "
                    "values are dates or numbers -- watch for type-note "
                    "warnings in query responses."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "resource_id": {
                            "type": "string",
                            "description": (
                                "CKAN resource UUID. Get one from "
                                "ckan__search_datasets or "
                                "ckan__get_dataset; do NOT invent."
                            ),
                        },
                    },
                    "required": ["resource_id"],
                },
            ),
            ToolDefinition(
                name="execute_sql",
                description=(
                    f"LAST RESORT: raw PostgreSQL SELECT against {city}'s "
                    "CKAN datastore. Try ckan__query_data and "
                    "ckan__aggregate_data first -- only use this for "
                    "joins, CTEs, window functions, or anything the "
                    "structured tools can't express. Field names and "
                    "resource UUIDs must come from ckan__get_schema / "
                    "ckan__get_dataset, not guesses. Only SELECT allowed; "
                    "resource UUIDs in FROM must be double-quoted "
                    '(FROM "uuid"). Responses carry a SQL PASSTHROUGH '
                    "warning so you can confirm the generated SQL matched "
                    "the user's actual question."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "PostgreSQL SELECT statement. Resource UUIDs in FROM must be double-quoted.",
                        },
                    },
                    "required": ["sql"],
                },
            ),
            ToolDefinition(
                name="aggregate_data",
                description=(
                    f"GROUP BY + counts/sums/avgs on a {city} resource. "
                    "CASE-SENSITIVE field names; typos rejected with a "
                    "'did you mean' hint. resource_id from "
                    "ckan__search_datasets / ckan__get_dataset; field "
                    "names from ckan__get_schema. Example: "
                    'group_by=["neighborhood"], '
                    'metrics={"n": "count(*)"}. Supports count(*), '
                    "sum, avg, min, max, stddev."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "resource_id": {
                            "type": "string",
                            "description": (
                                "CKAN resource UUID. Get one from "
                                "ckan__search_datasets or "
                                "ckan__get_dataset; do NOT invent."
                            ),
                        },
                        "group_by": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                "Field names to group by. "
                                "CASE-SENSITIVE; get exact names "
                                "from ckan__get_schema."
                            ),
                        },
                        "metrics": {
                            "type": "object",
                            "description": (
                                "JSON object of alias -> aggregate "
                                "expression. Supported aggregates: "
                                "count(*), count(field), sum(field), "
                                "avg(field), min(field), max(field), "
                                "stddev(field). Field names are "
                                "CASE-SENSITIVE; get them from "
                                "ckan__get_schema."
                            ),
                        },
                        "filters": {
                            "type": "object",
                            "description": (
                                "Equality-only row filters as a JSON "
                                "object of field -> value."
                            ),
                        },
                        "having": {
                            "type": "object",
                            "description": (
                                "HAVING clause: numeric thresholds on "
                                "aggregate expressions."
                            ),
                        },
                        "order_by": {
                            "type": "string",
                            "description": (
                                "Field or alias to ORDER BY; suffix "
                                "with ' DESC' for descending."
                            ),
                        },
                        "limit": {
                            "type": "integer",
                            "default": DEFAULT_QUERY_LIMIT,
                        },
                    },
                    "required": ["resource_id", "metrics"],
                },
            ),
            ToolDefinition(
                name="search_and_query",
                description=(
                    f"START HERE for {city} data: one call from keyword "
                    "to rows. `query` matches dataset TITLE/TAGS, NOT "
                    "row content; use `where` to filter rows (dates, "
                    "ranges, IN/LIKE). Auto-picks the queryable resource. "
                    "Multi-archive datasets (e.g. per-year 311 archives): "
                    'pass `resource_name="2020"` to pick a specific '
                    "year, or include_resource_totals=true for an "
                    "across-resources count breakdown."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": (
                                "Free-text keywords matched against "
                                "DATASET METADATA (title/tags/desc), NOT "
                                "row content. Use `where` (or "
                                "`ckan__execute_sql`) to filter ROWS by "
                                "date/status/etc. Examples: '311', "
                                "'parks', 'building permits'."
                            ),
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                f"Maximum number of data rows to return "
                                f"(default: {DEFAULT_QUERY_LIMIT})."
                            ),
                            "default": DEFAULT_QUERY_LIMIT,
                        },
                        "filters": {
                            "type": "object",
                            "description": (
                                "Equality-only row filters as a JSON "
                                "object of field -> value. For range, "
                                "date, IN, or LIKE comparisons use "
                                "`where` instead."
                            ),
                        },
                        "where": {
                            "type": "object",
                            "description": (
                                "Structured WHERE clause as a JSON "
                                "object of field -> spec for range, "
                                "date, IN, LIKE, or NULL checks. A "
                                "spec is a scalar (equality) or "
                                "{op: value} where `op` is one of: "
                                "eq, ne, gt, gte, lt, lte, in, "
                                "not_in, like, ilike, is_null. Field "
                                "names are case-sensitive; get them "
                                "from a prior query's 'Filterable "
                                "columns' footer."
                            ),
                        },
                        "dataset_index": {
                            "type": "integer",
                            "description": (
                                "Which search result to use (0 = best "
                                "match). If omitted, walks the search "
                                "results until one with a queryable "
                                "(datastore_active) resource is found."
                            ),
                        },
                        "resource_index": {
                            "type": "integer",
                            "description": (
                                "Which resource within the chosen dataset "
                                "to query. If omitted, auto-picks the "
                                "first datastore_active resource (Boston "
                                "datasets typically attach 5-7 resources "
                                "but only the CSV is queryable). "
                                "`resource_name` takes precedence."
                            ),
                        },
                        "resource_name": {
                            "type": "string",
                            "description": (
                                "Case-insensitive substring match on a "
                                "resource's `name`. Use this to pick a "
                                "specific archive when a dataset has "
                                "multiple queryable resources (e.g. "
                                "Boston's 311 dataset has per-year "
                                "archives '311 Service Requests - 2020', "
                                "'... - 2021', etc., plus a rolling "
                                "'NEW SYSTEM'). Examples: "
                                'resource_name="2020" picks the 2020 '
                                'archive; resource_name="NEW SYSTEM" '
                                "picks the rolling current view. The "
                                "alternates list in any prior "
                                "search_and_query response shows "
                                "available names. Takes precedence over "
                                "`resource_index`."
                            ),
                        },
                        "include_resource_totals": {
                            "type": "boolean",
                            "description": (
                                "When true, runs COUNT(*) in parallel "
                                "against every queryable resource in the "
                                "matched dataset and surfaces per-resource "
                                "totals + a grand total in the response. "
                                "Use this for dataset-wide counting "
                                "questions ('total 311 requests ever', "
                                "'how many permits across all years'). "
                                "If a `where` clause is set, it's applied "
                                "to each resource -- be aware schemas can "
                                "differ across per-year archives, so a "
                                "where clause built for one resource may "
                                "fail on others (those resources will "
                                "show n=null). Default false (one query, "
                                "fast path)."
                            ),
                        },
                    },
                    "required": ["query"],
                },
            ),
        ]

    async def execute_tool(
        self, tool_name: str, arguments: Dict[str, Any]
    ) -> ToolResult:
        """Execute a tool by name.

        Args:
            tool_name: Name of the tool
            arguments: Tool arguments

        Returns:
            ToolResult with content and success flag
        """
        try:
            if tool_name == "search_datasets":
                query = arguments.get("query", "")
                limit = arguments.get("limit", DEFAULT_SEARCH_LIMIT)
                datasets, total = await self._search_datasets_with_count(query, limit)
                text = self._format_search_results(datasets, total=total, limit=limit)
                summary = (
                    f"{self.plugin_config.portal_url.rstrip('/')} search for {query!r}"
                )
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._wrap_response(
                                text,
                                source_summary=summary,
                                calls=[("package_search", {"q": query, "rows": limit})],
                            ),
                        }
                    ],
                    success=True,
                )

            elif tool_name == "get_dataset":
                dataset_id = arguments.get("dataset_id")
                if not dataset_id:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="dataset_id is required",
                    )
                dataset = await self.get_dataset(dataset_id)
                text = self._format_dataset(dataset)
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._wrap_response(
                                text,
                                source_summary=self._source_summary_for_dataset(
                                    dataset=dataset
                                ),
                                calls=[("package_show", {"id": dataset_id})],
                            ),
                        }
                    ],
                    success=True,
                )

            elif tool_name == "query_data":
                resource_id = arguments.get("resource_id")
                if not resource_id:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="resource_id is required",
                    )
                filters = arguments.get("filters") or {}
                where = arguments.get("where") or None
                limit = arguments.get("limit", DEFAULT_QUERY_LIMIT)
                # Pre-flight: validate `where`/`filters` column names so
                # typos surface here (with suggestions) instead of as a
                # cryptic upstream 409. Best-effort; degrades silently.
                schema_fields = None
                if where or filters:
                    schema_fields = await self._schema_fields_safe(resource_id)
                    field_err = self._validate_field_names(
                        self._collect_field_refs(where, filters),
                        schema_fields,
                        context="where/filters",
                    )
                    if field_err:
                        return ToolResult(
                            content=[],
                            success=False,
                            error_message=field_err,
                        )
                records, fields, total, error = await self._query_with_schema(
                    resource_id=resource_id,
                    filters=filters,
                    limit=limit,
                    where=where,
                )
                if error:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message=error,
                    )
                text = self._format_query_results(records, fields, limit, total)
                # Pick the action that actually fired so the provenance
                # header matches the path taken (SQL path vs equality
                # search path).
                action = "datastore_search_sql" if where else "datastore_search"
                params: Dict[str, Any] = {
                    "resource_id": resource_id,
                    "limit": limit,
                }
                if filters:
                    params["filters"] = filters
                if where:
                    params["where"] = where
                summary = self._source_summary_for_dataset(resource={"id": resource_id})
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._wrap_response(
                                text,
                                source_summary=summary,
                                calls=[(action, params)],
                            ),
                        }
                    ],
                    success=True,
                )

            elif tool_name == "get_schema":
                resource_id = arguments.get("resource_id")
                if not resource_id:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="resource_id is required",
                    )
                schema = await self.get_schema(resource_id)
                text = self._format_schema(schema)
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._wrap_response(
                                text,
                                source_summary=self._source_summary_for_dataset(
                                    resource={"id": resource_id}
                                ),
                                calls=[
                                    (
                                        "datastore_search",
                                        {"resource_id": resource_id, "limit": 0},
                                    )
                                ],
                            ),
                        }
                    ],
                    success=True,
                )

            elif tool_name == "execute_sql":
                sql = arguments.get("sql")
                if not sql:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="sql parameter is required",
                    )
                result = await self.execute_sql(sql)
                if result.get("error"):
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message=result.get("message", "SQL execution failed"),
                    )
                records = result.get("records", [])
                fields = result.get("fields", [])
                effective_limit = result.get("effective_limit")
                formatted_text = self._format_sql_results(
                    records,
                    fields,
                    effective_limit=effective_limit,
                    is_passthrough=True,
                )
                summary = f"raw SQL on {self.plugin_config.portal_url.rstrip('/')}"
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._wrap_response(
                                formatted_text,
                                source_summary=summary,
                                calls=[("datastore_search_sql", {"sql": sql})],
                            ),
                        }
                    ],
                    success=True,
                )

            elif tool_name == "search_and_query":
                query = arguments.get("query")
                if not query:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="query is required",
                    )
                limit = arguments.get("limit", DEFAULT_QUERY_LIMIT)
                filters = arguments.get("filters") or {}
                where = arguments.get("where") or None
                dataset_index = arguments.get("dataset_index")
                resource_index = arguments.get("resource_index")
                resource_name = arguments.get("resource_name")
                include_resource_totals = bool(
                    arguments.get("include_resource_totals", False)
                )
                composite = await self.search_and_query(
                    query=query,
                    limit=limit,
                    filters=filters,
                    where=where,
                    dataset_index=dataset_index,
                    resource_index=resource_index,
                    resource_name=resource_name,
                    include_resource_totals=include_resource_totals,
                )
                if composite.get("error"):
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message=composite.get(
                            "message", "search_and_query failed"
                        ),
                    )
                text = self._format_search_and_query(composite, limit)
                # search_and_query fires two upstream actions in sequence.
                # Surface both so the user can reproduce the chain.
                chosen_rid = (composite.get("resource") or {}).get("id")
                second_action = "datastore_search_sql" if where else "datastore_search"
                second_params: Dict[str, Any] = {
                    "resource_id": chosen_rid,
                    "limit": limit,
                }
                if filters:
                    second_params["filters"] = filters
                if where:
                    second_params["where"] = where
                summary = self._source_summary_for_dataset(
                    dataset=composite.get("dataset"),
                    resource=composite.get("resource"),
                )
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._wrap_response(
                                text,
                                source_summary=summary,
                                calls=[
                                    ("package_search", {"q": query}),
                                    (second_action, second_params),
                                ],
                            ),
                        }
                    ],
                    success=True,
                )

            elif tool_name == "aggregate_data":
                resource_id = arguments.get("resource_id")
                if not resource_id:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="resource_id parameter is required",
                    )
                metrics = arguments.get("metrics", {})
                if not metrics:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="metrics parameter is required",
                    )
                group_by = arguments.get("group_by", []) or []
                filters = arguments.get("filters")
                # Pre-flight: catch field-name typos in group_by / metrics
                # / filters before we generate SQL. Best-effort.
                schema_fields = await self._schema_fields_safe(resource_id)
                field_err = self._validate_field_names(
                    self._collect_field_refs(
                        where=None,
                        filters=filters,
                        group_by=group_by,
                        metrics=metrics,
                    ),
                    schema_fields,
                    context="group_by/metrics/filters",
                )
                if field_err:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message=field_err,
                    )
                result = await self.aggregate_data(
                    resource_id=resource_id,
                    group_by=group_by,
                    metrics=metrics,
                    filters=filters,
                    having=arguments.get("having"),
                    order_by=arguments.get("order_by"),
                    limit=arguments.get("limit", DEFAULT_QUERY_LIMIT),
                )
                if result.get("error"):
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message=result.get("message", "Aggregation failed"),
                    )
                formatted = self._format_sql_results(
                    result.get("records", []),
                    result.get("fields", []),
                    effective_limit=result.get("effective_limit"),
                )
                agg_params: Dict[str, Any] = {
                    "resource_id": resource_id,
                    "metrics": metrics,
                }
                if group_by:
                    agg_params["group_by"] = group_by
                if filters:
                    agg_params["filters"] = filters
                summary = self._source_summary_for_dataset(resource={"id": resource_id})
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._wrap_response(
                                formatted,
                                source_summary=summary,
                                calls=[("datastore_search_sql", agg_params)],
                            ),
                        }
                    ],
                    success=True,
                )

            else:
                return ToolResult(
                    content=[],
                    success=False,
                    error_message=f"Unknown tool: {tool_name}",
                )

        except Exception as e:
            logger.error(f"Error executing tool {tool_name}: {e}", exc_info=True)
            return ToolResult(
                content=[],
                success=False,
                error_message=str(e) if str(e) else "Tool execution failed",
            )

    async def search_datasets(
        self, query: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Search for datasets matching a query.

        Args:
            query: Search query string
            limit: Maximum number of results

        Returns:
            List of dataset metadata dictionaries (count-aware variant is
            ``_search_datasets_with_count``).
        """
        datasets, _count = await self._search_datasets_with_count(query, limit)
        return datasets

    async def _search_datasets_with_count(
        self, query: str, limit: int = 20
    ) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        """Same as search_datasets but also returns CKAN's `count` -- the
        true number of datasets matching the query, regardless of the row
        cap. Lets the formatter say "20 of 47 matching datasets returned"
        instead of just "Found 20"."""
        response = await self._call_ckan_api(
            "package_search", {"q": query, "rows": limit}
        )
        result = response.get("result", {})
        count_val = result.get("count")
        try:
            count = int(count_val) if count_val is not None else None
        except (TypeError, ValueError):
            count = None
        return result.get("results", []), count

    async def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """Get detailed metadata for a specific dataset.

        Args:
            dataset_id: Dataset ID or name

        Returns:
            Dataset metadata dictionary
        """
        response = await self._call_ckan_api("package_show", {"id": dataset_id})
        return response.get("result", {})

    async def query_data(
        self,
        resource_id: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Query data from a specific resource.

        Args:
            resource_id: Resource ID
            filters: Equality-only filters (field: value) passed to
                CKAN's datastore_search
            limit: Maximum number of records
            where: Structured WHERE spec supporting comparison operators
                (gt/gte/lt/lte/in/not_in/like/ilike/is_null). When set,
                routes through datastore_search_sql for a real WHERE clause.

        Returns:
            List of data records (the schema-aware variant is
            ``_query_with_schema``).
        """
        records, _fields, _total, error = await self._query_with_schema(
            resource_id=resource_id,
            filters=filters,
            limit=limit,
            where=where,
        )
        if error:
            raise RuntimeError(error)
        return records

    async def _count_via_sql(
        self,
        resource_id: str,
        where_sql: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """Run a SELECT COUNT(*) with the same filters to discover the true
        row total when a SELECT * hit the limit.

        Returns ``None`` if the count call itself fails (we'd rather show
        a 'TRUNCATED' warning than block the data response on a failed
        count). Returns the integer total on success.
        """
        try:
            sql_parts = [f'SELECT COUNT(*) AS n FROM "{resource_id}"']
            if where_sql:
                sql_parts.append(f" WHERE {where_sql}")
            if filters:
                eq_conds = [
                    SafeSQLBuilder.build_filter_condition(f, v)
                    for f, v in filters.items()
                ]
                joiner = " AND " if where_sql else " WHERE "
                sql_parts.append(joiner + " AND ".join(eq_conds))
            sql = "".join(sql_parts)
            result = await self.execute_sql(sql)
            if result.get("error"):
                return None
            recs = result.get("records") or []
            if not recs:
                return None
            n = recs[0].get("n") or recs[0].get("count")
            if n is None:
                return None
            try:
                return int(n)
            except (TypeError, ValueError):
                return None
        except Exception:
            return None

    async def _query_with_schema(
        self,
        resource_id: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        where: Optional[Dict[str, Any]] = None,
    ) -> Tuple[
        List[Dict[str, Any]],
        List[Dict[str, Any]],
        Optional[int],
        Optional[str],
    ]:
        """Query datastore and return (records, fields, total, error).

        ``total`` is the true number of rows matching the filter, regardless
        of LIMIT. CKAN's datastore_search returns this for free; for the
        SQL (``where``) path we issue a follow-up COUNT(*) only when the
        result hit the limit (otherwise len(records) IS the total).
        Returns ``None`` when we couldn't determine a total.

        Routes through datastore_search_sql when ``where`` is set so the
        caller can express ranges/IN/LIKE; otherwise falls back to the
        cheaper datastore_search equality path.
        """
        if where:
            try:
                validated_id = SafeSQLBuilder.validate_resource_id(resource_id)
                where_sql = SafeSQLBuilder.build_where_clause(where)
                limit_int = SafeSQLBuilder.clamp_limit(limit)
            except ValueError as e:
                return [], [], None, str(e)

            sql_parts = [f'SELECT * FROM "{validated_id}"']
            if where_sql:
                sql_parts.append(f" WHERE {where_sql}")
            if filters:
                # Equality filters can ride alongside `where` clauses.
                try:
                    eq_conds = [
                        SafeSQLBuilder.build_filter_condition(f, v)
                        for f, v in filters.items()
                    ]
                except ValueError as e:
                    return [], [], None, str(e)
                joiner = " AND " if where_sql else " WHERE "
                sql_parts.append(joiner + " AND ".join(eq_conds))
            sql_parts.append(f" LIMIT {limit_int}")
            sql = "".join(sql_parts)

            result = await self.execute_sql(sql)
            if result.get("error"):
                return [], [], None, result.get("message", "SQL execution failed")
            records = result.get("records", [])
            fields = result.get("fields", [])

            # If we hit the LIMIT exactly, we don't actually know the total --
            # do a cheap COUNT(*) follow-up so the model gets a real number
            # instead of mistaking the limit for the count.
            total: Optional[int]
            if len(records) >= limit_int:
                total = await self._count_via_sql(validated_id, where_sql, filters)
            else:
                total = len(records)

            return records, fields, total, None

        # No `where` -> cheap datastore_search path.
        params: Dict[str, Any] = {"resource_id": resource_id, "limit": limit}
        if filters:
            params["filters"] = filters

        try:
            response = await self._call_ckan_api("datastore_search", params)
        except RuntimeError as e:
            msg = str(e)
            if "404" in msg or "not found" in msg.lower():
                return (
                    [],
                    [],
                    None,
                    f"{msg}\n"
                    "Hint: this resource may exist as a file download "
                    "(GeoJSON/KML/SHP/PDF) but not be loaded into the "
                    "datastore (datastore_active=false). Call "
                    "ckan__get_dataset on the parent dataset to find a "
                    "QUERYABLE resource (typically the CSV one), or use "
                    "ckan__search_and_query, which auto-picks the "
                    "datastore-loaded resource.",
                )
            return [], [], None, msg

        result = response.get("result", {})
        # CKAN returns `total` for free here -- a true count of rows
        # matching the filter, not capped by limit.
        total_val = result.get("total")
        try:
            total = int(total_val) if total_val is not None else None
        except (TypeError, ValueError):
            total = None
        return (
            result.get("records", []),
            result.get("fields", []),
            total,
            None,
        )

    async def get_schema(self, resource_id: str) -> Dict[str, Any]:
        """Get schema information for a resource.

        Args:
            resource_id: Resource ID

        Returns:
            Schema information dictionary
        """
        # Get schema by calling datastore_search with limit=0
        response = await self._call_ckan_api(
            "datastore_search", {"resource_id": resource_id, "limit": 0}
        )
        return response.get("result", {}).get("fields", [])

    async def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute raw PostgreSQL SELECT query with security validation.

        Args:
            sql: PostgreSQL SELECT statement

        Returns:
            Dictionary with success flag, records, fields, effective_limit,
            or error message
        """
        # Validate SQL
        is_valid, error = SQLValidator.validate_query(sql)
        if not is_valid:
            return {"error": True, "message": error}

        # Bound upstream scan cost: append LIMIT if the caller didn't set one.
        sql = SQLValidator.enforce_row_limit(sql)
        effective_limit = SQLValidator.extract_top_level_limit(sql)

        # Log SQL execution (truncated for security)
        logger.info("Executing SQL", extra={"sql": sql[:500]})

        # Execute
        try:
            result = await self._call_ckan_api("datastore_search_sql", {"sql": sql})
            if not result.get("success", True):
                return {
                    "error": True,
                    "message": self._parse_ckan_error(result, "SQL execution failed"),
                }
            return {
                "success": True,
                "records": result.get("result", {}).get("records", []),
                "fields": result.get("result", {}).get("fields", []),
                "effective_limit": effective_limit,
            }
        except Exception as e:
            logger.error(f"SQL execution failed: {e}", exc_info=True)
            return {"error": True, "message": str(e)}

    async def aggregate_data(
        self,
        resource_id: str,
        group_by: List[str],
        metrics: Dict[str, str],
        filters: Optional[Dict[str, Any]] = None,
        having: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Aggregate data with GROUP BY.

        Every identifier, metric expression, filter value, and LIMIT is
        validated against a strict allowlist via ``SafeSQLBuilder`` before
        the SQL is assembled, so caller-supplied strings cannot escape into
        the generated query.
        """
        try:
            resource_id = SafeSQLBuilder.validate_resource_id(resource_id)
            if not metrics:
                raise ValueError("metrics must be non-empty")

            group_by_quoted = [
                SafeSQLBuilder.quote_identifier(f) for f in (group_by or [])
            ]

            metric_parts: List[str] = []
            for alias, expr in metrics.items():
                alias_quoted = SafeSQLBuilder.quote_identifier(alias)
                expr_quoted = SafeSQLBuilder.validate_metric_expr(expr)
                metric_parts.append(f"{expr_quoted} AS {alias_quoted}")

            select_clause = ", ".join(group_by_quoted + metric_parts)

            where_clause = ""
            if filters:
                conditions = [
                    SafeSQLBuilder.build_filter_condition(f, v)
                    for f, v in filters.items()
                ]
                where_clause = " WHERE " + " AND ".join(conditions)

            group_clause = ""
            if group_by_quoted:
                group_clause = " GROUP BY " + ", ".join(group_by_quoted)

            having_clause = ""
            if having:
                having_parts: List[str] = []
                for expr, value in having.items():
                    expr_quoted = SafeSQLBuilder.validate_metric_expr(expr)
                    if isinstance(value, bool) or not isinstance(value, (int, float)):
                        raise ValueError(f"HAVING value must be numeric: {value!r}")
                    having_parts.append(f"{expr_quoted} > {value}")
                having_clause = " HAVING " + " AND ".join(having_parts)

            order_clause = ""
            if order_by:
                order_clause = " ORDER BY " + SafeSQLBuilder.validate_order_by(order_by)

            limit_int = SafeSQLBuilder.clamp_limit(limit)
        except ValueError as e:
            return {"error": True, "message": str(e)}

        sql = (
            f'SELECT {select_clause} FROM "{resource_id}"'
            f"{where_clause}{group_clause}{having_clause}{order_clause}"
            f" LIMIT {limit_int}"
        )

        return await self.execute_sql(sql)

    @staticmethod
    def _queryable_resources(dataset: Dict[str, Any]) -> List[Dict[str, Any]]:
        """All datastore_active resources in a dataset, in package_show order."""
        return [
            r for r in (dataset.get("resources") or []) if r.get("datastore_active")
        ]

    @classmethod
    def _resource_by_name(
        cls,
        dataset: Dict[str, Any],
        name_query: str,
        queryable_only: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Pick the first resource whose `name` contains `name_query`
        (case-insensitive substring)."""
        if not name_query:
            return None
        needle = name_query.casefold()
        candidates = (
            cls._queryable_resources(dataset)
            if queryable_only
            else (dataset.get("resources") or [])
        )
        for r in candidates:
            res_name = (r.get("name") or "").casefold()
            if needle in res_name:
                return r
        return None

    async def search_and_query(
        self,
        query: str,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None,
        where: Optional[Dict[str, Any]] = None,
        dataset_index: Optional[int] = None,
        resource_index: Optional[int] = None,
        resource_name: Optional[str] = None,
        include_resource_totals: bool = False,
    ) -> Dict[str, Any]:
        """Search for a dataset and immediately query a queryable resource.

        Combines search_datasets + query_data into one server-side step so
        callers don't have to extract a resource_id from a previous response.

        Resource selection precedence (highest to lowest):
          1. ``resource_name`` (substring match on resource ``name``).
          2. ``resource_index`` (explicit position in the dataset's
             resources array).
          3. First ``datastore_active`` resource in the dataset.

        Returns:
            Dict with either {"error": True, "message": ...} or
            {"dataset": {...}, "resource": {...}, "records": [...]}.
        """
        explicit_dataset = dataset_index is not None
        explicit_resource = resource_index is not None
        explicit_resource_name = bool(resource_name)
        # The model auto-picked the resource (no resource_name, resource_index,
        # or pinned dataset_index). Used by the formatter to decide whether to
        # warn that the answer is for ONE of N queryable resources and might
        # be partial relative to the user's "total" question.
        auto_picked_resource = not (explicit_resource or explicit_resource_name)
        ds_idx = dataset_index or 0
        # Cap how many search results we fetch so dataset_index can pick a
        # non-best match without an unbounded scan.
        search_rows = max(ds_idx + 1, 10)
        datasets = await self.search_datasets(query, limit=search_rows)
        if not datasets:
            return {
                "error": True,
                "message": (
                    f"No datasets found for query {query!r} in "
                    f"{self.plugin_config.city_name}'s open data portal."
                ),
            }

        if explicit_dataset:
            if ds_idx < 0 or ds_idx >= len(datasets):
                return {
                    "error": True,
                    "message": (
                        f"dataset_index {ds_idx} is out of range "
                        f"(found {len(datasets)} dataset(s))."
                    ),
                }
            candidate_indices = [ds_idx]
        else:
            # Auto-walk: try the best match first, fall through to the next
            # datasets if it has no queryable resource.
            candidate_indices = list(range(len(datasets)))

        chosen_dataset: Optional[Dict[str, Any]] = None
        chosen_resource: Optional[Dict[str, Any]] = None
        skipped_summary: List[str] = []

        for idx in candidate_indices:
            ds = datasets[idx]
            resources = ds.get("resources") or []
            if not resources:
                skipped_summary.append(
                    f"  [{idx}] {ds.get('title') or ds.get('id')}: no resources"
                )
                continue

            # 1) name match wins if provided
            if resource_name:
                matched = self._resource_by_name(ds, resource_name)
                if matched is not None:
                    chosen_dataset, chosen_resource = ds, matched
                    break
                # Only error out if the user fixed the dataset too.
                if explicit_dataset:
                    queryable_names = [
                        r.get("name") or "(unnamed)"
                        for r in self._queryable_resources(ds)
                    ]
                    return {
                        "error": True,
                        "message": (
                            f"No queryable resource in dataset "
                            f"{ds.get('id')!r} has a name matching "
                            f"{resource_name!r}. Available queryable "
                            f"resource names: "
                            f"{queryable_names or '(none)'}."
                        ),
                    }
                # Otherwise fall through and try the next dataset.
                skipped_summary.append(
                    f"  [{idx}] {ds.get('title') or ds.get('id')}: "
                    f"no resource name matching {resource_name!r}"
                )
                continue

            # 2) explicit positional pick
            if explicit_resource:
                if resource_index < 0 or resource_index >= len(resources):
                    return {
                        "error": True,
                        "message": (
                            f"resource_index {resource_index} is out of "
                            f"range for dataset {ds.get('id')!r} "
                            f"(has {len(resources)} resource(s))."
                        ),
                    }
                resource = resources[resource_index]
                if not self._is_queryable(resource):
                    return {
                        "error": True,
                        "message": (
                            f"resource_index {resource_index} of dataset "
                            f"{ds.get('id')!r} has datastore_active=false "
                            "(download-only). Pick a different "
                            "resource_index, or omit it to auto-pick the "
                            "queryable one."
                        ),
                    }
                chosen_dataset, chosen_resource = ds, resource
                break

            # 3) auto-pick the first queryable resource
            queryable = self._first_queryable_resource(ds)
            if queryable:
                chosen_dataset, chosen_resource = ds, queryable
                break

            formats = sorted({(r.get("format") or "?").upper() for r in resources})
            skipped_summary.append(
                f"  [{idx}] {ds.get('title') or ds.get('id')}: "
                f"no datastore-loaded resource (formats: {', '.join(formats)})"
            )

        # If we walked all datasets and resource_name was set but never
        # matched, give a name-specific error rather than the generic
        # "no queryable resource" one.
        if chosen_dataset is None and resource_name and not explicit_dataset:
            return {
                "error": True,
                "message": (
                    f"No dataset in the {len(datasets)} matches for query "
                    f"{query!r} has a queryable resource whose name "
                    f"matches {resource_name!r}.\nSkipped:\n"
                    + ("\n".join(skipped_summary) or "  (no datasets inspected)")
                ),
            }

        if chosen_dataset is None or chosen_resource is None:
            details = (
                "\n".join(skipped_summary)
                if skipped_summary
                else "  (no datasets inspected)"
            )
            return {
                "error": True,
                "message": (
                    f"No queryable (datastore_active) resource found among "
                    f"{len(datasets)} matching dataset(s) for query "
                    f"{query!r}.\nSkipped:\n{details}\nTry a different "
                    "keyword or call ckan__get_dataset to inspect resources."
                ),
            }

        resource_id = chosen_resource.get("id")
        if not resource_id:
            return {
                "error": True,
                "message": (
                    f"Chosen resource of dataset {chosen_dataset.get('id')!r}"
                    " has no id."
                ),
            }

        records, fields, total, error = await self._query_with_schema(
            resource_id=resource_id,
            filters=filters or None,
            where=where,
            limit=limit,
        )
        if error:
            return {
                "error": True,
                "message": (
                    f"Found dataset {chosen_dataset.get('id')!r} resource "
                    f"{resource_id!r} but query_data failed: {error}"
                ),
            }

        # Optional: parallel COUNT(*) across every queryable resource of the
        # chosen dataset, so a multi-archive dataset like Boston's 311 can
        # answer "total across all years" in one follow-up call instead of
        # 22 sequential ones.
        sibling_totals: Optional[Dict[str, Optional[int]]] = None
        if include_resource_totals:
            sibling_totals = await self._count_all_queryable(
                chosen_dataset, where=where, filters=filters
            )

        return {
            "dataset": chosen_dataset,
            "resource": chosen_resource,
            "records": records,
            "fields": fields,
            "total": total,
            "alternate_datasets": datasets,
            "auto_picked_resource": auto_picked_resource,
            "sibling_totals": sibling_totals,
        }

    async def _count_all_queryable(
        self,
        dataset: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Optional[int]]:
        """Run COUNT(*) in parallel against every queryable resource of a
        dataset. Returns a {resource_id: int_total or None} dict -- a None
        means the count failed for that resource (e.g. column doesn't exist
        in that archive's schema, common when applying a `where` clause
        that uses NEW SYSTEM column names against a per-year archive)."""
        try:
            where_sql = SafeSQLBuilder.build_where_clause(where) if where else ""
        except ValueError:
            where_sql = ""

        async def count_one(resource: Dict[str, Any]) -> Tuple[str, Optional[int]]:
            rid = resource.get("id") or ""
            try:
                validated_id = SafeSQLBuilder.validate_resource_id(rid)
            except ValueError:
                return rid, None
            n = await self._count_via_sql(validated_id, where_sql, filters)
            return rid, n

        queryables = self._queryable_resources(dataset)
        if not queryables:
            return {}
        import asyncio

        results = await asyncio.gather(
            *(count_one(r) for r in queryables), return_exceptions=False
        )
        return dict(results)

    async def health_check(self) -> bool:
        """Check if CKAN API is accessible.

        Returns:
            True if healthy
        """
        try:
            response = await self._call_ckan_api("status_show", {})
            return response.get("success", False)
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    @staticmethod
    def _iso_now_utc() -> str:
        """UTC ISO-8601 with `Z` suffix used by the Retrieved footer.

        Civic-AI principle: every response ends with a timestamp so a
        downstream caller can tell a fresh fetch from a stale cached one.
        """
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _params_repr(params: Dict[str, Any]) -> str:
        """Render a {k: v} dict as `k1=..., k2=...` for the echoed-Query line.

        Long string values (typically SQL) are tail-truncated so the
        provenance header doesn't dominate the response.
        """
        parts: List[str] = []
        for key, value in params.items():
            if isinstance(value, str) and len(value) > _PARAMS_REPR_MAX:
                value = value[: _PARAMS_REPR_MAX - 3] + "..."
            parts.append(f"{key}={value!r}")
        return ", ".join(parts)

    def _format_provenance_header(
        self,
        source_summary: str,
        calls: List[Tuple[str, Dict[str, Any]]],
    ) -> str:
        """Render the leading `## Source` block.

        Layout (top-down so Copilot summarizers see the load-bearing facts
        first; Copilot C2/C4):

            ## Source
            Source: <human one-liner -- dataset / resource / URL>
            API: POST <action_url>[; POST <action_url>...]
            Query: <echoed params>

        The human-readable `Source:` line is what the model is expected to
        quote when attributing answers. The `API:` / `Query:` lines are for
        reproducibility (civic-AI #1, #2).
        """
        if not source_summary and not calls:
            return ""
        lines: List[str] = ["## Source"]
        if source_summary:
            lines.append(f"Source: {source_summary}")
        if calls:
            base = self.plugin_config.base_url.rstrip("/")
            endpoints = [f"POST {base}/api/3/action/{a}" for a, _ in calls]
            lines.append("API: " + "; ".join(endpoints))
            # Echo each call's params on its own line so a multi-call
            # composite (search_and_query) doesn't blur which params
            # belong to which call.
            for action, params in calls:
                if params:
                    lines.append(f"Query [{action}]: {self._params_repr(params)}")
        return "\n".join(lines)

    @staticmethod
    def _format_critical_reminders(body: str) -> str:
        """Emit a bottom-of-response prose reminder for each critical
        caveat already fired upstream.

        Copilot A3: a caveat that appears once as a structured marker
        is more likely to be dropped by GPT-4o than one that also
        appears in prose. Repeating the load-bearing ones in plain
        language at the bottom is cheap insurance.

        Pulls the trigger from the caller's already-rendered body so we
        never double-fire and stay silent when nothing critical did.
        """
        reminders: List[str] = []
        # Order matches the body's caveat block so the prose reminders
        # read naturally if more than one fires.
        if "APPARENT ABANDONMENT" in body:
            reminders.append(
                "(Reminder: this dataset shows APPARENT ABANDONMENT -- "
                "the stated update cadence and the actual resource "
                "last-modified date diverge. Verify with the publisher "
                "before reporting these values as current.)"
            )
        if "NO UPDATE CADENCE DECLARED" in body:
            reminders.append(
                "(Reminder: the publisher has not declared an update "
                "cadence and the resource has not been touched in over "
                "two years. Treat this as a possibly one-shot snapshot.)"
            )
        if "SINGLE-RECORD CLAIM" in body:
            reminders.append(
                "(Reminder: only ONE record matched. Do not generalize "
                "or treat this as a trend; it is a single anecdote.)"
            )
        if "SQL PASSTHROUGH" in body:
            reminders.append(
                "(Reminder: the SQL above was written by the model, not "
                "selected from a curated set. Verify it matches the "
                "user's actual question before trusting the result.)"
            )
        return "\n".join(reminders)

    def _wrap_response(
        self,
        text: str,
        source_summary: str = "",
        calls: Optional[List[Tuple[str, Dict[str, Any]]]] = None,
    ) -> str:
        """Centralized response wrapper.

        Prepends a `## Source` block (civic-AI #1, #2; Copilot C4) and
        appends a UTC ISO-8601 retrieval timestamp (civic-AI #3). Also
        appends a prose-reminder block for critical caveats that fired
        in the body (Copilot A3). Per-formatter code never has to add
        these by hand.
        """
        header = self._format_provenance_header(source_summary, calls or [])
        reminders = self._format_critical_reminders(text)
        footer = f"_Retrieved: {self._iso_now_utc()}_"
        parts: List[str] = []
        if header:
            parts.append(header)
            parts.append("")
        parts.append(text)
        if reminders:
            parts.append("")
            parts.append(reminders)
        parts.append("")
        parts.append(footer)
        return "\n".join(parts)

    def _package_url(self, dataset_id: str) -> str:
        """Public CKAN package page URL.

        Boston's portal exposes `<portal_url>/dataset/<id-or-slug>`. We use
        the dataset name (slug) when available because it's the stable
        human-readable identifier on the portal; the UUID also resolves
        but reads worse.
        """
        return f"{self.plugin_config.portal_url.rstrip('/')}/dataset/{dataset_id}"

    def _source_summary_for_dataset(
        self,
        dataset: Optional[Dict[str, Any]] = None,
        resource: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Build the human-readable `Source:` one-liner.

        Civic-AI #1: `<dataset title> / resource <id> (<name>) /
        <package URL>` when we have full context; degrades gracefully when
        we have less.
        """
        portal = self.plugin_config.portal_url.rstrip("/")
        if not dataset:
            if resource:
                rid = resource.get("id", "?")
                rname = resource.get("name") or ""
                rname_part = f" ({rname})" if rname else ""
                return f"resource {rid}{rname_part} on {portal}"
            return f"{portal}"
        title = dataset.get("title") or dataset.get("name") or "unknown dataset"
        slug = dataset.get("name") or dataset.get("id") or ""
        url = self._package_url(slug) if slug else portal
        if resource:
            rid = resource.get("id", "?")
            rname = resource.get("name") or ""
            rname_part = f" ({rname})" if rname else ""
            return f"{title} / resource {rid}{rname_part} / {url}"
        return f"{title} / {url}"

    @staticmethod
    def _format_sample_size_caveat(
        total: Optional[int],
        unit: str = "row",
    ) -> str:
        """Emit a SINGLE-RECORD or SMALL SAMPLE banner so the model doesn't
        pattern-match a tiny result into a trend.

        Returns ``""`` when the sample is large enough not to warrant a
        warning, or when ``total`` is unknown (we'd rather stay silent than
        false-alarm). Civic-AI principles #5 and #6.
        """
        if total is None:
            return ""
        if total == 1:
            return (
                "=== SINGLE-RECORD CLAIM (N=1) ===\n"
                f"Exactly 1 {unit} matched. Do NOT report this as a "
                "trend, average, or general pattern -- it's an N=1 "
                "anecdote. Quote the record directly or say so when "
                "summarizing.\n"
                "================================="
            )
        if 1 < total <= _SMALL_SAMPLE_MAX:
            return (
                "=== SMALL SAMPLE ===\n"
                f"Only {total} {unit}(s) matched. Treat any aggregate "
                "or rate calculated from this as a small-sample estimate "
                "and call out the sample size when summarizing.\n"
                "===================="
            )
        return ""

    @staticmethod
    def _parse_ckan_iso(value: Optional[str]) -> Optional[datetime]:
        """Parse the timestamp shapes CKAN returns.

        Handles naive ISO with microseconds ("2021-04-14T12:34:56.789012"),
        trailing-Z UTC, and plain dates. Returns ``None`` on anything else
        so callers can degrade silently.
        """
        if not value or not isinstance(value, str):
            return None
        try:
            iso = value.rstrip("Z")
            parsed = datetime.fromisoformat(iso)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _frequency_days(frequency: Optional[str]) -> Optional[int]:
        """Translate a CKAN-declared `frequency` string to a day count.

        Returns ``None`` for unset / unrecognized values so the abandonment
        detector can stay silent rather than guess. Substring match because
        portals are inconsistent ("Weekly", "Updated Weekly", "weekly cadence").
        """
        if not frequency or not isinstance(frequency, str):
            return None
        f = frequency.strip().lower()
        if not f or f in ("as needed", "as-needed", "irregular", "one-time"):
            return None
        for key, days in _FREQUENCY_DAYS.items():
            if key in f:
                return days
        return None

    @classmethod
    def _format_freshness_caveat(
        cls,
        dataset: Optional[Dict[str, Any]] = None,
        resource: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Surface freshness caveats for the chosen dataset/resource.

        Renders, in this order (whichever apply):

          1. **APPARENT ABANDONMENT** (civic-AI #5): declared frequency is
             "weekly"/"daily"/etc. but the resource hasn't been touched in
             more than 4x that interval.
          2. **NO UPDATE CADENCE DECLARED** (civic-AI #6): frequency unset
             and resource > 2 years old.
          3. **DATA FRESHNESS** (civic-AI #4 + legacy): dual timestamps
             showing metadata_modified vs resource.last_modified, with a
             warning when they diverge by more than a year.

        Best-effort: missing fields are skipped rather than guessed.
        """
        if dataset is None and resource is None:
            return ""
        dataset = dataset or {}
        # If no explicit resource passed, fall back to the first queryable
        # one so get_dataset / search_and_query can both call this with
        # what they have.
        if resource is None:
            for r in dataset.get("resources") or []:
                if r.get("datastore_active"):
                    resource = r
                    break
            else:
                resource = (dataset.get("resources") or [None])[0]
        resource = resource or {}

        meta_mod = cls._parse_ckan_iso(dataset.get("metadata_modified"))
        res_mod = cls._parse_ckan_iso(
            resource.get("last_modified") or resource.get("revision_timestamp")
        )
        now = datetime.now(timezone.utc)
        frequency_text = dataset.get("frequency") or dataset.get("update_frequency")
        freq_days = cls._frequency_days(frequency_text)

        blocks: List[str] = []

        # 1. Abandonment detector
        if (
            res_mod is not None
            and freq_days is not None
            and (now - res_mod).days > freq_days * _ABANDONMENT_INTERVAL_MULTIPLIER
        ):
            age_days = (now - res_mod).days
            interval_label = (
                f"{age_days} days" if age_days < 365 else f"{age_days / 365:.1f} years"
            )
            blocks.append(
                "[APPARENT ABANDONMENT]\n"
                f"Dataset declares {frequency_text!r} updates but the "
                f"resource was last modified "
                f"{res_mod.strftime('%Y-%m-%d')} ({interval_label} ago). "
                f"Expected cadence: every ~{freq_days} days. The data is "
                "likely stale; verify with the publisher before relying "
                "on it for current questions.\n"
                "[/APPARENT ABANDONMENT]"
            )

        # 2. No-frequency note
        elif (
            res_mod is not None
            and freq_days is None
            and (now - res_mod).days > _NO_FREQUENCY_OLD_DAYS
        ):
            years = (now - res_mod).days / 365.0
            blocks.append(
                "[NO UPDATE CADENCE DECLARED]\n"
                f"Update cadence not declared; resource last modified "
                f"{res_mod.strftime('%Y-%m-%d')} ({years:.1f} years "
                "ago). Cannot tell if this is current or a one-time "
                "snapshot.\n"
                "[/NO UPDATE CADENCE DECLARED]"
            )

        # 3. Dual-timestamp DATA FRESHNESS card.
        #
        # Fires when:
        #   - the resource itself is older than a year, OR
        #   - the resource timestamp and the metadata timestamp diverge
        #     by more than a year (description edits papering over data
        #     that hasn't moved), OR
        #   - the resource timestamp is missing but metadata_modified is
        #     itself stale (degraded signal -- still better than silence).
        res_old = res_mod is not None and (now - res_mod).days > _STALE_DATASET_DAYS
        divergent = (
            res_mod is not None
            and meta_mod is not None
            and abs((meta_mod - res_mod).days) > _STALE_DATASET_DAYS
        )
        meta_only_old = (
            res_mod is None
            and meta_mod is not None
            and (now - meta_mod).days > _STALE_DATASET_DAYS
        )
        if res_old or divergent or meta_only_old:
            lines = ["[DATA FRESHNESS]"]
            if res_mod is not None:
                age = (now - res_mod).days
                lines.append(
                    f"Data last updated: {res_mod.strftime('%Y-%m-%d')} "
                    f"(resource, {age} days ago)."
                )
            if meta_mod is not None:
                age = (now - meta_mod).days
                lines.append(
                    f"Metadata last touched: "
                    f"{meta_mod.strftime('%Y-%m-%d')} "
                    f"(description/tags, {age} days ago -- "
                    "may not reflect data changes)."
                )
            if divergent:
                lines.append(
                    "WARNING: these timestamps diverge by more than a "
                    "year. The metadata says recent, the data underneath "
                    "is stale. Use the resource timestamp when judging "
                    "currency."
                )
            lines.append(
                "Treat this as a historical snapshot, not current state. "
                "If the user asked about 'now' / 'today' / 'currently', "
                "say so explicitly."
            )
            lines.append("[/DATA FRESHNESS]")
            blocks.append("\n".join(lines))

        return "\n\n".join(blocks)

    @staticmethod
    def _looks_like_date(values: List[Any]) -> bool:
        """True when most sampled string values match a date-ish shape.

        Accepts ISO 8601 ("2024-06-15"), US format ("06/15/2024"), and the
        ISO datetime forms. 80% threshold so a column with the odd typo
        still trips the check.
        """
        if not values:
            return False
        date_re = re.compile(
            r"^\s*\d{4}-\d{2}-\d{2}"  # ISO
            r"|^\s*\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}"  # US or DMY
            r"|^\s*[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}"  # named month
        )
        matches = sum(1 for v in values if isinstance(v, str) and date_re.match(v))
        return matches / max(len(values), 1) >= 0.8

    @staticmethod
    def _looks_like_number(values: List[Any]) -> bool:
        """True when most sampled string values are numeric-looking.

        Skips dates (the date check should run first) -- a "20240615"
        string would otherwise be misclassified as a number; the date
        regex catches dashed forms before this is consulted.
        """
        if not values:
            return False
        num_re = re.compile(r"^\s*-?\d+(?:\.\d+)?\s*$")
        matches = sum(1 for v in values if isinstance(v, str) and num_re.match(v))
        return matches / max(len(values), 1) >= 0.8

    @classmethod
    def _format_stringly_typed_caveat(
        cls,
        records: List[Dict[str, Any]],
        fields: Optional[List[Dict[str, Any]]],
    ) -> str:
        """Emit a TYPE NOTE banner when TEXT fields hold date- or number-
        shaped values. Civic-AI #9.

        Pulls sampled values from the actual returned rows so we don't
        false-alarm on TEXT columns that genuinely hold text. Best-effort;
        if records or fields are missing, returns "".
        """
        if not records or not fields:
            return ""
        text_types = {"text", "string", "varchar"}
        candidates = [
            f
            for f in fields
            if (f.get("type") or "").lower() in text_types
            and f.get("id")
            and f.get("id") != "_id"
        ]
        if not candidates:
            return ""
        notes: List[str] = []
        for f in candidates:
            fid = f["id"]
            sampled = [r.get(fid) for r in records if r.get(fid) not in (None, "")]
            # Need a minimum sample so a single value doesn't trip the
            # check. Three matches the small-sample threshold elsewhere.
            if len(sampled) < 3:
                continue
            if cls._looks_like_date(sampled):
                notes.append(
                    f"  - {fid!r} is stored as TEXT but values look like "
                    "dates. Comparisons like > '2024-01-01' will be "
                    "STRING comparison, not date comparison; ORDER BY "
                    "this column will sort alphabetically. Cast in SQL "
                    f'(e.g. ("{fid}")::timestamp) before comparing '
                    "or ordering."
                )
            elif cls._looks_like_number(sampled):
                notes.append(
                    f"  - {fid!r} is stored as TEXT but values look like "
                    "numbers. Comparisons (> < BETWEEN) will be string "
                    "comparison ('10' < '2'); ORDER BY this column will "
                    f'sort lexicographically. Cast (e.g. ("{fid}")::'
                    "numeric) before comparing or aggregating."
                )
        if not notes:
            return ""
        return (
            "[STRINGLY-TYPED FIELDS]\n"
            + "\n".join(notes)
            + ("\n[/STRINGLY-TYPED FIELDS]")
        )

    @staticmethod
    def _is_null_like(value: Any) -> bool:
        """True when a value is one of CKAN's six common null
        representations (civic-AI #10): actual null, "", "N/A", "Unknown",
        "None", "NULL"."""
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip().lower() in _NULL_LIKE_STRINGS
        return False

    @classmethod
    def _render_null_like(cls, value: Any) -> str:
        """Render a null-like value distinctly so the model doesn't treat
        the string 'Unknown' as a real category.

        Real nulls become `<null>`, empty strings `<empty>`, and other
        null-like literals become `<"<original>">` -- the quoting makes
        clear the literal *value* is sentinel, not real content.
        """
        if value is None:
            return "<null>"
        if isinstance(value, str):
            text = value.strip()
            if text == "":
                return "<empty>"
            return f'<"{value}">'
        return str(value)

    @classmethod
    def _normalize_value_for_display(
        cls,
        value: Any,
        field_type: Optional[str],
    ) -> str:
        """Render a single cell for record preview.

        For timestamp/date columns we coerce to ISO 8601. Midnight-aligned
        timestamps render as date-only so a date column doesn't grow a
        meaningless `T00:00:00`. NULL-likes are rendered distinctly
        (civic-AI #10) so the model can tell "Unknown" the category apart
        from missing data.
        """
        if cls._is_null_like(value):
            return cls._render_null_like(value)
        if not field_type:
            return str(value)
        ftype = field_type.lower()
        if ftype in ("timestamp", "timestamptz", "date"):
            parsed = cls._parse_timestamp(value)
            if parsed is None:
                return str(value)
            if (
                parsed.hour == 0
                and parsed.minute == 0
                and parsed.second == 0
                and parsed.microsecond == 0
            ):
                return parsed.strftime("%Y-%m-%d")
            return parsed.strftime("%Y-%m-%dT%H:%M:%S")
        return str(value)

    @classmethod
    def _format_null_like_frequency_caveat(
        cls,
        records: List[Dict[str, Any]],
        fields: Optional[List[Dict[str, Any]]],
    ) -> str:
        """Civic-AI #11: when >20% of a column's returned values are
        null-like, emit a DATA QUALITY note naming the field and its
        missing-rate."""
        if not records or not fields:
            return ""
        usable_fields = [f for f in fields if f.get("id") and f.get("id") != "_id"]
        offenders: List[Tuple[str, float]] = []
        n = len(records)
        for f in usable_fields:
            fid = f["id"]
            missing = sum(1 for r in records if cls._is_null_like(r.get(fid)))
            ratio = missing / n if n else 0.0
            if ratio >= _NULL_LIKE_FREQ_THRESHOLD:
                offenders.append((fid, ratio))
        if not offenders:
            return ""
        # Sort by missing-rate descending so the worst offenders surface
        # first when there are several.
        offenders.sort(key=lambda x: x[1], reverse=True)
        lines = ["[DATA QUALITY]"]
        for fid, ratio in offenders:
            pct = int(round(ratio * 100))
            lines.append(
                f"  - {fid!r} is empty / null / 'N/A' / 'Unknown' in "
                f"{pct}% of returned records. Treat aggregations on this "
                "field with care: high missing-rate."
            )
        lines.append("[/DATA QUALITY]")
        return "\n".join(lines)

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[datetime]:
        """Best-effort parse of upstream timestamp values.

        CKAN/Postgres returns ISO strings but Boston's portal also ships
        epoch-ms integers in some resources. Handle both; return None if
        neither shape matches.
        """
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            try:
                # Heuristic: > 10^11 => ms, otherwise seconds.
                ts = value / 1000.0 if value > 1e11 else float(value)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    @staticmethod
    def _field_types_by_id(
        fields: Optional[List[Dict[str, Any]]],
    ) -> Dict[str, str]:
        """Build {field_id: type} lookup used by record-display normalization."""
        if not fields:
            return {}
        return {f.get("id"): (f.get("type") or "") for f in fields if f.get("id")}

    def _format_record_lines(
        self,
        record: Dict[str, Any],
        field_types: Dict[str, str],
    ) -> List[str]:
        """Render one record's fields with date normalization applied.

        Skips CKAN's internal `_id` column the same way the legacy formatters
        did, so output is unchanged for non-date columns.
        """
        out: List[str] = []
        for key, value in record.items():
            if key == "_id":
                continue
            rendered = self._normalize_value_for_display(value, field_types.get(key))
            out.append(f"  {key}: {rendered}")
        return out

    @staticmethod
    def _validate_field_names(
        provided: List[str],
        schema_fields: Optional[List[Dict[str, Any]]],
        context: str,
    ) -> Optional[str]:
        """Pre-validate caller-supplied field names against the resource's
        schema and return a 'did you mean?' error message for any typo.

        Best-effort (civic-AI #15): if we couldn't fetch a schema, returns
        ``None`` and lets the upstream API do its own (cryptic) error. The
        upstream's complaint isn't useful to an LLM, so when we *do* have
        the schema we'd rather catch it here.
        """
        if not schema_fields:
            return None
        known = {f.get("id") for f in schema_fields if f.get("id")}
        if not known:
            return None
        unknown = [name for name in provided if name and name not in known]
        if not unknown:
            return None
        suggestions: List[str] = []
        known_list = list(known)
        for name in unknown:
            close = difflib.get_close_matches(name, known_list, n=1, cutoff=0.6)
            if close:
                suggestions.append(f"'{name}' (did you mean '{close[0]}'?)")
            else:
                suggestions.append(f"'{name}'")
        return (
            f"Unknown field name(s) in {context}: "
            f"{', '.join(suggestions)}. Valid columns: "
            f"{', '.join(sorted(known_list))}."
        )

    async def _schema_fields_safe(
        self, resource_id: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch the resource's field list, swallowing any error.

        Used by the pre-flight field validator (civic-AI #9). If this fails
        we degrade silently -- false silences beat false alarms.
        """
        try:
            return await self.get_schema(resource_id)
        except Exception:  # noqa: BLE001 -- best-effort by design
            return None

    @staticmethod
    def _collect_field_refs(
        where: Optional[Dict[str, Any]],
        filters: Optional[Dict[str, Any]],
        group_by: Optional[List[str]] = None,
        metrics: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """Extract every column name the caller wants to reference in a
        single call, so the field validator can check them all at once."""
        refs: List[str] = []
        if where:
            refs.extend(k for k in where.keys() if isinstance(k, str))
        if filters:
            refs.extend(k for k in filters.keys() if isinstance(k, str))
        if group_by:
            refs.extend(k for k in group_by if isinstance(k, str))
        if metrics:
            # Metric expressions look like 'count(*)' or 'avg(field)' -- pull
            # the column name out of the parens and skip count(*) since
            # there's nothing to validate.
            metric_re = re.compile(
                r"^\s*[a-zA-Z_]+\s*\(\s*(?:distinct\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$",
                re.IGNORECASE,
            )
            for expr in metrics.values():
                if not isinstance(expr, str):
                    continue
                m = metric_re.match(expr)
                if m:
                    refs.append(m.group(1))
        return refs

    @staticmethod
    def _format_search_ambiguity_caveat(
        datasets: List[Dict[str, Any]],
    ) -> str:
        """Warn when search_datasets returned multiple plausible matches
        for a topic (civic-AI #14).

        Heuristic: fires when >= 2 datasets share a non-trivial title-token
        with the top result (e.g. 'crime', 'crime incident report', 'crime
        stats summary'). Without this warning, the model silently picks
        the first hit and reports it as canonical.
        """
        if not datasets or len(datasets) < 2:
            return ""
        # Build a token set from the top dataset's title (skipping short
        # stopwords); count how many other datasets share at least one
        # of those tokens.
        stop = {"of", "and", "the", "in", "for", "to", "by", "on", "a", "an"}

        def tokens(text: str) -> set:
            return {
                t.lower()
                for t in re.findall(r"[A-Za-z]{3,}", text or "")
                if t.lower() not in stop
            }

        top_tokens = tokens(datasets[0].get("title") or "")
        if not top_tokens:
            return ""
        plausible: List[str] = []
        for ds in datasets[1:5]:  # cap at 4 alternates so the banner stays brief
            ds_tokens = tokens(ds.get("title") or "")
            if top_tokens & ds_tokens:
                plausible.append(
                    f"  - {ds.get('title') or '(untitled)'} "
                    f"(dataset_id={ds.get('id', '?')})"
                )
        if not plausible:
            return ""
        return (
            "[AMBIGUOUS SEARCH]\n"
            f"Multiple datasets match this topic; the top hit "
            f"({datasets[0].get('title') or '?'!r}) is one of "
            f"{1 + len(plausible)} plausible candidates. Do NOT silently "
            "pick the first one. Either show the user the alternates "
            "below, or call ckan__search_and_query with each candidate "
            "individually to compare. Plausible alternates:\n"
            + "\n".join(plausible)
            + "\n[/AMBIGUOUS SEARCH]"
        )

    def _format_search_results(
        self,
        datasets: List[Dict[str, Any]],
        total: Optional[int] = None,
        limit: int = 20,
    ) -> str:
        """Format search results for user display."""
        if not datasets:
            return (
                f"No datasets matched in {self.plugin_config.city_name}'s "
                "open data portal.\n"
                "NOTE: zero results does NOT mean the data doesn't exist. "
                "Try a broader keyword, a synonym, or check the portal "
                f"directly at {self.plugin_config.portal_url}."
            )

        suggested_resource_id: Optional[str] = None
        suggested_dataset_id: Optional[str] = None
        for ds in datasets:
            queryable = self._first_queryable_resource(ds)
            if queryable and queryable.get("id"):
                suggested_resource_id = queryable.get("id")
                suggested_dataset_id = ds.get("id")
                break

        lines: List[str] = []

        # Civic-AI #14: when multiple plausible datasets matched, warn
        # before the listing so the model doesn't silently pick the first.
        ambiguity_caveat = self._format_search_ambiguity_caveat(datasets)
        if ambiguity_caveat:
            lines.append("## Caveats")
            lines.append(ambiguity_caveat)
            lines.append("")

        if suggested_resource_id:
            lines.extend(
                [
                    "=== NEXT STEP (read this first) ===",
                    f"suggested_resource_id: {suggested_resource_id}",
                    "suggested_next_tool: ckan__query_data",
                    f'suggested_call: ckan__query_data(resource_id="{suggested_resource_id}")',
                    "(this is the datastore-loaded resource -- only such "
                    "resources can be queried; others are file downloads.)",
                    "(or use ckan__search_and_query for a one-call "
                    "keyword-to-data flow.)",
                    "===================================",
                    "",
                ]
            )
        else:
            lines.extend(
                [
                    "=== NEXT STEP ===",
                    "None of the matched datasets have a queryable resource "
                    "(datastore_active=true). The attached resources are "
                    "file downloads only. Try a different search keyword, "
                    "or call ckan__get_dataset to inspect non-datastore "
                    "resources.",
                    "=================",
                    "",
                ]
            )

        # Lead with the X-of-Y framing so the model can't mistake the
        # results-shown count for the count of matching datasets.
        n_returned = len(datasets)
        if total is not None and total > n_returned:
            lines.append(
                f"{n_returned} of {total} matching dataset(s) shown "
                f"(limit={limit}; raise limit to see more) in "
                f"{self.plugin_config.city_name}'s open data portal:\n"
            )
        elif total is not None:
            lines.append(
                f"{total} matching dataset(s) (full result, limit={limit}) "
                f"in {self.plugin_config.city_name}'s open data portal:\n"
            )
        else:
            lines.append(
                f"{n_returned} dataset(s) in "
                f"{self.plugin_config.city_name}'s open data portal:\n"
            )

        for i, dataset in enumerate(datasets, 1):
            title = dataset.get("title", "Untitled")
            dataset_id = dataset.get("id", "unknown")
            notes = (
                dataset.get("notes", "")[:100] + "..."
                if dataset.get("notes")
                else "No description"
            )
            resources = dataset.get("resources") or []
            queryable = self._first_queryable_resource(dataset)
            queryable_id = queryable.get("id") if queryable else None
            queryable_format = queryable.get("format") if queryable else None

            lines.append(f"{i}. {title}")
            lines.append(f"   dataset_id: {dataset_id}")
            if queryable_id:
                fmt = f" [{queryable_format}]" if queryable_format else ""
                lines.append(
                    f"   resource_id (use this with ckan__query_data){fmt}: "
                    f"{queryable_id}"
                )
            elif resources:
                lines.append(
                    f"   resource_id: NONE QUERYABLE -- this dataset has "
                    f"{len(resources)} resource(s) but none are loaded into "
                    "the datastore (datastore_active=false). Use "
                    "ckan__get_dataset for download URLs."
                )
            lines.append(f"   Description: {notes}")
            lines.append(
                f"   Portal: {self.plugin_config.portal_url}/dataset/{dataset_id}"
            )
            lines.append("")

        lines.append(
            f"View all datasets at: {self.plugin_config.portal_url}\n"
            f"Use ckan__get_dataset with a dataset_id (above) for full resource details, "
            f"or ckan__query_data with a resource_id to fetch rows."
        )
        if suggested_dataset_id:
            # Hint for narrative chaining: makes the dataset_id discoverable too.
            lines.append(f"suggested_dataset_id: {suggested_dataset_id}")

        return "\n".join(lines)

    def _format_dataset(self, dataset: Dict[str, Any]) -> str:
        """Format dataset metadata for user display."""
        title = dataset.get("title", "Untitled")
        dataset_id = dataset.get("id", "unknown")
        notes = dataset.get("notes", "No description")
        organization = dataset.get("organization", {}).get("title", "Unknown")
        resources = dataset.get("resources", []) or []

        queryable = self._first_queryable_resource(dataset)
        suggested_resource_id = queryable.get("id") if queryable else None

        lines: List[str] = []
        if suggested_resource_id:
            lines.extend(
                [
                    "=== NEXT STEP (read this first) ===",
                    f"suggested_resource_id: {suggested_resource_id}",
                    "suggested_next_tool: ckan__query_data",
                    f'suggested_call: ckan__query_data(resource_id="{suggested_resource_id}")',
                    "(this is the datastore-loaded resource; the others are "
                    "file downloads only.)",
                    "===================================",
                    "",
                ]
            )
        elif resources:
            lines.extend(
                [
                    "=== NEXT STEP ===",
                    f"This dataset has {len(resources)} resource(s) but none "
                    "are loaded into the datastore (datastore_active=false), "
                    "so ckan__query_data will not work on them. They are "
                    "file downloads -- see URLs below.",
                    "=================",
                    "",
                ]
            )

        # Pick the queryable resource (or first if none) for freshness math.
        freshness_resource = (
            self._first_queryable_resource(dataset)
            or (dataset.get("resources") or [None])[0]
        )
        freshness_caveat = self._format_freshness_caveat(
            dataset=dataset, resource=freshness_resource
        )
        if freshness_caveat:
            lines.append(freshness_caveat)
            lines.append("")

        lines.extend(
            [
                f"Dataset: {title}",
                f"dataset_id: {dataset_id}",
                f"Organization: {organization}",
                f"Description: {notes}",
                "",
                f"Portal URL: {self.plugin_config.portal_url}/dataset/{dataset_id}",
                "",
            ]
        )

        if resources:
            lines.append(f"Resources ({len(resources)}):")
            for i, resource in enumerate(resources, 1):
                res_name = resource.get("name", "Unnamed")
                res_id = resource.get("id", "unknown")
                res_format = resource.get("format", "unknown")
                res_url = resource.get("url", "")
                queryable_flag = self._is_queryable(resource)
                marker = "QUERYABLE" if queryable_flag else "DOWNLOAD-ONLY"
                lines.append(f"  {i}. [{marker}] {res_name} ({res_format})")
                lines.append(f"     resource_id: {res_id}")
                if queryable_flag:
                    lines.append(
                        f'     Use ckan__query_data with resource_id="{res_id}" to fetch rows.'
                    )
                else:
                    if res_url:
                        lines.append(f"     download_url: {res_url}")
                    lines.append(
                        "     (not loaded into datastore -- ckan__query_data "
                        "will return 404 for this resource_id)"
                    )
        else:
            lines.append(
                "No resources available for this dataset. Try a different "
                "dataset_id or use ckan__search_datasets again."
            )

        return "\n".join(lines)

    def _format_query_results(
        self,
        records: List[Dict[str, Any]],
        fields: Optional[List[Dict[str, Any]]] = None,
        limit: int = 100,
        total: Optional[int] = None,
    ) -> str:
        """Format query results for user display.

        Layout (Copilot C2: caveats lead, records follow):

            ## Caveats           (only if any fire)
              TRUNCATED / SAMPLE / STRINGLY-TYPED / DATA QUALITY
            ## Records           (always, or empty-state message)
              header + records
            ## Schema            (Filterable columns footer)
        """
        n_returned = len(records)
        truncated_warning = self._format_truncation_block(n_returned, limit, total)
        sample_caveat = self._format_sample_size_caveat(total, unit="row")
        stringly_caveat = self._format_stringly_typed_caveat(records, fields)
        null_caveat = self._format_null_like_frequency_caveat(records, fields)
        field_types = self._field_types_by_id(fields)

        caveats = [
            c
            for c in (truncated_warning, sample_caveat, stringly_caveat, null_caveat)
            if c
        ]

        parts: List[str] = []
        if caveats:
            parts.append("## Caveats")
            parts.extend(caveats)
            parts.append("")

        parts.append("## Records")
        if not records:
            parts.append(
                "No records matched the query.\n"
                "NOTE: zero records does NOT mean zero data exists. Verify "
                "that the column names in `where` / `filters` are correct "
                "(see schema below), that the date range covers the right "
                "period, and that the filter values match the dataset's "
                "vocabulary (e.g. 'Closed' vs 'closed' vs 'CLOSED')."
            )
            schema_footer = self._format_schema_footer(fields)
            if schema_footer:
                parts.append("")
                parts.append(schema_footer)
            return "\n".join(parts)

        parts.append(self._format_count_header(n_returned, limit, total))
        # Repeat the "don't generalize" directive when the sample is
        # capped (Copilot C3 + C10: redundancy is cheap, dropped caveats
        # are expensive).
        if total is not None and total > n_returned:
            parts.append(
                "(Reminder: the records below are a SAMPLE -- do NOT "
                "generalize counts or percentages from them. The TOTAL "
                f"is {total}, see the line above.)"
            )
        parts.append("")

        for i, record in enumerate(records[:5], 1):
            parts.append(f"Record {i}:")
            parts.extend(self._format_record_lines(record, field_types))
            parts.append("")

        if n_returned > 5:
            parts.append(f"... and {n_returned - 5} more record(s) returned")

        schema_footer = self._format_schema_footer(fields)
        if schema_footer:
            parts.append("")
            parts.append(schema_footer)

        return "\n".join(parts)

    @staticmethod
    def _format_count_header(
        n_returned: int,
        limit: int,
        total: Optional[int],
        unit: str = "rows",
    ) -> str:
        """One-line "X of Y" summary used at the top of every row-returning
        response. The model has been mistaking returned-rows for true count;
        this phrasing makes the partial/total distinction unambiguous."""
        if total is not None and total > n_returned:
            return (
                f"{n_returned} of {total} {unit} returned "
                f"(limit={limit}; raise limit or use ckan__aggregate_data "
                "for full count)."
            )
        if total is not None:
            # total == n_returned, all rows returned
            return f"{total} {unit} returned (full result, limit={limit})."
        # total unknown
        return (
            f"{n_returned} {unit} returned (limit={limit}, "
            "true total unknown -- see TRUNCATED warning above if any)."
        )

    def _format_truncation_block(
        self,
        n_returned: int,
        limit: int,
        total: Optional[int],
    ) -> str:
        """Emit a clear warning when the result set is -- or might be --
        truncated by LIMIT.

        Returns ``""`` when no warning is needed (result fits within limit
        and total is known/equal to returned)."""
        # Total known and matches returned -> fits within limit, no warning.
        if total is not None and total <= n_returned:
            return ""

        # Total known but exceeds returned -> exact truncation, exact total.
        if total is not None and total > n_returned:
            return (
                "=== TRUNCATED ===\n"
                f"This query has {total} matching rows, but only "
                f"{n_returned} were returned (limit={limit}). For "
                "counting questions, the answer is "
                f"{total}, NOT {n_returned}. To return more rows, raise "
                "`limit` (max 10000) or use `ckan__execute_sql` with "
                "your own LIMIT/ORDER BY. For just the count, use "
                "ckan__aggregate_data with metrics="
                '{"count": "count(*)"} and a matching filter -- '
                "it's cheaper than fetching rows.\n"
                "================="
            )

        # Total unknown and we hit the limit exactly -> likely truncated.
        if total is None and n_returned >= limit:
            return (
                "=== MAY BE TRUNCATED ===\n"
                f"Result returned exactly the requested limit "
                f"({limit}) and the true total could not be determined. "
                "Treat this as a possibly-incomplete sample. For "
                "counting questions, do NOT report "
                f"{n_returned} as the answer -- use ckan__aggregate_data "
                'with metrics={"count": "count(*)"} and the same '
                "filter, or re-run with a higher limit.\n"
                "========================"
            )

        return ""

    def _format_schema_footer(self, fields: Optional[List[Dict[str, Any]]]) -> str:
        """Render a per-call 'Filterable columns' block listing every field
        the model can pass to ``where``, ``filters``, or reference in
        ``execute_sql``.

        We surface this on every successful row-returning call so the next
        pivot (e.g. 'now filter by close_date') is a one-shot."""
        if not fields:
            return ""
        usable = [f for f in fields if f.get("id") and f.get("id") != "_id"]
        if not usable:
            return ""
        lines = [
            "Filterable columns (use these names in `where`, `filters`, "
            "or `execute_sql`):"
        ]
        for f in usable:
            fid = f.get("id")
            ftype = f.get("type", "?")
            lines.append(f"  - {fid} ({ftype})")
        return "\n".join(lines)

    def _format_schema(self, fields: List[Dict[str, Any]]) -> str:
        """Format schema information for user display."""
        if not fields:
            return "No schema information available."

        lines = ["Schema fields:"]
        for field in fields:
            field_id = field.get("id", "unknown")
            field_type = field.get("type", "unknown")
            field_info = field.get("info", {})
            description = field_info.get("label", "") if field_info else ""

            lines.append(f"  * {field_id} ({field_type})")
            if description:
                lines.append(f"    {description}")

        return "\n".join(lines)

    def _format_search_and_query(self, composite: Dict[str, Any], limit: int) -> str:
        """Format a search_and_query composite result for user display."""
        dataset = composite.get("dataset", {}) or {}
        resource = composite.get("resource", {}) or {}
        records = composite.get("records", []) or []
        fields = composite.get("fields", []) or []
        total = composite.get("total")
        alternates = composite.get("alternate_datasets", []) or []
        auto_picked = bool(composite.get("auto_picked_resource"))
        sibling_totals: Optional[Dict[str, Optional[int]]] = composite.get(
            "sibling_totals"
        )

        dataset_id = dataset.get("id", "unknown")
        dataset_title = dataset.get("title", "Untitled")
        resource_id = resource.get("id", "unknown")
        resource_name = resource.get("name", "Unnamed")
        n_returned = len(records)

        count_line = self._format_count_header(n_returned, limit, total)

        lines: List[str] = []

        # PARTIAL warning fires when the model auto-picked one of N
        # queryable resources. Without this, GPT-4o tends to read the
        # one-resource answer as canonical and report it as the dataset
        # total. (Pre-enhancement behavior was actually better here:
        # the model was forced into search_datasets->get_dataset->iterate
        # query_data, which surfaced all resources naturally.)
        sibling_queryables = self._queryable_resources(dataset)
        siblings = [r for r in sibling_queryables if r.get("id") != resource_id]
        if auto_picked and siblings:
            n_total_resources = len(sibling_queryables)
            partial_block = (
                "=== PARTIAL DATASET ANSWER ===\n"
                f"This dataset has {n_total_resources} queryable resources "
                f"(e.g. a rolling current view + per-year archives). "
                f"This response covers ONE of them: "
                f"'{resource_name}'.\n\n"
                "If the user's question was about a TOTAL across all "
                "resources (e.g. 'how many ever', 'in total', "
                "'across all years'), this answer is INCOMPLETE. Options:\n"
                "  - Re-call ckan__search_and_query with "
                "include_resource_totals=true to get a per-resource "
                "row count breakdown in one call.\n"
                "  - Pick a specific archive with resource_name=<...> "
                '(e.g. resource_name="2018").\n'
                "  - Use ckan__execute_sql with UNION ALL across the "
                "resource UUIDs listed below.\n"
                "If the user's question was about RECENT data, this "
                "rolling/current resource is likely fine.\n"
                "==============================="
            )
            lines.append(partial_block)
            lines.append("")

        truncated_warning = self._format_truncation_block(n_returned, limit, total)
        sample_caveat = self._format_sample_size_caveat(total, unit="row")
        stringly_caveat = self._format_stringly_typed_caveat(records, fields)
        null_caveat = self._format_null_like_frequency_caveat(records, fields)
        freshness_caveat = self._format_freshness_caveat(
            dataset=dataset, resource=resource
        )

        composite_caveats = [
            c
            for c in (
                freshness_caveat,
                truncated_warning,
                sample_caveat,
                stringly_caveat,
                null_caveat,
            )
            if c
        ]
        if composite_caveats:
            lines.append("## Caveats")
            for caveat in composite_caveats:
                lines.append(caveat)
                lines.append("")

        lines.extend(
            [
                "=== search_and_query result ===",
                f"matched_dataset: {dataset_title}",
                f"dataset_id: {dataset_id}",
                f"resource_id (use with ckan__query_data): {resource_id}",
                f"resource_name: {resource_name}",
                count_line,
                "================================",
                "",
            ]
        )

        # When include_resource_totals=true was requested, lead with the
        # cross-resource breakdown -- this is the answer to "total"
        # questions and should be the most prominent thing.
        if sibling_totals is not None:
            grand: Optional[int] = 0
            per_resource_lines: List[str] = []
            had_failure = False
            # Order by package_show order (rolling view first, then
            # per-year archives) so the model can read it sequentially.
            for r in sibling_queryables:
                rid = r.get("id") or ""
                rname = r.get("name") or "(unnamed)"
                n = sibling_totals.get(rid)
                if n is None:
                    per_resource_lines.append(f"  - {rname}: n=null (count failed)")
                    had_failure = True
                else:
                    per_resource_lines.append(f"  - {rname}: {n}")
                    if grand is not None:
                        grand += n
            grand_line = (
                f"GRAND TOTAL across {len(sibling_queryables)} resources: "
                f"{grand}"
                + (
                    " (some resources returned null -- sum is partial)"
                    if had_failure
                    else ""
                )
            )
            lines.append("=== Per-resource totals ===")
            lines.append(grand_line)
            lines.extend(per_resource_lines)
            lines.append("===========================")
            lines.append("")

        if not records:
            lines.append(
                "No rows returned. Try broadening filters or pick a different "
                "dataset/resource (see alternates below)."
            )
        else:
            if total is not None and total > n_returned:
                preview_caption = (
                    f"Showing first 5 of {n_returned} returned (true total: {total}):"
                )
            else:
                preview_caption = f"Showing first 5 of {n_returned} returned:"
            lines.append(preview_caption)
            field_types = self._field_types_by_id(fields)
            for i, record in enumerate(records[:5], 1):
                lines.append(f"Record {i}:")
                lines.extend(self._format_record_lines(record, field_types))
                lines.append("")
            if n_returned > 5:
                lines.append(f"... and {n_returned - 5} more record(s) returned")

        schema_footer = self._format_schema_footer(fields)
        if schema_footer:
            lines.append("")
            lines.append(schema_footer)

        # Sibling queryable resources within the SAME dataset. Boston's 311
        # dataset has 22 (a rolling view + per-year archives back to 2011)
        # -- without this block the model can't see them. Only render here
        # if include_resource_totals=false; the per-resource-totals block
        # above already lists everything with counts.
        if siblings and sibling_totals is None:
            lines.append("")
            lines.append(
                "Other queryable resources in this dataset "
                "(pass resource_name=... to pick one, or "
                "include_resource_totals=true for a full breakdown):"
            )
            for r in siblings:
                r_name = r.get("name") or "(unnamed)"
                r_fmt = r.get("format") or "?"
                r_id = r.get("id") or "?"
                lines.append(f"  - {r_name} [{r_fmt}]")
                lines.append(f"    resource_id: {r_id}")

        if len(alternates) > 1:
            lines.append("")
            lines.append("Other matching datasets (pass dataset_index=N to switch):")
            chosen_dataset_id = dataset.get("id")
            for i, alt in enumerate(alternates):
                if alt.get("id") == chosen_dataset_id:
                    continue  # skip the dataset we already returned rows for
                alt_title = alt.get("title", "Untitled")
                alt_id = alt.get("id", "unknown")
                alt_queryable = self._first_queryable_resource(alt)
                lines.append(f"  [dataset_index={i}] {alt_title}")
                lines.append(f"    dataset_id: {alt_id}")
                if alt_queryable:
                    lines.append(
                        f"    resource_id (queryable): {alt_queryable.get('id')}"
                    )
                else:
                    lines.append("    (no datastore-loaded resource -- download-only)")

        return "\n".join(lines)

    def _format_sql_results(
        self,
        records: List[Dict[str, Any]],
        fields: List[Dict[str, Any]],
        effective_limit: Optional[int] = None,
        is_passthrough: bool = False,
    ) -> str:
        """Format SQL query results for user display.

        Args:
            records: List of record dictionaries
            fields: List of field metadata dictionaries
            effective_limit: The LIMIT clause that was actually executed --
                either user-supplied or the enforced default. Used to
                detect truncation: if len(records) >= effective_limit, the
                result was almost certainly capped.

        Returns:
            Formatted string representation of results
        """
        n_returned = len(records)

        # Heuristic truncation detection -- datastore_search_sql doesn't
        # return a "total"; the only signal is "did we hit our LIMIT?"
        truncation_block = ""
        if effective_limit is not None and n_returned >= effective_limit:
            truncation_block = (
                "=== MAY BE TRUNCATED ===\n"
                f"This SQL returned exactly the LIMIT ({effective_limit}) "
                "rows. The true total could not be determined from "
                "datastore_search_sql alone. For counting questions, do "
                f"NOT report {n_returned} as the answer -- instead run a "
                "separate SELECT COUNT(*) with the same WHERE clause, or "
                "use ckan__aggregate_data with metrics="
                '{"count": "count(*)"}.\n'
                "========================"
            )

        # SQL path: when len < limit, every returned row is the full
        # result, so we know the true total and can fire the sample-size
        # caveat. When len == limit, total is unknown and the truncation
        # block above already warns the model not to infer counts.
        sample_caveat = ""
        if effective_limit is None or n_returned < effective_limit:
            sample_caveat = self._format_sample_size_caveat(n_returned, unit="row")
        stringly_caveat = self._format_stringly_typed_caveat(records, fields)
        null_caveat = self._format_null_like_frequency_caveat(records, fields)

        # Civic-AI #13: execute_sql is a direct passthrough -- name it so
        # the model knows the SQL it ran was something *it wrote*, not a
        # canonical query. Suppressed for aggregate_data, where the SQL
        # is built from validated structured input.
        sql_passthrough = (
            (
                "[SQL PASSTHROUGH]\n"
                "This response came from a direct SQL query. Confirm the SQL "
                "matched the user's actual question -- the model wrote it.\n"
                "[/SQL PASSTHROUGH]"
            )
            if is_passthrough
            else ""
        )

        caveats = [
            c
            for c in (
                sql_passthrough,
                truncation_block,
                sample_caveat,
                stringly_caveat,
                null_caveat,
            )
            if c
        ]

        parts: List[str] = []
        if caveats:
            parts.append("## Caveats")
            parts.extend(caveats)
            parts.append("")

        parts.append("## Records")
        if not records:
            parts.append(
                "No records matched the SQL query.\n"
                "NOTE: zero rows does NOT mean zero data. Check the WHERE "
                "clause column names against get_schema, verify date "
                "ranges, and confirm filter values match the dataset's "
                "vocabulary (case-sensitive)."
            )
            return "\n".join(parts)

        if effective_limit is not None:
            parts.append(
                f"{n_returned} rows returned (limit={effective_limit}, "
                "true total unknown -- see warning above if any)."
            )
        else:
            parts.append(f"{n_returned} rows returned.")
        parts.append("")

        if fields:
            field_names = [field.get("id", "unknown") for field in fields]
            parts.append(f"Fields: {', '.join(field_names)}")
            parts.append("")

        field_types = self._field_types_by_id(fields)
        for i, record in enumerate(records[:10], 1):
            parts.append(f"Record {i}:")
            parts.extend(self._format_record_lines(record, field_types))
            parts.append("")

        if n_returned > 10:
            parts.append(f"... and {n_returned - 10} more record(s) returned")

        return "\n".join(parts)
