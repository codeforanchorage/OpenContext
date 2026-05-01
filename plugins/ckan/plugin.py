"""CKAN plugin implementation for OpenContext.

This plugin provides access to CKAN-based open data portals.
"""

import logging
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
        dataset as 5–7 download-only resources (GeoJSON, KML, SHP, ...) plus
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
        """
        city = self.plugin_config.city_name
        return [
            ToolDefinition(
                name="search_datasets",
                description=(
                    f"Search for datasets in {city}'s open data portal by keyword.\n\n"
                    "Returns a list of CKAN datasets. Each dataset contains a "
                    "`resources` array; each resource has its own `id` (a UUID) "
                    "that identifies a queryable table.\n\n"
                    "Next step:\n"
                    "  - EASIEST: if you just want data rows, call "
                    "`ckan__search_and_query` with the same query — it combines "
                    "search + query in one call.\n"
                    "  - Otherwise: pick a resource from `resources[].id` in the "
                    "response and call `ckan__query_data` with that value as "
                    "`resource_id`.\n"
                    "  - To inspect a dataset's resources first, call "
                    "`ckan__get_dataset` with `dataset_id` set to the dataset's "
                    "`id` or `name`.\n\n"
                    "The formatted response surfaces a `suggested_resource_id` "
                    "and `suggested_next_tool` line at the top — read those to "
                    "pick the next call."
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
                            "description": "Maximum number of datasets to return (default: 20).",
                            "default": 20,
                        },
                    },
                    "required": ["query"],
                },
            ),
            ToolDefinition(
                name="get_dataset",
                description=(
                    f"Get full metadata for one dataset in {city}'s open data "
                    "portal, including its `resources` array.\n\n"
                    "Use this to find the resource UUIDs needed by "
                    "`ckan__query_data`, `ckan__get_schema`, "
                    "`ckan__aggregate_data`, or `ckan__execute_sql`. The "
                    "response lists each resource with its `Resource ID` "
                    "(a UUID).\n\n"
                    "Next step: call `ckan__query_data` with `resource_id` set "
                    "to one of the `Resource ID` values from this response."
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
                    f"Query rows from a specific resource in {city}'s open "
                    "data portal.\n\n"
                    "The `resource_id` parameter is a CKAN resource UUID — "
                    "NOT a dataset ID. Get one by first calling "
                    "`ckan__search_datasets` or `ckan__get_dataset` and "
                    "reading the `id` inside the `resources` array.\n\n"
                    "IMPORTANT: only resources with `datastore_active=true` "
                    "are queryable here. Boston datasets typically attach "
                    "5–7 resources (GeoJSON, KML, SHP, PDF, ArcGIS REST, "
                    "CSV) but only the CSV one is loaded into the "
                    "datastore. If you call this tool with a download-only "
                    "resource UUID it will return 404. The output of "
                    "`ckan__search_datasets` and `ckan__get_dataset` "
                    "labels resources as QUERYABLE or DOWNLOAD-ONLY — pick "
                    "the QUERYABLE one.\n\n"
                    "FILTERING — pick the right knob:\n"
                    "  - `filters` is EQUALITY ONLY (case_status='Closed'). "
                    "Cannot do dates, ranges, BETWEEN, IN, LIKE, or any "
                    "comparison. A timestamp column will NEVER match an "
                    "equality filter on a date string like '2026-04-29'.\n"
                    "  - `where` is structured comparison. Use this for "
                    "date ranges, numeric bounds, IN-lists, LIKE/ILIKE, "
                    "or NULL checks. Example for 'closed on 2026-04-29':\n"
                    "    where = {\"close_date\": {\"gte\": "
                    "\"2026-04-29\", \"lt\": \"2026-04-30\"}, "
                    "\"case_status\": \"Closed\"}\n"
                    "  - For window functions, CTEs, joins, or anything "
                    "the structured `where` can't express, use "
                    "`ckan__execute_sql` instead.\n\n"
                    "Note: `query` arguments on search tools match dataset "
                    "metadata (titles/tags), NOT row content. To filter "
                    "ROWS by date/status/etc., use `where` here or in "
                    "`ckan__search_and_query`.\n\n"
                    "Tip: if you only have a keyword and no resource_id "
                    "yet, use `ckan__search_and_query` instead — it does "
                    "the lookup and the data fetch in a single call and "
                    "auto-picks the datastore-loaded resource."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "resource_id": {
                            "type": "string",
                            "description": (
                                "CKAN resource UUID (36-char, e.g. "
                                "'11111111-2222-3333-4444-555555555555'). "
                                "Provenance: the `id` field inside the "
                                "`resources` array returned by "
                                "`ckan__search_datasets` or "
                                "`ckan__get_dataset`. This is NOT a dataset ID."
                            ),
                        },
                        "filters": {
                            "type": "object",
                            "description": (
                                "EQUALITY-ONLY filters as field:value "
                                "pairs (e.g. {\"status\": \"Open\"}). For "
                                "ranges/dates/IN/LIKE, use `where` "
                                "instead — `filters` cannot express "
                                "anything other than exact equality."
                            ),
                        },
                        "where": {
                            "type": "object",
                            "description": (
                                "Structured WHERE clause supporting "
                                "comparison operators. Each entry is "
                                "either {field: scalar} (equality) or "
                                "{field: {op: value, ...}} where op is "
                                "one of: eq, ne, gt, gte, lt, lte, in, "
                                "not_in, like, ilike, is_null. Example "
                                "for 'closed on 2026-04-29': "
                                "{\"close_date\": {\"gte\": "
                                "\"2026-04-29\", \"lt\": \"2026-04-30\"}, "
                                "\"case_status\": \"Closed\"}. The "
                                "schema footer in any prior query result "
                                "lists available column names and types."
                            ),
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of records (default: 100).",
                            "default": 100,
                        },
                    },
                    "required": ["resource_id"],
                },
            ),
            ToolDefinition(
                name="get_schema",
                description=(
                    f"Get the schema (field names and types) for a resource "
                    f"in {city}'s open data portal.\n\n"
                    "Call this BEFORE `ckan__aggregate_data` or "
                    "`ckan__execute_sql` so you know the exact field names "
                    "to reference in `group_by`, `metrics`, SELECT, or WHERE "
                    "clauses.\n\n"
                    "Next step: pass the field names you discover to "
                    "`ckan__aggregate_data` (in `group_by` / `metrics`) or "
                    "to `ckan__execute_sql`."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "resource_id": {
                            "type": "string",
                            "description": (
                                "CKAN resource UUID. Provenance: the `id` "
                                "inside the `resources` array returned by "
                                "`ckan__search_datasets` or "
                                "`ckan__get_dataset`."
                            ),
                        },
                    },
                    "required": ["resource_id"],
                },
            ),
            ToolDefinition(
                name="execute_sql",
                description=(
                    f"Execute a raw PostgreSQL SELECT query against "
                    f"{city}'s CKAN datastore.\n\n"
                    "⚠️ Use this only when the structured `where` "
                    "argument on `ckan__query_data` / "
                    "`ckan__search_and_query` cannot express your filter "
                    "(e.g. window functions, CTEs, joins, aggregations "
                    "beyond ckan__aggregate_data).\n\n"
                    "Security: Only SELECT allowed. INSERT/UPDATE/DELETE "
                    "blocked.\n\n"
                    "Concrete examples:\n"
                    "- Closed on a specific date:\n"
                    "    SELECT * FROM \"<resource_id>\" WHERE "
                    "close_date >= '2026-04-29' AND close_date < "
                    "'2026-04-30' AND case_status = 'Closed' LIMIT 100\n"
                    "- Counts by day:\n"
                    "    SELECT date_trunc('day', close_date) AS d, "
                    "COUNT(*) FROM \"<resource_id>\" GROUP BY d "
                    "ORDER BY d DESC LIMIT 30\n"
                    "- Window functions: RANK() OVER (...)\n"
                    "- CTEs: WITH subquery AS (...)\n\n"
                    "Resource IDs must be double-quoted: "
                    "FROM \"uuid-here\"\n\n"
                    "Prerequisites:\n"
                    "  - resource UUID for the FROM clause: get from "
                    "`ckan__search_datasets` or `ckan__get_dataset`.\n"
                    "  - field names: get from `ckan__get_schema`, or "
                    "the 'Filterable columns' footer of any prior "
                    "successful `ckan__query_data` / "
                    "`ckan__search_and_query` call."
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
                    f"Aggregate data with GROUP BY from {city}'s open data "
                    "portal.\n\n"
                    "Prerequisites:\n"
                    "  - `resource_id`: get from `ckan__search_datasets` / "
                    "`ckan__get_dataset` (the `id` inside the `resources` "
                    "array).\n"
                    "  - field names for `group_by` / `metrics`: get from "
                    "`ckan__get_schema`.\n\n"
                    "Examples:\n"
                    '- Count by field: group_by=["neighborhood"], '
                    'metrics={"count": "count(*)"}\n'
                    '- Multiple metrics: metrics={"total": "count(*)", '
                    '"avg": "avg(field)"}\n'
                    '- With filters: filters={"status": "Open"}\n\n'
                    "Supports: count(*), sum(), avg(), min(), max(), stddev()."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "resource_id": {
                            "type": "string",
                            "description": (
                                "CKAN resource UUID. Provenance: the `id` "
                                "inside the `resources` array returned by "
                                "`ckan__search_datasets` or "
                                "`ckan__get_dataset`."
                            ),
                        },
                        "group_by": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Field names to group by. Get exact names from `ckan__get_schema`.",
                        },
                        "metrics": {
                            "type": "object",
                            "description": "Map of alias -> aggregate expression, e.g. {\"count\": \"count(*)\"}.",
                        },
                        "filters": {"type": "object"},
                        "having": {"type": "object"},
                        "order_by": {"type": "string"},
                        "limit": {"type": "integer", "default": 100},
                    },
                    "required": ["resource_id", "metrics"],
                },
            ),
            ToolDefinition(
                name="search_and_query",
                description=(
                    f"ONE-CALL keyword-to-data for {city}'s open data "
                    "portal: searches for the best-matching dataset and "
                    "immediately returns rows from its first datastore-"
                    "loaded resource — no tool chaining required.\n\n"
                    "Use this when you have a keyword (e.g. "
                    "'311 service requests', 'parks', 'building permits') "
                    "and want actual data rows. It combines "
                    "`ckan__search_datasets` + `ckan__query_data` into a "
                    "single server-side step, so you do NOT need to "
                    "extract a resource_id from a previous response.\n\n"
                    "Auto-picks the right resource: Boston datasets "
                    "typically attach 5–7 resources (GeoJSON, KML, SHP, "
                    "PDF, ArcGIS REST, CSV) but only the CSV is loaded "
                    "into the queryable datastore. This tool walks the "
                    "search results and skips datasets / resources that "
                    "aren't datastore-active, so you don't get a 404 "
                    "from a download-only resource.\n\n"
                    "WHAT `query` MEANS: `query` matches dataset metadata "
                    "(title, tags, description) — it does NOT filter ROWS. "
                    "If the user asks for '311 requests closed on 4/29', "
                    "the `query` finds the 311 dataset and `where` does "
                    "the row filtering:\n"
                    "  query=\"311\", where={\"close_date\": {\"gte\": "
                    "\"2026-04-29\", \"lt\": \"2026-04-30\"}, "
                    "\"case_status\": \"Closed\"}\n\n"
                    "Returns: data rows from the chosen dataset's chosen "
                    "resource, plus a 'Filterable columns' footer listing "
                    "the schema (so you can refine with `where` or pivot "
                    "to `ckan__execute_sql` for joins/CTEs/window funcs), "
                    "an 'Other queryable resources in this dataset' "
                    "block listing siblings (e.g. per-year archives), "
                    "and a header showing which dataset and resource "
                    "were used.\n\n"
                    "MULTI-RESOURCE DATASETS: a single dataset can hold "
                    "many queryable resources. Boston's 311 dataset has "
                    "22 (a rolling 'NEW SYSTEM' view plus per-year "
                    "archives 2011–2026). Use `resource_name` to pick a "
                    "specific one — e.g. resource_name=\"2020\" picks "
                    "'311 Service Requests - 2020'. If you don't pass "
                    "`resource_name`, the first datastore-loaded "
                    "resource is used (which is typically the rolling "
                    "current view, NOT historical archives — so older "
                    "questions need `resource_name`)."
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
                            "description": "Maximum number of data rows to return (default: 100).",
                            "default": 100,
                        },
                        "filters": {
                            "type": "object",
                            "description": (
                                "EQUALITY-ONLY row filters (e.g. "
                                "{\"case_status\": \"Closed\"}). For "
                                "ranges/dates/IN/LIKE, use `where` "
                                "instead."
                            ),
                        },
                        "where": {
                            "type": "object",
                            "description": (
                                "Structured WHERE clause for ranges, "
                                "dates, IN, LIKE, NULL checks. Each "
                                "entry is {field: scalar} (equality) or "
                                "{field: {op: value, ...}} where op is "
                                "one of: eq, ne, gt, gte, lt, lte, in, "
                                "not_in, like, ilike, is_null. The "
                                "right knob for 'closed on 2026-04-29': "
                                "{\"close_date\": {\"gte\": "
                                "\"2026-04-29\", \"lt\": \"2026-04-30\"}}."
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
                                "datasets typically attach 5–7 resources "
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
                                "resource_name=\"2020\" picks the 2020 "
                                "archive; resource_name=\"NEW SYSTEM\" "
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
                                "to each resource — be aware schemas can "
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
                limit = arguments.get("limit", 20)
                datasets, total = await self._search_datasets_with_count(
                    query, limit
                )
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._format_search_results(
                                datasets, total=total, limit=limit
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
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._format_dataset(dataset),
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
                limit = arguments.get("limit", 100)
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
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._format_query_results(
                                records, fields, limit, total
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
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._format_schema(schema),
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
                # Format SQL results
                records = result.get("records", [])
                fields = result.get("fields", [])
                effective_limit = result.get("effective_limit")
                formatted_text = self._format_sql_results(
                    records, fields, effective_limit=effective_limit
                )
                return ToolResult(
                    content=[{"type": "text", "text": formatted_text}],
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
                limit = arguments.get("limit", 100)
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
                return ToolResult(
                    content=[
                        {
                            "type": "text",
                            "text": self._format_search_and_query(composite, limit),
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
                result = await self.aggregate_data(
                    resource_id=resource_id,
                    group_by=arguments.get("group_by", []),
                    metrics=metrics,
                    filters=arguments.get("filters"),
                    having=arguments.get("having"),
                    order_by=arguments.get("order_by"),
                    limit=arguments.get("limit", 100),
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
                return ToolResult(
                    content=[{"type": "text", "text": formatted}], success=True
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
        """Same as search_datasets but also returns CKAN's `count` — the
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
                return [], [], None, result.get(
                    "message", "SQL execution failed"
                )
            records = result.get("records", [])
            fields = result.get("fields", [])

            # If we hit the LIMIT exactly, we don't actually know the total —
            # do a cheap COUNT(*) follow-up so the model gets a real number
            # instead of mistaking the limit for the count.
            total: Optional[int]
            if len(records) >= limit_int:
                total = await self._count_via_sql(
                    validated_id, where_sql, filters
                )
            else:
                total = len(records)

            return records, fields, total, None

        # No `where` → cheap datastore_search path.
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
        # CKAN returns `total` for free here — a true count of rows
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
                    if isinstance(value, bool) or not isinstance(
                        value, (int, float)
                    ):
                        raise ValueError(
                            f"HAVING value must be numeric: {value!r}"
                        )
                    having_parts.append(f"{expr_quoted} > {value}")
                having_clause = " HAVING " + " AND ".join(having_parts)

            order_clause = ""
            if order_by:
                order_clause = " ORDER BY " + SafeSQLBuilder.validate_order_by(
                    order_by
                )

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
        return [r for r in (dataset.get("resources") or []) if r.get("datastore_active")]

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
        auto_picked_resource = not (
            explicit_resource or explicit_resource_name
        )
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
                    f"  [{idx}] {ds.get('title') or ds.get('id')}: "
                    "no resources"
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

            formats = sorted(
                {
                    (r.get("format") or "?").upper()
                    for r in resources
                }
            )
            skipped_summary.append(
                f"  [{idx}] {ds.get('title') or ds.get('id')}: "
                f"no datastore-loaded resource (formats: {', '.join(formats)})"
            )

        # If we walked all datasets and resource_name was set but never
        # matched, give a name-specific error rather than the generic
        # "no queryable resource" one.
        if (
            chosen_dataset is None
            and resource_name
            and not explicit_dataset
        ):
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
        dataset. Returns a {resource_id: int_total or None} dict — a None
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
            n = await self._count_via_sql(
                validated_id, where_sql, filters
            )
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

    def _format_search_results(
        self,
        datasets: List[Dict[str, Any]],
        total: Optional[int] = None,
        limit: int = 20,
    ) -> str:
        """Format search results for user display."""
        if not datasets:
            return f"No datasets found in {self.plugin_config.city_name}'s open data portal."

        suggested_resource_id: Optional[str] = None
        suggested_dataset_id: Optional[str] = None
        for ds in datasets:
            queryable = self._first_queryable_resource(ds)
            if queryable and queryable.get("id"):
                suggested_resource_id = queryable.get("id")
                suggested_dataset_id = ds.get("id")
                break

        lines: List[str] = []
        if suggested_resource_id:
            lines.extend(
                [
                    "=== NEXT STEP (read this first) ===",
                    f"suggested_resource_id: {suggested_resource_id}",
                    "suggested_next_tool: ckan__query_data",
                    f"suggested_call: ckan__query_data(resource_id=\"{suggested_resource_id}\")",
                    "(this is the datastore-loaded resource — only such "
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
                    f"   resource_id: NONE QUERYABLE — this dataset has "
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
                    f"suggested_call: ckan__query_data(resource_id=\"{suggested_resource_id}\")",
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
                    "file downloads — see URLs below.",
                    "=================",
                    "",
                ]
            )

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
                        f"     Use ckan__query_data with resource_id=\"{res_id}\" to fetch rows."
                    )
                else:
                    if res_url:
                        lines.append(f"     download_url: {res_url}")
                    lines.append(
                        "     (not loaded into datastore — ckan__query_data "
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
        """Format query results for user display."""
        n_returned = len(records)
        truncated_warning = self._format_truncation_block(
            n_returned, limit, total
        )

        if not records:
            text = "No records found matching the query."
            parts = [truncated_warning, text] if truncated_warning else [text]
            schema_footer = self._format_schema_footer(fields)
            if schema_footer:
                parts.append("")
                parts.append(schema_footer)
            return "\n".join(parts)

        lines: List[str] = []
        if truncated_warning:
            lines.append(truncated_warning)
            lines.append("")

        # Header line: lead with the X-of-Y framing so the model can't
        # mistake the rows-returned count for the answer to a counting
        # question.
        lines.append(self._format_count_header(n_returned, limit, total))
        lines.append("")

        # Show first few records as examples
        for i, record in enumerate(records[:5], 1):
            lines.append(f"Record {i}:")
            for key, value in record.items():
                if key != "_id":  # Skip internal ID
                    lines.append(f"  {key}: {value}")
            lines.append("")

        if n_returned > 5:
            lines.append(f"... and {n_returned - 5} more record(s) returned")

        schema_footer = self._format_schema_footer(fields)
        if schema_footer:
            lines.append("")
            lines.append(schema_footer)

        return "\n".join(lines)

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
            "true total unknown — see TRUNCATED warning above if any)."
        )

    def _format_truncation_block(
        self,
        n_returned: int,
        limit: int,
        total: Optional[int],
    ) -> str:
        """Emit a clear warning when the result set is — or might be —
        truncated by LIMIT.

        Returns ``""`` when no warning is needed (result fits within limit
        and total is known/equal to returned)."""
        # Total known and matches returned → fits within limit, no warning.
        if total is not None and total <= n_returned:
            return ""

        # Total known but exceeds returned → exact truncation, exact total.
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
                '{"count": "count(*)"} and a matching filter — '
                "it's cheaper than fetching rows.\n"
                "================="
            )

        # Total unknown and we hit the limit exactly → likely truncated.
        if total is None and n_returned >= limit:
            return (
                "=== MAY BE TRUNCATED ===\n"
                f"Result returned exactly the requested limit "
                f"({limit}) and the true total could not be determined. "
                "Treat this as a possibly-incomplete sample. For "
                "counting questions, do NOT report "
                f"{n_returned} as the answer — use ckan__aggregate_data "
                'with metrics={"count": "count(*)"} and the same '
                "filter, or re-run with a higher limit.\n"
                "========================"
            )

        return ""

    def _format_schema_footer(
        self, fields: Optional[List[Dict[str, Any]]]
    ) -> str:
        """Render a per-call 'Filterable columns' block listing every field
        the model can pass to ``where``, ``filters``, or reference in
        ``execute_sql``.

        We surface this on every successful row-returning call so the next
        pivot (e.g. 'now filter by close_date') is a one-shot."""
        if not fields:
            return ""
        usable = [
            f for f in fields if f.get("id") and f.get("id") != "_id"
        ]
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

            lines.append(f"  • {field_id} ({field_type})")
            if description:
                lines.append(f"    {description}")

        return "\n".join(lines)

    def _format_search_and_query(
        self, composite: Dict[str, Any], limit: int
    ) -> str:
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
        # the model was forced into search_datasets→get_dataset→iterate
        # query_data, which surfaced all resources naturally.)
        sibling_queryables = self._queryable_resources(dataset)
        siblings = [
            r for r in sibling_queryables if r.get("id") != resource_id
        ]
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
                "(e.g. resource_name=\"2018\").\n"
                "  - Use ckan__execute_sql with UNION ALL across the "
                "resource UUIDs listed below.\n"
                "If the user's question was about RECENT data, this "
                "rolling/current resource is likely fine.\n"
                "==============================="
            )
            lines.append(partial_block)
            lines.append("")

        truncated_warning = self._format_truncation_block(
            n_returned, limit, total
        )
        if truncated_warning:
            lines.append(truncated_warning)
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
        # cross-resource breakdown — this is the answer to "total"
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
                + (" (some resources returned null — sum is partial)"
                   if had_failure else "")
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
                    f"Showing first 5 of {n_returned} returned "
                    f"(true total: {total}):"
                )
            else:
                preview_caption = f"Showing first 5 of {n_returned} returned:"
            lines.append(preview_caption)
            for i, record in enumerate(records[:5], 1):
                lines.append(f"Record {i}:")
                for key, value in record.items():
                    if key != "_id":
                        lines.append(f"  {key}: {value}")
                lines.append("")
            if n_returned > 5:
                lines.append(f"... and {n_returned - 5} more record(s) returned")

        schema_footer = self._format_schema_footer(fields)
        if schema_footer:
            lines.append("")
            lines.append(schema_footer)

        # Sibling queryable resources within the SAME dataset. Boston's 311
        # dataset has 22 (a rolling view + per-year archives back to 2011)
        # — without this block the model can't see them. Only render here
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
            lines.append(
                "Other matching datasets (pass dataset_index=N to switch):"
            )
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
                        f"    resource_id (queryable): "
                        f"{alt_queryable.get('id')}"
                    )
                else:
                    lines.append(
                        "    (no datastore-loaded resource — "
                        "download-only)"
                    )

        return "\n".join(lines)

    def _format_sql_results(
        self,
        records: List[Dict[str, Any]],
        fields: List[Dict[str, Any]],
        effective_limit: Optional[int] = None,
    ) -> str:
        """Format SQL query results for user display.

        Args:
            records: List of record dictionaries
            fields: List of field metadata dictionaries
            effective_limit: The LIMIT clause that was actually executed —
                either user-supplied or the enforced default. Used to
                detect truncation: if len(records) >= effective_limit, the
                result was almost certainly capped.

        Returns:
            Formatted string representation of results
        """
        n_returned = len(records)

        # Heuristic truncation detection — datastore_search_sql doesn't
        # return a "total"; the only signal is "did we hit our LIMIT?"
        truncation_block = ""
        if effective_limit is not None and n_returned >= effective_limit:
            truncation_block = (
                "=== MAY BE TRUNCATED ===\n"
                f"This SQL returned exactly the LIMIT ({effective_limit}) "
                "rows. The true total could not be determined from "
                "datastore_search_sql alone. For counting questions, do "
                f"NOT report {n_returned} as the answer — instead run a "
                "separate SELECT COUNT(*) with the same WHERE clause, or "
                "use ckan__aggregate_data with metrics="
                '{"count": "count(*)"}.\n'
                "========================"
            )

        if not records:
            text = "No records found matching the SQL query."
            return f"{truncation_block}\n\n{text}" if truncation_block else text

        lines: List[str] = []
        if truncation_block:
            lines.append(truncation_block)
            lines.append("")

        # Header — total is unknown for raw SQL, so show "X rows returned".
        if effective_limit is not None:
            lines.append(
                f"{n_returned} rows returned (limit={effective_limit}, "
                "true total unknown — see warning above if any).\n"
            )
        else:
            lines.append(f"{n_returned} rows returned.\n")

        # Show field names if available
        if fields:
            field_names = [field.get("id", "unknown") for field in fields]
            lines.append(f"Fields: {', '.join(field_names)}\n")

        # Show first few records as examples
        for i, record in enumerate(records[:10], 1):
            lines.append(f"Record {i}:")
            for key, value in record.items():
                if key != "_id":  # Skip internal ID
                    lines.append(f"  {key}: {value}")
            lines.append("")

        if n_returned > 10:
            lines.append(f"... and {n_returned - 10} more record(s) returned")

        return "\n".join(lines)
