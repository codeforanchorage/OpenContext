# Security, privacy, and portal protection

This document covers the controls this fork adds to protect three distinct parties:

1. **The upstream open-data portal** (`data.boston.gov`) — the most important, because it is a shared civic resource, not our infrastructure.
2. **This MCP deployment** — AWS account cost and availability.
3. **End users** — privacy of who asks what.

All three are addressed by the same change set. The sections below describe what was added, why, and what to look at if you are forking this for a different portal.

> **Upstream-portal-first ethos.** An MCP server is a traffic amplifier: a single LLM conversation can fan out into many upstream queries. Being a good citizen of someone else's public API is the top design constraint in this fork.

---

## 1. Protecting the upstream data portal

Boston's CKAN portal at `data.boston.gov` is a public, unauthenticated civic resource. It is shared by journalists, researchers, city staff, and anyone else building on the open-data ecosystem. An MCP server in front of it can easily become the noisiest client on the portal — one Claude conversation can translate into dozens of SQL queries, each of which hits CKAN's DataStore.

Four layers of defense keep this fork from becoming that client:

### 1.1 Reserved Lambda concurrency (hard cap)

`terraform/aws/variables.tf` defines `lambda_reserved_concurrency` (default 10). Only 10 Lambda invocations run concurrently; additional requests get throttled by AWS before they reach the portal. Even if a client bypasses the API Gateway rate limit (e.g. via the Lambda Function URL), they cannot drive more than 10 parallel upstream SQL queries through this deployment.

### 1.2 API Gateway rate limit and daily quota

- `api_rate_limit = 5` sustained req/s, `api_burst_limit = 10` — per-client-key.
- `api_quota_limit = 3000` requests/day — per-client-key.

These are conservative on purpose. The MCP surface is small (tool discovery + a handful of queries per conversation), so 5 rps is well above legitimate use.

### 1.3 Enforced `LIMIT` on every `execute_sql` query

CKAN's DataStore will happily execute an unbounded `SELECT *` against a multi-million-row table. This fork rejects that implicitly: `SQLValidator.enforce_row_limit` appends `LIMIT 10000` (the `DEFAULT_ROW_LIMIT`) to any validated SQL that doesn't already declare one at the top level. A user-supplied top-level `LIMIT` is kept as-is; a `LIMIT` buried inside a subquery or CTE does not count.

See `plugins/ckan/sql_validator.py:SQLValidator.enforce_row_limit`.

### 1.4 `aggregate_data` `LIMIT` is clamped

`SafeSQLBuilder.clamp_limit` (in the same file) enforces `MAX_LIMIT = 10000` on the aggregation path. A caller who asks for `limit: 999999999` gets 10000.

### 1.5 When forking for another portal

If you reuse this fork for another city's CKAN/ArcGIS portal, revisit:

- **`lambda_reserved_concurrency`** — a smaller portal might need lower.
- **`DEFAULT_ROW_LIMIT` and `MAX_LIMIT`** (`plugins/ckan/sql_validator.py`) — both set to 10000; lower them if the target portal is slower or more sensitive.
- **Respect `Retry-After` and portal-published rate limits.** This fork does not yet do adaptive backoff; see [§5 Known gaps](#5-known-gaps).

---

## 2. SQL injection and query validation

The `execute_sql` and `aggregate_data` tools both forward user-controlled input into SQL sent to CKAN DataStore. Upstream had a regex-based SQL validator; this fork replaces it with a defense-in-depth validator plus a typed, allowlist-only builder for aggregation.

### 2.1 `SQLValidator` (execute_sql path)

`plugins/ckan/sql_validator.py:SQLValidator.validate_query` enforces:

- **Length cap reduced from 50,000 → 8,192 bytes.** No legitimate MCP-generated query is that long.
- **Comment stripping before scanning.** `/* ... */` and `-- ...` are removed before keyword and function scans run, so obfuscated payloads like `SEL/**/ECT ... UNI/**/ON` or `DROP /* hidden */ TABLE` cannot slip past.
- **Expanded forbidden keyword list.** Upstream blocked the obvious DDL verbs; this fork adds `PREPARE`, `COPY`, `LISTEN`, `NOTIFY`, `VACUUM`, `ANALYZE`, `CLUSTER`, `REINDEX`, `LOAD`, `DO`.
- **Forbidden function list.** `xp_cmdshell`, `pg_sleep`, `pg_read_file`, `pg_read_binary_file`, `pg_ls_dir`, `pg_stat_file`, `lo_import`, `lo_export`, `current_setting`, `set_config`, `dblink`.
- **File-write pattern match.** `INTO OUTFILE` and `INTO DUMPFILE`.
- **AST-validated `FROM`/`JOIN` targets.** Uses `sqlparse` to walk the statement; every table reference must be either a UUID-quoted CKAN resource ID (e.g. `"11111111-2222-3333-4444-555555555555"`) or a CTE alias declared in the same statement. Schema-qualified targets like `pg_catalog.pg_class` are rejected.
- **Single-statement only.** Multiple statements separated by `;` are rejected.
- **SELECT/WITH only.** The statement type must be `SELECT` (including `WITH ... SELECT`).

### 2.2 `SafeSQLBuilder` (aggregate_data path)

The upstream `aggregate_data` implementation built SQL by string concatenation — including for user-supplied `group_by`, `metrics`, `filters`, `having`, and `order_by`. This fork rewrites the path to use `SafeSQLBuilder`, which treats every caller-supplied value as untrusted input:

| Input          | Validation                                                                            |
| -------------- | ------------------------------------------------------------------------------------- |
| `resource_id`  | Must match UUID regex.                                                                |
| Column name    | Must match `[A-Za-z_][A-Za-z0-9_]*` — then double-quoted.                             |
| Metric expr    | Allowlist: `count(*)`, `{count\|sum\|avg\|min\|max\|stddev}([DISTINCT] <ident>)`.     |
| Filter value   | Coerced by type: `None → IS NULL`, bool → `TRUE`/`FALSE`, int/float formatted, string single-quoted with `'` escaped to `''`. |
| `order_by`     | Must match `<identifier> [ASC\|DESC]`.                                                |
| `having` value | Must be numeric.                                                                      |
| `limit`        | Must be a positive `int`; clamped to `MAX_LIMIT = 10000`.                             |

Anything not on the allowlist raises `ValueError` and is surfaced to the caller as an error; nothing is executed against CKAN.

### 2.3 Tests

`tests/test_sql_validator.py` and `tests/test_ckan_plugin.py` cover valid queries, each forbidden keyword and function, comment-based obfuscation, schema-qualified FROM targets, UUID validation, and every `SafeSQLBuilder` method.

---

## 3. Rate limiting and body size (this deployment)

### 3.1 Request body size cap

`server/http_handler.py` rejects any JSON-RPC body larger than **65,536 bytes (64 KB)** with HTTP 413 before the JSON parser runs. The MCP surface is small — every legitimate tool call fits well under a few KB — so a megabyte-sized payload is either a bug or abuse. Tests: `tests/test_http_handler.py:TestBodySizeCap`.

### 3.2 API Gateway

See [§1.2](#12-api-gateway-rate-limit-and-daily-quota).

### 3.3 SQL length cap

Upstream allowed 50 KB SQL strings; this fork drops the cap to 8 KB (`SQLValidator.MAX_SQL_LENGTH`). Combined with the body-size cap, an attacker cannot inflate the work we relay to CKAN via a single huge query.

---

## 4. Privacy

### 4.1 What this server stores

**Nothing user-identifying, by design.** This deployment is stateless:

- No database. No user accounts. No cookies. No session tokens.
- CloudWatch logs capture per-request: `request_id`, HTTP method/path, duration, status, and (truncated) tool name and SQL. Logs retention is 14 days.
- SQL log entries are truncated to 500 characters (`plugins/ckan/plugin.py: logger.info("Executing SQL", extra={"sql": sql[:500]})`).
- API Gateway access logs may record caller IPs per AWS defaults — treat these as the only identifying data we retain.

### 4.2 What the upstream portal sees

From CKAN's perspective, this server is a single client. End-user identity is not forwarded: every upstream request is made by the Lambda using its own outbound IP pool. This is a privacy win (your CKAN query isn't tied to your IP) but means rate-limit abuse by one user affects everyone sharing the deployment — which is exactly why [§1](#1-protecting-the-upstream-data-portal) exists.

### 4.3 What users should know

Connectors built on top of this MCP pass prompts through Claude. This deployment only sees the tool calls that Claude generates — not the user's raw prompt — but those tool calls (especially `execute_sql`) may contain content the user typed. The 14-day log retention and truncation are there to minimize this, but anyone deploying this should treat CloudWatch as "may contain incidental user content" for compliance purposes.

### 4.4 Data is public

All data this server returns comes from `data.boston.gov`, which is public open data. There is no private, PII-bearing, or licensed content behind this API. If you fork for a portal with non-public or licensed data, that changes the threat model substantially — add authentication in front of API Gateway.

---

## 5. Known gaps

- **No adaptive backoff on upstream errors.** If CKAN starts returning 429 or 5xx, this server does not currently slow down — it just relays the error. A future change should honor `Retry-After` and apply exponential backoff.
- **No per-tool rate limiting.** The API Gateway limit is per-client-key across all tools; a caller could spend their entire 5 rps on `execute_sql`. This is fine for now (reserved concurrency is the backstop) but worth revisiting if usage patterns change.
- **Lambda Function URL is public.** The Terraform stack still creates one for debugging; it bypasses the API Gateway quota. Disable it (`create_lambda_url = false` if the variable is added, or remove the resource) before handing out the URL publicly.
- **No authentication.** API Gateway uses usage plans + API keys for rate limiting, but there is no per-user auth. Appropriate for a public open-data proxy; not appropriate for anything else.

---

## 6. Reporting a vulnerability

Please open a private security advisory on the GitHub repo rather than filing a public issue. Include a proof-of-concept request and the expected vs. actual behavior.
