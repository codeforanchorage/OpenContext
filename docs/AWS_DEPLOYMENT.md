# AWS Deployment (Boston fork)

This document describes how the Boston-specific deployment of OpenContext is hosted on AWS, what changed relative to the upstream defaults, and how to operate the stack. It complements [DEPLOYMENT.md](DEPLOYMENT.md), which covers the upstream single-Lambda/API-Gateway architecture.

- **Public endpoint (prod):** `https://boston-data.codeforanchorage.org`
- **Upstream data source:** Boston CKAN portal at `https://data.boston.gov/`
- **Runtime:** AWS Lambda (Python 3.11) behind API Gateway, us-west-2

> **Design constraint:** this fork's top operational priority is **not overwhelming `data.boston.gov`**. It is a shared civic resource, not our infrastructure. Every defensive control below — reserved Lambda concurrency, API Gateway rate limits and daily quota, enforced `LIMIT` on SQL, clamped aggregation limits, body-size caps — exists to keep this MCP server from becoming the noisiest client on that portal. See [SECURITY.md §1](SECURITY.md#1-protecting-the-upstream-data-portal) for the full rationale.

---

## 1. What changed in this fork

The upstream deployment assumes a single-region (us-east-1) Lambda with a standard rate-limited API Gateway in front of it. This fork makes the following operational changes:

### 1.1 Region moved to us-west-2

Terraform variables and the deploy script default to `us-west-2`:

- `terraform/aws/prod.tfvars`, `terraform/aws/staging.tfvars`: `aws_region = "us-west-2"`
- `config.yaml`: `aws.region: "us-west-2"`

The move is for co-location with other Code for Anchorage infrastructure and has no functional effect on the Lambda. Cost numbers in [DEPLOYMENT.md](DEPLOYMENT.md#cost-us-east-1) still apply; us-west-2 pricing is effectively identical for Lambda and API Gateway.

### 1.2 Terraform backend extracted and renamed

The upstream `main.tf` hard-coded an `opencontext-terraform-state` bucket in us-east-1. This fork moves the backend into its own file so the bootstrap account+region+bucket are explicit, and renames the bucket to the convention used by `scripts/setup-backend.sh`:

`terraform/aws/backend.tf` (new file):

```hcl
terraform {
  backend "s3" {
    bucket         = "boston-opencontext-tfstate-<AWS_ACCOUNT_ID>-us-west-2"
    key            = "terraform.tfstate"
    region         = "us-west-2"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}
```

The actual `backend.tf` in this repo hardcodes the Code for Anchorage AWS account ID — Terraform cannot interpolate variables into a backend block, so the literal value has to live in the file. A DynamoDB table (`terraform-state-lock`) is used for state locking — forked deployments should run `scripts/setup-backend.sh` to create both the bucket and the lock table, update the account ID in `backend.tf`, then `terraform init` against `terraform/aws/`.

### 1.3 Reserved Lambda concurrency

A new `lambda_reserved_concurrency` variable caps the number of concurrent Lambda invocations. Default is **10**, set in both staging and prod `.tfvars`.

```hcl
# terraform/aws/variables.tf
variable "lambda_reserved_concurrency" {
  default = 10
}
```

This serves two purposes. The first is cost containment: a surprise traffic spike can't run the bill away. The second, more important one, is **protecting the upstream open-data portal**. Boston's CKAN portal is a shared civic resource; if a misbehaving client fans out into thousands of parallel SQL queries, reserved concurrency bounds how much of that load we can relay. See [SECURITY.md](SECURITY.md#3-upstream-portal-protection) for the full threat model.

Set to `-1` to disable the cap (fall back to the account-wide concurrency limit). Don't do this in prod without a reason.

### 1.4 API Gateway quota raised, rate limits unchanged

```
api_quota_limit = 3000   # was 1000 upstream
api_rate_limit  = 5      # unchanged — sustained req/s
api_burst_limit = 10     # unchanged — burst req/s
```

The daily quota was raised to 3000 after staging traffic showed legitimate per-connector usage (tool discovery + a handful of queries per conversation) could brush against 1000/day for a single user. The per-second rate is kept low deliberately — see [SECURITY.md §2](SECURITY.md#2-rate-limiting-and-body-size).

### 1.5 Custom domain

Prod now fronts the API Gateway with an ACM cert and the custom domain `boston-data.codeforanchorage.org`. Staging has no custom domain (`custom_domain = ""`) — use the raw API Gateway URL from `terraform output`.

### 1.6 Cross-platform, 3.11-pinned packaging

Both `scripts/deploy.sh` and `.github/workflows/release.yml` were updated so the Lambda ZIP matches the runtime regardless of the build host.

- Detects `python3` or falls back to `python` (Windows build hosts).
- Forces cp311 manylinux wheels on every dependency install:
  ```bash
  pip install -r requirements.txt -t ./package \
      --platform manylinux2014_x86_64 \
      --python-version 3.11 \
      --implementation cp \
      --abi cp311 \
      --only-binary :all: \
      --no-compile
  ```
  Without the pin, a build host running Python 3.14 will pull cp314 wheels that fail to import at Lambda cold start with a 502 `InternalServerErrorException`.
- Builds the ZIP with Python's stdlib `zipfile` module instead of the `zip` binary, which isn't present on every runner (notably the staging CI image and Windows).

### 1.7 `local_server.py` serves both `/` and `/mcp`

The Claude Desktop stdio bridge posts to `/mcp`; some earlier testing tools post to `/`. The local dev server now accepts both so you can point Claude Desktop and MCP Inspector at the same endpoint without editing routes.

### 1.8 Concrete Boston CKAN `config.yaml`

Upstream `config.yaml` is a symlink to the DC ArcGIS example. This fork replaces it with a concrete CKAN config targeting `data.boston.gov`. ArcGIS is kept `enabled: false` in the file for reference (Boston's ArcGIS hub at `data-boston.hub.arcgis.com` returns 401 without auth; CKAN is the public entry point).

```yaml
plugins:
  ckan:
    enabled: true
    base_url: "https://data.boston.gov/"
    portal_url: "https://data.boston.gov/"
    city_name: "Boston"
    timeout: 120
  arcgis:
    enabled: false
```

---

## 2. Operator reference

### 2.1 First-time bootstrap

```bash
# 1. Create the state bucket + lock table (once per account/region)
export AWS_REGION=us-west-2
./scripts/setup-backend.sh

# 2. Initialize Terraform against the S3 backend
cd terraform/aws
terraform init
```

### 2.2 Deploying changes

The deploy script validates `config.yaml`, builds a cp311/manylinux Lambda ZIP, and runs `terraform apply`:

```bash
# Staging
./scripts/deploy.sh --environment staging

# Prod
./scripts/deploy.sh --environment prod
```

Under the hood:

1. Counts enabled plugins (must be exactly one — enforced by `core/validators.py`).
2. Builds `lambda-deployment.zip` with dependencies forced to cp311 manylinux wheels.
3. `terraform apply -var-file=<env>.tfvars` against `terraform/aws/`.

### 2.3 Environment configuration

| Variable                        | Staging                      | Prod                                       |
| ------------------------------- | ---------------------------- | ------------------------------------------ |
| `lambda_name`                   | `boston-ckan-mcp-staging`    | `boston-opencontext-mcp-prod`              |
| `aws_region`                    | `us-west-2`                  | `us-west-2`                                |
| `lambda_memory`                 | 512 MB                       | 512 MB                                     |
| `lambda_timeout`                | 120 s                        | 120 s                                      |
| `lambda_reserved_concurrency`   | 10                           | 10                                         |
| `api_quota_limit`               | 3000 / day                   | 3000 / day                                 |
| `api_rate_limit` / `burst`      | 5 / 10 req/s                 | 5 / 10 req/s                               |
| `custom_domain`                 | *(none)*                     | `boston-data.codeforanchorage.org`         |

### 2.4 Getting the endpoint URL

```bash
cd terraform/aws
terraform output -raw api_gateway_url   # Custom domain on prod, exec-api URL on staging
```

### 2.5 Monitoring

CloudWatch log group `/aws/lambda/<lambda_name>`, 14-day retention. Logs are JSON-structured (`logging.format: json` in `config.yaml`) and include a `request_id` field you can join against API Gateway access logs.

```bash
aws logs tail /aws/lambda/boston-opencontext-mcp-prod --follow --region us-west-2
```

### 2.6 Cost

Expected steady-state cost at current quota is well under \$5/month: at 3000 requests/day × 30 days × 512 MB × ~1 s, Lambda runs roughly \$1–2/month. API Gateway REST API adds ~\$3.50 per million requests; at 100k/month that is ~\$0.35. Route 53 hosted zone + ACM cert are the fixed floor (~\$0.50/month).

---

## 3. Known limitations

- **Single-region, single-AZ.** No failover. Fine for a civic-data read proxy; not for critical services.
- **Reserved concurrency is a fuse, not a queue.** Beyond 10 in-flight requests, API Gateway returns 429. Clients must retry with backoff.
- **ArcGIS plugin is disabled.** Enabling it requires an authenticated portal; Boston's hub returns 401 without auth.
