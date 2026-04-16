variable "lambda_name" {
  description = "Name of the Lambda function"
  type        = string
  default     = "opencontext-mcp-server"
}

variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "config_file" {
  description = "Path to config.yaml file"
  type        = string
  default = "../../config.yaml"
}

variable "lambda_memory" {
  description = "Lambda memory in MB"
  type        = number
  default     = 512
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 120
}

variable "lambda_reserved_concurrency" {
  description = "Maximum concurrent Lambda invocations. Caps how hard a single abusive client can fan out into the upstream open-data portal. Set to -1 to disable the limit (use AWS account-wide concurrency)."
  type        = number
  default     = 10
}

variable "api_quota_limit" {
  description = "API Gateway daily request quota"
  type        = number
  default     = 1000
}

variable "api_rate_limit" {
  description = "API Gateway requests per second rate limit"
  type        = number
  default     = 5
}

variable "api_burst_limit" {
  description = "API Gateway burst limit"
  type        = number
  default     = 10
}

variable "stage_name" {
  description = "API Gateway stage name (e.g. prod, dev, staging)"
  type        = string
  default     = "staging"
}

variable "custom_domain" {
  description = "Custom domain name for API Gateway (leave empty to skip custom domain setup)"
  type        = string
  default     = ""
}
