# ─── Remote backend ────────────────────────────────────────────────────────────
terraform {
  required_providers {
    aws   = { source = "hashicorp/aws" }
    null  = { source = "hashicorp/null" }
    local = { source = "hashicorp/local" }
  }
  backend "s3" {
    bucket = "bws-artifacts-sae1-prd"
    key    = "terraform/dynamodb/terraform.tfstate"
    region = "sa-east-1"
  }
}

provider "aws" {
  region = "sa-east-1"
}

# ─── Locals — read JSON files and convert to maps keyed by partition key ───────
# Each JSON file = one DynamoDB table.
# Each object inside the JSON array = one item in that table.
# The map key is the partition key value, required by Terraform's for_each.
locals {
  ingestion_items    = jsondecode(file("${path.module}/../../dynamo_params/ingestion_params.json"))
  quality_items      = jsondecode(file("${path.module}/../../dynamo_params/quality_params.json"))
  notification_items = jsondecode(file("${path.module}/../../dynamo_params/notification_params.json"))
  refined_items      = jsondecode(file("${path.module}/../../dynamo_params/refined_params.json"))

  ingestion_map    = { for i in local.ingestion_items    : i.trgt_tbl     => i }
  quality_map      = { for i in local.quality_items      : i.trgt_tbl     => i }
  notification_map = { for i in local.notification_items : i.trgt_tbl     => i }
  refined_map      = { for i in local.refined_items      : i.target_table => i }
}

# ─── DynamoDB Tables — PAY_PER_REQUEST billing (no fixed cost) ─────────────────
resource "aws_dynamodb_table" "ingestion_params" {
  name         = "ingestion_params"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "trgt_tbl"
  attribute {
    name = "trgt_tbl"
    type = "S"
  }
  lifecycle { prevent_destroy = true }
}

resource "aws_dynamodb_table" "quality_params" {
  name         = "quality_params"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "trgt_tbl"
  attribute {
    name = "trgt_tbl"
    type = "S"
  }
  lifecycle { prevent_destroy = true }
}

resource "aws_dynamodb_table" "notification_params" {
  name         = "notification_params"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "trgt_tbl"
  attribute {
    name = "trgt_tbl"
    type = "S"
  }
  lifecycle { prevent_destroy = true }
}

resource "aws_dynamodb_table" "refined_params" {
  name         = "refined_params"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "target_table"
  attribute {
    name = "target_table"
    type = "S"
  }
  lifecycle { prevent_destroy = true }
}

# ─── DynamoDB Items ─────────────────────────────────────────────────────────────
# Strategy: local_file writes the DynamoDB-format JSON to disk,
# then null_resource calls "aws dynamodb put-item --item file://<path>".
#
# Why not aws_dynamodb_table_item?
#   It always uses "attribute_not_exists" internally, failing for existing items.
#   This is a known, unfixable limitation of the AWS Terraform provider.
#
# Why not shell string interpolation?
#   The table_schema values contain Python-style single quotes that break
#   cmd.exe and PowerShell string parsing on Windows.
#
# Solution: Terraform writes a clean JSON file; AWS CLI reads it via file://.
#           No shell quoting issues, cross-platform safe.

# ── ingestion_params ────────────────────────────────────────────────────────────
resource "local_file" "ingestion_item_json" {
  for_each = local.ingestion_map
  filename = "${path.module}/.item_cache/ingestion_${each.key}.json"
  content  = jsonencode({
    for k, v in each.value : k => { S = try(tostring(v), jsonencode(v)) }
  })
}

resource "null_resource" "ingestion_params_items" {
  for_each = local.ingestion_map
  triggers = { content_hash = sha256(jsonencode(each.value)) }

  provisioner "local-exec" {
    command = "aws dynamodb put-item --table-name ingestion_params --region sa-east-1 --item file://${replace(local_file.ingestion_item_json[each.key].filename, "\\", "/")}"
  }

  depends_on = [aws_dynamodb_table.ingestion_params, local_file.ingestion_item_json]
}

# ── quality_params ──────────────────────────────────────────────────────────────
resource "local_file" "quality_item_json" {
  for_each = local.quality_map
  filename = "${path.module}/.item_cache/quality_${each.key}.json"
  content  = jsonencode({
    for k, v in each.value : k => { S = try(tostring(v), jsonencode(v)) }
  })
}

resource "null_resource" "quality_params_items" {
  for_each = local.quality_map
  triggers = { content_hash = sha256(jsonencode(each.value)) }

  provisioner "local-exec" {
    command = "aws dynamodb put-item --table-name quality_params --region sa-east-1 --item file://${replace(local_file.quality_item_json[each.key].filename, "\\", "/")}"
  }

  depends_on = [aws_dynamodb_table.quality_params, local_file.quality_item_json]
}

# ── notification_params ─────────────────────────────────────────────────────────
resource "local_file" "notification_item_json" {
  for_each = local.notification_map
  filename = "${path.module}/.item_cache/notification_${each.key}.json"
  content  = jsonencode({
    for k, v in each.value : k => { S = try(tostring(v), jsonencode(v)) }
  })
}

resource "null_resource" "notification_params_items" {
  for_each = local.notification_map
  triggers = { content_hash = sha256(jsonencode(each.value)) }

  provisioner "local-exec" {
    command = "aws dynamodb put-item --table-name notification_params --region sa-east-1 --item file://${replace(local_file.notification_item_json[each.key].filename, "\\", "/")}"
  }

  depends_on = [aws_dynamodb_table.notification_params, local_file.notification_item_json]
}

# ── refined_params ──────────────────────────────────────────────────────────────
resource "local_file" "refined_item_json" {
  for_each = local.refined_map
  filename = "${path.module}/.item_cache/refined_${each.key}.json"
  content  = jsonencode({
    for k, v in each.value : k => { S = try(tostring(v), jsonencode(v)) }
  })
}

resource "null_resource" "refined_params_items" {
  for_each = local.refined_map
  triggers = { content_hash = sha256(jsonencode(each.value)) }

  provisioner "local-exec" {
    command = "aws dynamodb put-item --table-name refined_params --region sa-east-1 --item file://${replace(local_file.refined_item_json[each.key].filename, "\\", "/")}"
  }

  depends_on = [aws_dynamodb_table.refined_params, local_file.refined_item_json]
}
