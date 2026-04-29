# ─── Remote backend — Terraform state stored in S3 ────────────────────────────
# Run "terraform init" once locally when changing the backend configuration.
# GitHub Actions reads state directly from S3 — no local tfstate file needed.
terraform {
  backend "s3" {
    bucket = "bws-artifacts-sae1-prd"
    key    = "terraform/step_functions/terraform.tfstate"
    region = "sa-east-1"
  }
}

provider "aws" {
  region = "sa-east-1"
}

# ─── 1. IAM Role for Step Functions ───────────────────────────────────────────
# Allows the AWS Step Functions service to assume this role.
resource "aws_iam_role" "step_functions_role" {
  name = "sfn_pipeline_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

# ─── 2. Inline policy — Lambda invocation + Glue job execution ────────────────
resource "aws_iam_role_policy" "step_functions_policy" {
  name = "sfn_lambda_glue_policy"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = [
          "arn:aws:lambda:*:*:function:BronzeApiCaptureInfoDengue",
          "arn:aws:lambda:*:*:function:BronzeApiCaptureIbgeMunicipios",
          "arn:aws:lambda:*:*:function:BronzeS3CaptureSinanNotif",
          "arn:aws:lambda:*:*:function:BronzeApiCaptureIbgePopulacao"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = [
          "arn:aws:glue:*:*:job/gold_to_silver",
          "arn:aws:glue:*:*:job/bronze_to_silver",
          "arn:aws:glue:*:*:job/silver_to_gold"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule"
        ]
        Resource = "*"
      }
    ]
  })
}

# ─── 3. Step Functions State Machine ──────────────────────────────────────────
# The pipeline definition is loaded from the JSON file in this same directory.
resource "aws_sfn_state_machine" "sfn_pipeline" {
  name     = "Semanal-Bronze-To-Gold-Pipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = file("${path.module}/pipeline_definition.json")
}

# ─── 4. IAM Role for EventBridge ──────────────────────────────────────────────
# Allows EventBridge to trigger the Step Function
resource "aws_iam_role" "eventbridge_sfn_role" {
  name = "eventbridge_sfn_invoke_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn_policy" {
  name = "eventbridge_sfn_invoke_policy"
  role = aws_iam_role.eventbridge_sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["states:StartExecution"]
        Resource = [aws_sfn_state_machine.sfn_pipeline.arn]
      }
    ]
  })
}

# ─── 5. EventBridge Rule (Scheduler) ──────────────────────────────────────────
# Schedule to run once a week, every Sunday at 00:00 UTC
resource "aws_cloudwatch_event_rule" "weekly_pipeline_rule" {
  name                = "semanal-bronze-to-gold-rule"
  description         = "Triggers the Semanal-Bronze-To-Gold-Pipeline Step Function once a week"
  schedule_expression = "cron(0 0 ? * SUN *)" 
}

resource "aws_cloudwatch_event_target" "sfn_target" {
  rule      = aws_cloudwatch_event_rule.weekly_pipeline_rule.name
  target_id = "TriggerStepFunction"
  arn       = aws_sfn_state_machine.sfn_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_sfn_role.arn
}