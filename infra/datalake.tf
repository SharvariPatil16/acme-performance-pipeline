# Data lake bucket for the acme-performance pipeline: landing/, bronze/, silver/, gold/.
# The name matches the participant IAM policy pattern (coding-workshop*<participant-id>*).
resource "aws_s3_bucket" "datalake" {
  bucket        = format("%s-datalake-%s", var.aws_project, local.app_id)
  force_destroy = true # allows bin/cleanup-environment.sh to remove it with its contents

  tags = local.app_tags
}

# The data lake is private: block every form of public access.
resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Encrypt every object at rest with S3-managed keys.
resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Settings injected into each data job running on EKS (written to a git-ignored, sensitive
# values file so the database password never appears in Terraform or Helm command output).
resource "local_sensitive_file" "job_env" {
  for_each = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? local.data_names_python : {}

  filename = format("%s/.job-env-%s.yaml", path.module, each.key)
  content = yamlencode({
    pythonJob = {
      ttlSecondsAfterFinished = 3600 # keep the finished pod (and its logs) for an hour
      env = merge(
        { for key, value in local.env_vars : key => value if try(trimspace(value), "") != "" },
        {
          DATA_LAKE_BUCKET   = aws_s3_bucket.datalake.bucket
          AWS_REGION         = data.aws_region.this.region
          SOURCE_SEED_PREFIX = "seed/postgres/"
        },
      )
    }
  })
}
