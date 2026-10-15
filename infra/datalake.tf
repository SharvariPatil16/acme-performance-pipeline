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

# Source database for the data jobs on EKS. "true" runs PostgreSQL inside the cluster
# (templates/source-db.yaml): the job reaches it without any security-group change.
# "false" uses the Aurora cluster, which needs TCP 5432 from the EKS cluster security group.
variable "data_source_in_cluster" {
  description = "Run the data jobs' source PostgreSQL inside EKS instead of using Aurora"
  type        = bool
  default     = true
}

# Password for the in-cluster source database (kept in Terraform state and the git-ignored values file).
resource "random_password" "source_db" {
  count   = var.data_source_in_cluster ? 1 : 0
  length  = 24
  special = false
}

# Settings injected into each data job running on EKS (written to a git-ignored, sensitive
# values file so the database password never appears in Terraform or Helm command output).
resource "local_sensitive_file" "job_env" {
  for_each = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? local.data_names_python : {}

  filename = format("%s/.job-env-%s.yaml", path.module, each.key)
  content = yamlencode({
    pythonJob = {
      ttlSecondsAfterFinished = 3600 # keep the finished pod (and its logs) for an hour
      hostNetwork             = true # node-role AWS credentials for S3 (see templates/python-job.yaml)
      env = merge(
        { for key, value in local.env_vars : key => value if try(trimspace(value), "") != "" },
        {
          DATA_LAKE_BUCKET   = aws_s3_bucket.datalake.bucket
          AWS_REGION         = data.aws_region.this.region
          SOURCE_SEED_PREFIX = "seed/postgres/"
          # give boto3 more time/attempts to fetch the node role's credentials
          AWS_METADATA_SERVICE_TIMEOUT      = "5"
          AWS_METADATA_SERVICE_NUM_ATTEMPTS = "3"
        },
      )
    }
    sourceDb = {
      enabled  = var.data_source_in_cluster
      password = var.data_source_in_cluster ? random_password.source_db[0].result : ""
    }
  })
}
