#!/usr/bin/env bash
# Script: Deploy the acme-performance pipeline to the workshop AWS account
# Usage:  ./data/acme-performance/deploy_cloud.sh plan     # read-only: show what would be created
#         ./data/acme-performance/deploy_cloud.sh apply    # create/update resources and run the job
#         ./data/acme-performance/deploy_cloud.sh status   # show the job's pod and recent logs
#
# apply does, in order:
#   1. creates the data lake bucket (Terraform, targeted)
#   2. uploads the sample data: CSVs + schema.sql -> seed/postgres/, events -> landing/
#   3. runs the official bin/deploy-backend.sh with EKS enabled (EKS, JupyterHub, data job and,
#      by default, an in-cluster PostgreSQL source; set TF_VAR_data_source_in_cluster=false for Aurora)
# The job's "seed" stage loads the CSVs into the empty source database, then Bronze -> Silver -> Gold.
set -euo pipefail

MODE="${1:-plan}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
DATASET="${ACME_DATASET_DIR:-$ROOT/sampleData/data-engineering-performance-talent-management}"
JOB_NAME="coding-workshop-acme-performance"
export TF_VAR_aws_eks_enabled=true

# Fresh temporary AWS credentials + participant settings (official workshop script).
"$ROOT/bin/setup-participant.sh" >/dev/null
# shellcheck disable=SC1091
source "$ROOT/ENVIRONMENT.config"
# Always the workshop's cloud region: a shell used for local runs may still have the LocalStack
# region (us-east-1) exported, and bin/deploy-backend.sh keeps whatever AWS_REGION is set.
export AWS_REGION="${ACME_CLOUD_REGION:-ap-south-1}" AWS_DEFAULT_REGION="${ACME_CLOUD_REGION:-ap-south-1}"
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3   # real AWS, never LocalStack

cd "$ROOT/infra"
terraform init -reconfigure -input=false \
    -backend-config="bucket=coding-workshop-tfstate-${PARTICIPANT_ID}" \
    -backend-config="region=${AWS_REGION}" >/dev/null

case "$MODE" in
  plan)
    terraform plan -input=false -no-color -out=/tmp/acme.tfplan > /tmp/acme-plan.txt
    echo "== Resources Terraform would change =="
    grep -E "^  # .* will be (created|updated|destroyed|replaced)|^Plan:" /tmp/acme-plan.txt || true
    echo "Full plan: /tmp/acme-plan.txt (nothing has been created)"
    ;;
  apply)
    BUCKET="${TF_VAR_aws_project:-coding-workshop}-datalake-${PARTICIPANT_ID}"
    echo "INFO: [1/3] Creating data lake bucket $BUCKET ..."
    terraform apply -input=false -auto-approve \
        -target=aws_s3_bucket.datalake \
        -target=aws_s3_bucket_public_access_block.datalake \
        -target=aws_s3_bucket_server_side_encryption_configuration.datalake

    echo "INFO: [2/3] Uploading sample data ..."
    aws s3 cp "$DATASET/schema.sql" "s3://$BUCKET/seed/postgres/schema.sql" --only-show-errors
    for table in teams competencies employees projects performance_reviews competency_assessments talent_review; do
        aws s3 cp "$DATASET/$table.csv" "s3://$BUCKET/seed/postgres/$table.csv" --only-show-errors
    done
    aws s3 sync "$DATASET/learning_activity" "s3://$BUCKET/landing/learning_activity/" --only-show-errors
    echo "INFO: landing files in S3: $(aws s3 ls "s3://$BUCKET/landing/" --recursive | wc -l)"

    echo "INFO: [3/3] Deploying infrastructure and the data job (EKS takes ~15-20 minutes) ..."
    "$ROOT/bin/deploy-backend.sh" aws
    echo "INFO: Done. Watch the job with: $0 status"
    ;;
  status)
    aws eks update-kubeconfig --region "$AWS_REGION" --name "coding-workshop-${PARTICIPANT_ID}" >/dev/null
    kubectl get job "$JOB_NAME" -n default || true
    kubectl get pods -n default -l "app.kubernetes.io/component=$JOB_NAME-db" || true
    kubectl get pods -n default -l "job-name=$JOB_NAME" || true
    kubectl logs -n default "job/$JOB_NAME" --all-containers --tail=40 || true
    BUCKET="${TF_VAR_aws_project:-coding-workshop}-datalake-${PARTICIPANT_ID}"
    for layer in bronze silver gold; do
        echo "$layer files in S3: $(aws s3 ls "s3://$BUCKET/$layer/" --recursive | wc -l)"
    done
    ;;
  *)
    echo "Usage: $0 plan|apply|status" >&2
    exit 1
    ;;
esac
