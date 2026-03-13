#!/bin/bash
set -e

STACK_NAME="data-automation-sagemaker"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
TEMPLATE="sagemaker-notebook.yaml"

if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" &>/dev/null; then
  ACTION="update-stack"
  WAITER="stack-update-complete"
else
  ACTION="create-stack"
  WAITER="stack-create-complete"
fi

echo "Running $ACTION for stack: $STACK_NAME in $REGION..."

aws cloudformation "$ACTION" \
  --stack-name "$STACK_NAME" \
  --template-body "file://$TEMPLATE" \
  --capabilities CAPABILITY_IAM \
  --region "$REGION"

echo "Waiting for $WAITER..."
aws cloudformation wait "$WAITER" --stack-name "$STACK_NAME" --region "$REGION"

# Get stack outputs
BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
  --output text)

NOTEBOOK_ARN=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`NotebookInstanceName`].OutputValue' \
  --output text)
# Ref returns the ARN; extract the name from the last path segment
NOTEBOOK_NAME=$(basename "$NOTEBOOK_ARN")

# Upload workshop files to S3 so the lifecycle config can download them
echo "Uploading workshop files to s3://$BUCKET/workshop/..."
aws s3 cp src/      "s3://$BUCKET/workshop/src/"      --recursive --quiet
aws s3 cp samples/  "s3://$BUCKET/workshop/samples/"  --recursive --quiet
echo "Upload complete."

# Restart the notebook so the lifecycle config re-runs and picks up the S3 files
echo "Restarting notebook instance: $NOTEBOOK_NAME..."
aws sagemaker stop-notebook-instance  --notebook-instance-name "$NOTEBOOK_NAME" --region "$REGION"
aws sagemaker wait notebook-instance-stopped --notebook-instance-name "$NOTEBOOK_NAME" --region "$REGION"
aws sagemaker start-notebook-instance --notebook-instance-name "$NOTEBOOK_NAME" --region "$REGION"
aws sagemaker wait notebook-instance-in-service --notebook-instance-name "$NOTEBOOK_NAME" --region "$REGION"
echo "Notebook ready."

echo "Done."
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs' \
  --output table
