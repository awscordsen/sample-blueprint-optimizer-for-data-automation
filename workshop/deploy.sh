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

echo "Done."
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs' \
  --output table
