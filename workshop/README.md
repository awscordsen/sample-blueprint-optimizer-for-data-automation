# Amazon Bedrock Data Automation - Blueprint Instruction Optimization Workshop

This workshop walks through the BDA Blueprint Instruction Optimization API end-to-end using a single Jupyter notebook.

## APIs Covered

| API | Purpose |
|---|---|
| `CreateBlueprint` | Create a blueprint with custom field definitions |
| `InvokeBlueprintOptimizationAsync` | Start an async optimization job using ground truth samples |
| `GetBlueprintOptimizationStatus` | Poll the optimization job status |
| `GetBlueprint` | Retrieve the optimized blueprint schema |
| `CopyBlueprintStage` | Promote DEVELOPMENT blueprint to LIVE |
| `DeleteBlueprint` | Clean up |

## Prerequisites

- AWS account with Amazon Bedrock Data Automation access
- Configured AWS credentials
- Python 3.8+
- Jupyter Notebook environment (SageMaker Studio, local Jupyter, etc.)

## Setup

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Open `blueprint-optimization-workshop.ipynb` and run cells in order

The notebook downloads a sample bank statement PDF from the [AWS BDA samples repo](https://github.com/aws-samples/sample-document-processing-with-amazon-bedrock-data-automation) at runtime. No sample data files are needed in the repo.

## IAM Permissions Required

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "bedrock:CreateBlueprint",
                "bedrock:GetBlueprint",
                "bedrock:DeleteBlueprint",
                "bedrock:UpdateBlueprint",
                "bedrock:InvokeBlueprintOptimizationAsync",
                "bedrock:GetBlueprintOptimizationStatus",
                "bedrock:CopyBlueprintStage",
                "s3:PutObject",
                "s3:GetObject",
                "s3:CreateBucket",
                "s3:ListBucket"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": ["bedrock:InvokeDataAutomationAsync"],
            "Resource": "arn:aws:bedrock:*:*:data-automation-profile/*"
        }
    ]
}
```

## Resources

- [Blueprint Optimization docs](https://docs.aws.amazon.com/bedrock/latest/userguide/bda-optimize-blueprint-info.html)
- [BDA API reference](https://docs.aws.amazon.com/bedrock/latest/userguide/bda-using-api.html)
- [BDA CLI guide](https://docs.aws.amazon.com/bedrock/latest/userguide/bda-cli-guide.html)
- [Sample BDA documents repo](https://github.com/aws-samples/sample-document-processing-with-amazon-bedrock-data-automation)

## License

This library is licensed under the MIT-0 License.
