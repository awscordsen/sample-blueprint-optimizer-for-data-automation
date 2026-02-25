# Amazon Bedrock Data Automation - Blueprint Optimization Workshop

This workshop demonstrates Amazon Bedrock Data Automation blueprint optimization using purchase order documents. The notebook walks through the complete optimization workflow from creating a blueprint to testing optimized field extraction.

## What You'll Learn

This workshop covers the Bedrock Data Automation Blueprint Optimization API:

| API | Purpose |
|---|---|
| `CreateBlueprint` | Create a blueprint with custom field definitions |
| `InvokeBlueprintOptimizationAsync` | Start an async optimization job using ground truth samples |
| `GetBlueprintOptimizationStatus` | Poll the optimization job status |
| `GetBlueprint` | Retrieve the optimized blueprint schema |
| `CopyBlueprintStage` | Promote DEVELOPMENT blueprint to LIVE |
| `DeleteBlueprint` | Clean up resources |

## Prerequisites

### 1. AWS Account
- Create an AWS account at https://aws.amazon.com if you don't have one
- Ensure you have access to Amazon Bedrock Data Automation in your region

### 2. AWS CLI Setup
Install and configure the AWS CLI:

```bash
# Install AWS CLI (macOS)
brew install awscli

# Install AWS CLI (Linux)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Configure credentials
aws configure
```

When prompted, enter:
- AWS Access Key ID
- AWS Secret Access Key
- Default region (e.g., `us-east-1`)
- Default output format (e.g., `json`)

### 3. Python Environment
- Python 3.8 or later
- pip package manager

## Quick Start

### Option 1: Deploy to SageMaker (Recommended)

Deploy a fully configured SageMaker notebook instance with all dependencies:

```bash
cd workshop
./deploy.sh
```

This creates:
- S3 bucket for data storage
- SageMaker notebook instance with the workshop notebook pre-loaded
- IAM roles with required Bedrock and S3 permissions

After deployment completes, open the SageMaker console and launch the notebook instance.

### Option 2: Run Locally

1. Clone this repository:
```bash
git clone https://github.com/awscordsen/sample-blueprint-optimizer-for-data-automation.git
cd sample-blueprint-optimizer-for-data-automation/workshop
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up S3 bucket:
```bash
export BDA_BUCKET=your-bucket-name
aws s3 mb s3://$BDA_BUCKET
```

4. Launch Jupyter and open the notebook:
```bash
jupyter notebook src/purchase-order-optimization-workshop.ipynb
```

## Workshop Notebook

The main workshop is in `src/purchase-order-optimization-workshop.ipynb`. It demonstrates:

1. Creating a blueprint for purchase order field extraction
2. Uploading ground truth samples to S3
3. Running blueprint optimization
4. Testing the optimized blueprint
5. Promoting to production and cleanup

Sample purchase order documents are included in the `samples/` directory.

## IAM Permissions Required

If running locally, ensure your AWS credentials have these permissions:

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
                "bedrock:InvokeBlueprintOptimizationAsync",
                "bedrock:GetBlueprintOptimizationStatus",
                "bedrock:CopyBlueprintStage",
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": "*"
        }
    ]
}
```

## Troubleshooting

**Region availability**: Bedrock Data Automation may not be available in all regions. Check the [AWS Regional Services List](https://aws.amazon.com/about-aws/global-infrastructure/regional-product-services/).

**Credentials**: Verify your AWS credentials are configured correctly:
```bash
aws sts get-caller-identity
```

**S3 bucket**: Ensure the bucket name is globally unique and in the same region as your Bedrock service.

## Resources

- [Blueprint Optimization Documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/bda-optimize-blueprint-info.html)
- [Bedrock Data Automation API Reference](https://docs.aws.amazon.com/bedrock/latest/userguide/bda-using-api.html)
- [Bedrock Data Automation CLI Guide](https://docs.aws.amazon.com/bedrock/latest/userguide/bda-cli-guide.html)

## License

This library is licensed under the MIT-0 License.
