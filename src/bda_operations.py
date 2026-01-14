from typing import Dict, Optional
import os
import json
import logging
from dotenv import load_dotenv
from src.aws_clients import AWSClients
from src.path_security import validate_path_within_directory, validate_file_extension

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


class BDAOperations:
    """Class to handle Bedrock Data Automation operations"""

    def __init__(self, project_arn: str, blueprint_arn: str, blueprint_ver: str, blueprint_stage: str, input_s3_uri: str,
                 output_s3_uri: str, profile_arn: str = None):
        """
        Initialize with AWS clients and project configuration

        Args:
            project_arn (str): ARN of the project
            blueprint_arn (str): ARN of the blueprint
            blueprint_ver (str): Version of the blueprint
            blueprint_stage (str): Stage of the blueprint
            input_s3_uri (str): S3 URI for input documents
            output_s3_uri (str): S3 URI for output results
            profile_arn (str, optional): ARN of the data automation profile
        """
        # Get AWS clients
        aws = AWSClients()
        self.bda_runtime_client = aws.bda_runtime_client
        self.bda_client = aws.bda_client

        # Store configuration
        self.project_arn = project_arn
        self.blueprint_arn = blueprint_arn
        self.blueprint_ver = blueprint_ver
        self.blueprint_stage = blueprint_stage
        self.input_s3_uri = input_s3_uri
        self.output_s3_uri = output_s3_uri
        self.region_name = aws.region
        self.profile_arn = profile_arn

        # Validate inputs
        self._validate_config()

    def _validate_config(self):
        """Validate required configuration"""
        required_fields = {
            'project_arn': self.project_arn,
            'blueprint_arn': self.blueprint_arn,
            'blueprint_ver': self.blueprint_ver,
            'blueprint_stage': self.blueprint_stage,
            'input_s3_uri': self.input_s3_uri,
            'output_s3_uri': self.output_s3_uri,
        }

        # Validate all required fields are present
        missing = [k for k, v in required_fields.items() if not v]  # noqa: validation logic
        if missing:
            raise ValueError(
                f"Missing required configuration: {', '.join(missing)}")

    def invoke_data_automation(self) -> Dict:
        """
        Invoke an asynchronous data automation job.

        Returns:
            dict: The response including the invocationArn
            
        Raises:
            ValueError: If required configuration is missing
            RuntimeError: If the API call fails
        """
        try:
            # Create blueprint configuration
            blueprints = [{
                "blueprintArn": self.blueprint_arn,
                "version": self.blueprint_ver,
                "stage": self.blueprint_stage,
            }]

            # Use the profile ARN if provided, otherwise construct it
            profile_arn = self.profile_arn
            if not profile_arn:
                account_id = os.getenv('ACCOUNT')
                if not account_id:
                    raise ValueError("ACCOUNT environment variable is required when profile_arn is not provided")
                profile_arn = f'arn:aws:bedrock:{self.region_name}:{account_id}:data-automation-profile/us.data-automation-v1'

            # Invoke the automation
            response = self.bda_runtime_client.invoke_data_automation_async(
                inputConfiguration={
                    's3Uri': self.input_s3_uri
                },
                outputConfiguration={
                    's3Uri': self.output_s3_uri
                },
                dataAutomationProfileArn=profile_arn,
                dataAutomationConfiguration={
                    'dataAutomationProjectArn': self.project_arn,
                    'stage': 'LIVE'
                }
            )

            invocation_arn = response.get('invocationArn', 'Unknown')
            logger.info(f'Invoked data automation job with invocation ARN: {invocation_arn}')

            return response

        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error invoking data automation: {type(e).__name__}: {e}")
            raise RuntimeError("Failed to invoke data automation") from e

    def update_blueprint(self, schema_path: str, allowed_dir: str = ".") -> Dict:
        """
        Update blueprint with new instructions

        Args:
            schema_path (str): Path to the schema file
            allowed_dir (str): Base directory that schema_path must be within
            
        Returns:
            dict: The response from the API call
            
        Raises:
            ValueError: If path traversal is detected, file extension is invalid, or JSON is invalid
            RuntimeError: If the API call fails
        """
        # Validate path is within allowed directory
        safe_path = validate_path_within_directory(schema_path, allowed_dir)
        validate_file_extension(safe_path, ['.json'])
        
        # Read the schema file as a string to avoid double serialization
        # Path is validated by validate_path_within_directory above
        with open(safe_path, 'r') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - safe_path validated above
            schema_str = f.read()
        
        # Validate that it's valid JSON
        try:
            json.loads(schema_str)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON in schema file") from e
        
        try:
            # Update the blueprint with the schema string directly
            response = self.bda_client.update_test_blueprint(
                blueprintArn=self.blueprint_arn,
                blueprintStage='LIVE',
                schema=schema_str
            )

            blueprint_data = response.get('blueprint')
            if blueprint_data:
                blueprint_name = blueprint_data.get('blueprintName', 'Unknown')
                logger.info(f'Updated instructions for blueprint: {blueprint_name}')

            return response

        except Exception as e:
            logger.error(f"Error updating blueprint: {type(e).__name__}: {e}")
            raise RuntimeError("Failed to update blueprint") from e
