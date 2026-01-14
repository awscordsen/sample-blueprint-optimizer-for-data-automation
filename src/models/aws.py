"""
AWS models for the BDA optimization application.
"""
import logging
import traceback
from typing import Dict, List, Optional, Any, Tuple, Union

from botocore.exceptions import ClientError
from pydantic import BaseModel, Field
import json
import time
import os
import pandas as pd

from src.aws_clients import AWSClients
from src.models.schema import Schema
from src.path_security import validate_path_within_directory

# Configure logging
logger = logging.getLogger(__name__)



class Blueprint(BaseModel):
    """
    Represents a blueprint in the BDA project.
    """
    blueprintArn: str
    blueprintVersion: Optional[str] = None
    blueprintStage: str
    blueprintName: Optional[str] = None
    
    model_config = {
        "extra": "allow"  # Allow extra fields that might be in the response
    }


class BDAClient(BaseModel):
    """
    Client for interacting with AWS BDA services.
    """
    project_arn: str
    blueprint_arn: str
    blueprint_ver: str
    blueprint_stage: str
    input_s3_uri: str
    output_s3_uri: str
    region_name: str = Field(default="us-east-1")
    bedrock_data_automation_client: Any = None
    bedrock_runtime_client: Any = None
    s3_storage_client: Any = None
    test_blueprint_arn: Optional[str] = None
    test_blueprint_stage: Optional[str] = None
    
    model_config = {
        "arbitrary_types_allowed": True
    }
    
    def __init__(self, **data):
        super().__init__(**data)
        # Initialize AWS clients
        aws = AWSClients()
        self.bedrock_data_automation_client = aws.bda_client
        self.bedrock_runtime_client = aws.bda_runtime_client
        self.s3_storage_client = aws.s3_client
    
    def get_blueprint_schema_to_file(self, output_path: str) -> str:
        """
        Get the schema for the blueprint from AWS API and save it to a file.
        
        Args:
            output_path: Path to save the schema file
            
        Returns:
            str: Path to the saved schema file
        """
        try:
            # Validate output path to prevent path traversal
            base_dir = os.path.dirname(output_path) or '.'
            validated_path = validate_path_within_directory(output_path, base_dir)
            
            # Create directory if it doesn't exist
            validated_dir = os.path.dirname(validated_path)
            if validated_dir:
                os.makedirs(validated_dir, exist_ok=True)
            
            # Get blueprint from AWS API
            response = self.bedrock_data_automation_client.get_blueprint(
                blueprintArn=self.blueprint_arn,
                blueprintStage=self.blueprint_stage
            )
            
            # Extract schema string from response
            schema_str = response.get('blueprint', {}).get('schema')
            if not schema_str:
                raise ValueError("No schema found in blueprint response")
            
            # Write schema string to validated path (path traversal protected)
            with open(validated_path, 'w') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal
                f.write(schema_str)
            
            logger.info(f"Blueprint schema saved to {validated_path}")
            return validated_path
            
        except Exception as e:
            logger.error(f"Error getting blueprint schema: {type(e).__name__}")
            raise
    
    @classmethod
    def from_config(cls, config_file: str) -> "BDAClient":
        """
        Create a BDA client from a configuration file.
        
        Args:
            config_file: Path to the configuration file
            
        Returns:
            BDAClient: BDA client
        """
        from src.models.config import BDAConfig
        import os
        
        config = BDAConfig.from_file(config_file)
        
        # Save the profile ARN to environment variable with validation
        if hasattr(config, 'dataAutomation_profilearn') and config.dataAutomation_profilearn:
            # Security: Validate profile ARN format to prevent command injection
            import re
            profile_arn = config.dataAutomation_profilearn
            # AWS ARN format: arn:aws:service:region:account:resource
            arn_pattern = r'^arn:aws:[a-zA-Z0-9\-]+:[a-zA-Z0-9\-]*:[0-9]*:[a-zA-Z0-9\-_/:.]+$'
            if not re.match(arn_pattern, profile_arn):
                raise ValueError(f"Invalid profile ARN format: {profile_arn[:100]}")
            if len(profile_arn) > 500:
                raise ValueError("Profile ARN exceeds maximum allowed length")
            # Setting validated ARN to env var (ARN format validated above)
            os.environ['DATA_AUTOMATION_PROFILE_ARN'] = profile_arn  # nosec B108 # nosemgrep: python.lang.security.audit.os-environ-injection
        
        # Get blueprints
        aws = AWSClients()
        blueprints = cls.get_project_blueprints(
            bda_client=aws.bda_client,
            project_arn=config.project_arn,
            project_stage=config.project_stage
        )
        
        # Find the right blueprint
        found_blueprint = cls.find_blueprint_by_id(blueprints, config.blueprint_id)
        
        # If not found in project blueprints, try to construct ARN and access directly
        if not found_blueprint:
            logger.debug("Blueprint not found in project blueprints, trying direct access...")
            
            # Determine the blueprint ARN
            if config.blueprint_id.startswith('arn:aws:bedrock'):
                # Already a full ARN
                blueprint_arn = config.blueprint_id
            else:
                # Construct ARN from project ARN and blueprint ID
                # Extract region and account from project ARN
                # Format: arn:aws:bedrock:region:account:data-automation-project/project-id
                project_parts = config.project_arn.split(':')
                if len(project_parts) >= 5:
                    region = project_parts[3]
                    account = project_parts[4]
                    blueprint_arn = f"arn:aws:bedrock:{region}:{account}:blueprint/{config.blueprint_id}"
                else:
                    raise ValueError(f"Invalid project ARN format: {config.project_arn}")
            
            try:
                # Try to get blueprint directly by ARN
                response = aws.bda_client.get_blueprint(
                    blueprintArn=blueprint_arn,
                    blueprintStage='LIVE'
                )
                if response and 'blueprint' in response:
                    blueprint_data = response['blueprint']
                    # Create a Blueprint object from the response
                    found_blueprint = Blueprint(
                        blueprintArn=blueprint_data.get('blueprintArn'),
                        blueprintName=blueprint_data.get('blueprintName'),
                        blueprintStage=blueprint_data.get('blueprintStage', 'LIVE'),
                        blueprintVersion=blueprint_data.get('blueprintVersion', '1')
                    )
                    logger.debug(f"Successfully accessed blueprint directly: {found_blueprint.blueprintName}")
            except Exception as e:
                logger.debug(f"Failed to access blueprint directly: {type(e).__name__}")
        
        if not found_blueprint:
            raise ValueError(f"No blueprint found with ID: '{config.blueprint_id}'")
        
        # Use default version "1" if blueprintVersion is None
        blueprint_ver = found_blueprint.blueprintVersion or "1"
        
        # Extract the bucket and path from the input document S3 URI
        from urllib.parse import urlparse
        
        if not config.input_document or not config.input_document.startswith('s3://'):
            raise ValueError(f"Invalid input document S3 URI: {config.input_document}")
        
        parsed_uri = urlparse(config.input_document)
        if not parsed_uri.netloc:
            raise ValueError(f"Invalid S3 URI - missing bucket name: {config.input_document}")
        
        input_s3_uri = config.input_document
        
        # For output, we'll use the same bucket but with an 'output/' prefix
        # This will be overridden by the actual output location from the BDA job
        output_s3_uri = f"s3://{parsed_uri.netloc}/output/"
        
        return cls(
            project_arn=config.project_arn,
            blueprint_arn=found_blueprint.blueprintArn,
            blueprint_ver=blueprint_ver,
            blueprint_stage=found_blueprint.blueprintStage,
            input_s3_uri=input_s3_uri,
            output_s3_uri=output_s3_uri
        )
    
    @staticmethod
    def get_project_blueprints(bda_client, project_arn: str, project_stage: str) -> List[Blueprint]:
        """
        Get all blueprints from a data automation project.
        
        Args:
            bda_client: Bedrock Data Automation client
            project_arn: ARN of the project
            project_stage: Project stage ('DEVELOPMENT' or 'LIVE')
            
        Returns:
            List[Blueprint]: List of blueprints
            
        Raises:
            ClientError: If AWS API call fails
        """
        try:
            # Call the API to get project details
            response = bda_client.get_data_automation_project(
                projectArn=project_arn,
                projectStage=project_stage
            )
            
            # Extract blueprints from the response
            if response and 'project' in response:
                custom_config = response['project'].get('customOutputConfiguration', {})
                blueprint_dicts = custom_config.get('blueprints', [])
                
                # Use list comprehension for better PEP8 compliance
                blueprints = [Blueprint(**bp_dict) for bp_dict in blueprint_dicts]
                
                logger.debug(f"Found {len(blueprints)} blueprints in project {project_arn}")
                return blueprints
            
            logger.warning("No project data found in response")
            return []
                
        except ClientError as e:
            logger.error(f"AWS API error listing blueprints for {project_arn}: {e.response['Error']['Message']}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing blueprints for {project_arn}: {type(e).__name__} - {e}")
            raise
    
    @staticmethod
    def find_blueprint_by_id(blueprints: List[Blueprint], blueprint_id: str) -> Optional[Blueprint]:
        """
        Find a blueprint by its ID from a list of blueprints.
        
        Args:
            blueprints: List of blueprints
            blueprint_id: The blueprint ID to search for (can be full ARN or just ID)
            
        Returns:
            Blueprint or None: The matching blueprint or None if not found
        """
        if not blueprints or not blueprint_id:
            logger.debug("No blueprints or blueprint_id provided for search")
            return None
        
        try:
            # Loop through blueprints and check for matches
            for blueprint in blueprints:
                arn = blueprint.blueprintArn
                if not arn:
                    continue
                
                # If blueprint_id is a full ARN, do exact match
                if blueprint_id.startswith('arn:aws:bedrock') and blueprint_id == arn:
                    logger.debug(f"Found blueprint by exact ARN match: {arn}")
                    return blueprint
                # If blueprint_id is just an ID, check if it's in the ARN
                elif blueprint_id in arn:
                    logger.debug(f"Found blueprint by ID match in ARN: {arn}")
                    return blueprint
            
            # If no match is found
            logger.debug(f"No blueprint found matching ID: {blueprint_id}")
            return None
        except Exception as e:
            logger.error(f"Error searching for blueprint {blueprint_id}: {type(e).__name__}")
            return None

    def create_test_blueprint(self, blueprint_name: str) -> Dict[str, Any]:
        """
        Create a Bedrock Document Analysis blueprint for testing.

        Args:
            blueprint_name: Name for the new blueprint

        Returns:
            dict: Result with 'status' ('success' or 'error') and blueprint details or error message
        """
        if not blueprint_name or not isinstance(blueprint_name, str):
            return {"status": "error", "error_message": "Blueprint name is required"}
        
        try:
            # Get the source blueprint to copy from
            response = self.bedrock_data_automation_client.get_blueprint(
                blueprintArn=self.blueprint_arn,
                blueprintStage=self.blueprint_stage
            )
            source_blueprint = response.get('blueprint')
            if not source_blueprint:
                raise ValueError("No blueprint data in response")

            # Create the new blueprint based on source
            create_response = self.bedrock_data_automation_client.create_blueprint(
                blueprintName=blueprint_name,
                type=source_blueprint.get('type', 'CUSTOM'),
                blueprintStage='DEVELOPMENT',
                schema=source_blueprint.get('schema', '{}')
            )
            
            new_blueprint = create_response.get('blueprint')
            if new_blueprint is None:
                raise ValueError("Blueprint creation failed. No blueprint response received.")

            self.test_blueprint_arn = new_blueprint.get("blueprintArn")
            self.test_blueprint_stage = new_blueprint.get('blueprintStage')
            
            if not self.test_blueprint_arn:
                raise ValueError("Blueprint ARN not found in response")
            
            logger.info(f"Blueprint created successfully: {self.test_blueprint_arn}")
            return {
                "status": "success",
                "blueprint": new_blueprint
            }
            
        except ClientError as e:
            error_msg = e.response['Error']['Message']
            logger.error(f"AWS API error creating blueprint '{blueprint_name}': {error_msg}")
            return {"status": "error", "error_message": error_msg}
        except Exception as e:
            logger.error(f"Error creating blueprint '{blueprint_name}': {type(e).__name__} - {e}")
            return {"status": "error", "error_message": str(e)}

    def update_test_blueprint(self, schema_path: str) -> bool:
        return self._update_blueprint( schema_path, self.test_blueprint_arn, self.test_blueprint_stage)

    def update_customer_blueprint(self, schema_path: str) -> bool:
        return self._update_blueprint(schema_path, self.blueprint_arn, self.blueprint_stage)

    def _update_blueprint(self, schema_path: str, blueprint_arn, blueprint_stage ) -> bool:
        """
        Update blueprint with new schema.
        
        Args:
            schema_path: Path to the schema file
            
        Returns:
            bool: Whether the update was successful
        """
        try:
            # Validate schema path to prevent path traversal using proper validation
            base_dir = os.path.dirname(schema_path) or '.'
            validated_path = validate_path_within_directory(schema_path, base_dir)
            
            if not os.path.isfile(validated_path):
                raise ValueError(f"Schema file not found: {schema_path}")
            
            # Read the schema file using validated path (path traversal protected)
            with open(validated_path, 'r') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal
                schema_str = f.read()
            
            # Validate that it's valid JSON and parse once for efficiency
            try:
                schema_dict = json.loads(schema_str)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in schema file '{schema_path}': {e}")
                raise ValueError(f"Invalid JSON in schema file: {e}") from e
            
            # Clean up schema to remove conflicts between nested and flattened properties
            cleaned_schema = self._clean_schema_for_blueprint(schema_dict)
            cleaned_schema_str = json.dumps(cleaned_schema, indent=2)
            
            # Update the blueprint with the cleaned schema string
            response = self.bedrock_data_automation_client.update_blueprint(
                blueprintArn=blueprint_arn,
                blueprintStage=blueprint_stage,
                schema=cleaned_schema_str,
            )
            
            blueprint_data = response.get('blueprint', {})
            blueprint_name = blueprint_data.get('blueprintName', 'Unknown')
            logger.info(f'\nUpdated instructions for blueprint: {blueprint_name}')
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating blueprint: {str(e)}")
            return False
    
    def _clean_schema_for_blueprint(self, schema_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean schema to remove conflicts between nested definitions and flattened properties.
        
        Args:
            schema_dict: The schema dictionary to clean
            
        Returns:
            Cleaned schema dictionary
        """
        cleaned_schema = schema_dict.copy()
        
        # If we have both definitions and flattened properties, remove the nested references
        if "definitions" in cleaned_schema and "properties" in cleaned_schema:
            properties = cleaned_schema["properties"].copy()
            
            # Remove properties that reference definitions (nested objects)
            for prop_name in list(properties.keys()):
                prop_def = properties[prop_name]
                if isinstance(prop_def, dict) and "$ref" in prop_def:
                    # This is a reference to a definition, remove it since we have flattened properties
                    del properties[prop_name]
                    logger.info(f"Removed nested reference property: {prop_name}")
            
            cleaned_schema["properties"] = properties
            
            # Also remove definitions if we have flattened properties
            has_flattened_props = any("." in p for p in properties.keys())
            if has_flattened_props:
                logger.info("Removing definitions section due to flattened properties")
                del cleaned_schema["definitions"]
        
        return cleaned_schema
    
    def invoke_data_automation(self) -> Dict[str, Any]:
        """
        Invoke an asynchronous data automation job.
        
        Returns:
            dict: The response including the invocationArn
            
        Raises:
            ValueError: If required configuration is missing
            ClientError: If AWS API call fails
        """
        try:
            logger.info(f"Invoking data automation job for {self.project_arn} with blueprint {self.blueprint_arn}")
            
            if not self.test_blueprint_arn:
                raise ValueError("Test blueprint ARN not set. Call create_test_blueprint first.")
            
            # Create blueprint configuration
            blueprints = [{
                "blueprintArn": self.test_blueprint_arn,
                "stage": 'DEVELOPMENT',
            }]
            
            # Get the profile ARN from the environment
            profile_arn = os.getenv('DATA_AUTOMATION_PROFILE_ARN')
            if not profile_arn:
                raise ValueError("DATA_AUTOMATION_PROFILE_ARN environment variable not set")
            
            # Invoke the automation
            response = self.bedrock_runtime_client.invoke_data_automation_async(
                inputConfiguration={
                    's3Uri': self.input_s3_uri
                },
                outputConfiguration={
                    's3Uri': self.output_s3_uri
                },
                dataAutomationProfileArn=profile_arn,
                blueprints=blueprints
            )
            
            invocation_arn = response.get('invocationArn')
            if not invocation_arn:
                raise RuntimeError("No invocation ARN returned from data automation")
            
            logger.info(f'Invoked data automation job with invocation ARN: {invocation_arn}')
            return response
            
        except ClientError as e:
            logger.error(f"AWS API error invoking data automation: {e.response['Error']['Message']}")
            raise
        except Exception as e:
            logger.error(f"Error invoking data automation: {type(e).__name__} - {e}")
            raise
    
    def check_job_status(self, invocation_arn: str, max_attempts: int = 30, sleep_time: int = 10) -> Dict[str, Any]:
        """
        Check the status of a Bedrock Data Analysis job until completion or failure.
        
        Args:
            invocation_arn: The ARN of the job invocation
            max_attempts: Maximum number of status check attempts
            sleep_time: Time to wait between status checks in seconds
            
        Returns:
            dict: The final response from the get_data_automation_status API
            
        Raises:
            TimeoutError: If job does not complete within max_attempts
            ClientError: If AWS API call fails
        """
        if not invocation_arn:
            raise ValueError("invocation_arn is required")
        
        attempts = 0
        while attempts < max_attempts:
            try:
                response = self.bedrock_runtime_client.get_data_automation_status(
                    invocationArn=invocation_arn
                )
                
                status = response.get('status')
                logger.debug(f"Job status check {attempts + 1}/{max_attempts}: {status}")
                
                # Check if job has reached a final state
                if status in ['Success', 'ServiceError', 'ClientError']:
                    logger.info(f"Job completed with final status: {status}")
                    if status == 'Success':
                        logger.debug(f"Results location: {response.get('outputConfiguration', {}).get('s3Uri')}")
                    else:
                        error_msg = response.get('errorMessage', 'Unknown error')
                        logger.error(f"Job failed with status {status}: {error_msg}")
                    return response
                    
                # If job is still running, wait before checking again
                elif status in ['Created', 'InProgress']:
                    logger.debug(f"Job is {status}. Waiting {sleep_time}s before next check.")
                    # nosemgrep: python.lang.best-practice.arbitrary-sleep
                    time.sleep(sleep_time)  # Intentional polling delay for AWS async job status
                    
                else:
                    logger.warning(f"Unexpected job status: {status}")
                    return response
                    
            except ClientError as e:
                logger.error(f"AWS API error checking job status: {e.response['Error']['Message']}")
                raise
            except Exception as e:
                logger.error(f"Error checking job status for {invocation_arn}: {type(e).__name__} - {e}")
                raise
                
            attempts += 1
            
        logger.warning(f"Maximum attempts ({max_attempts}) reached. Job did not complete.")
        raise TimeoutError(f"Job did not complete within {max_attempts} attempts ({max_attempts * sleep_time}s)")
    
    def _extract_custom_output_path(self, job_response: Dict[str, Any]) -> str:
        """
        Extract the custom output path from a successful BDA job response.
        
        Args:
            job_response: The response from check_job_status
            
        Returns:
            str: The custom output path from S3
            
        Raises:
            ValueError: If required fields are missing from the response
        """
        output_config = job_response.get('outputConfiguration', {})
        job_metadata_s3_location = output_config.get('s3Uri')
        if not job_metadata_s3_location:
            raise ValueError("No S3 URI in job output configuration")
        
        job_metadata = json.loads(self._read_s3_object(job_metadata_s3_location))
        output_metadata = job_metadata.get('output_metadata', [])
        if not output_metadata:
            raise ValueError("No output_metadata in job response")
        
        segment_metadata = output_metadata[0].get('segment_metadata', [])
        if not segment_metadata:
            raise ValueError("No segment_metadata in job response")
        
        custom_output_path = segment_metadata[0].get('custom_output_path')
        if not custom_output_path:
            raise ValueError("No custom_output_path in job response")
        
        return custom_output_path
    
    def _save_dataframe(self, df: pd.DataFrame, output_dir: str, filename: str) -> str:
        """
        Save a DataFrame to CSV in the specified directory.
        
        Args:
            df: DataFrame to save
            output_dir: Directory to save to
            filename: Name of the CSV file
            
        Returns:
            str: Full path to the saved file
        """
        os.makedirs(output_dir, exist_ok=True)
        filepath = f"{output_dir}/{filename}"
        df.to_csv(filepath, index=False)
        return filepath
    
    def run_bda_job(self, input_df: pd.DataFrame, iteration: int, timestamp: str) -> Tuple[pd.DataFrame, Dict[str, float], bool]:
        """
        Run a BDA job and process the results.
        
        Args:
            input_df: Input DataFrame with expected values
            iteration: Current iteration number
            timestamp: Timestamp for file naming
            
        Returns:
            Tuple containing:
                - DataFrame with similarity scores
                - Dictionary of similarity scores by field
                - Whether the job was successful (always True if no exception)
                
        Raises:
            RuntimeError: If job invocation or execution fails
            ValueError: If job response is missing required data
        """
        from src.util_sequential import extract_similarities_from_dataframe
        from src.util import add_semantic_similarity_column, merge_bda_and_input_dataframes, extract_inference_from_s3_to_df
        
        logger.info(f"Running BDA job for iteration {iteration}...")
        
        # Invoke automation and get invocation ARN
        response = self.invoke_data_automation()
        invocation_arn = response.get('invocationArn')
        
        # Check job status until completion
        job_response = self.check_job_status(
            invocation_arn=invocation_arn,
            max_attempts=int(os.getenv("JOB_MAX_TRIES", "60")),
            sleep_time=int(os.getenv("SLEEP_TIME", "15"))
        )
        
        # Handle job result
        job_status = job_response.get('status')
        if job_status != 'Success':
            error_msg = job_response.get('errorMessage', 'Unknown error')
            logger.error(f"Job failed with status {job_status}: {error_msg}")
            raise RuntimeError(f"BDA job failed with status: {job_status}")
        
        # Extract output path from job metadata
        custom_output_path = self._extract_custom_output_path(job_response)
        
        # Extract BDA results and save
        df_bda, _ = extract_inference_from_s3_to_df(custom_output_path)
        self._save_dataframe(df_bda, "output/bda_output/sequential", f"df_bda_{iteration}_{timestamp}.csv")
        
        # Merge with input data and save
        merged_df = merge_bda_and_input_dataframes(df_bda, input_df)
        self._save_dataframe(merged_df, "output/merged_df_output/sequential", f"merged_df_{iteration}_{timestamp}.csv")
        
        # Calculate similarity scores and save
        df_with_similarity = add_semantic_similarity_column(merged_df, threshold=0.0)
        self._save_dataframe(df_with_similarity, "output/similarity_output/sequential", f"similarity_df_{iteration}_{timestamp}.csv")
        
        # Extract and log similarities by field
        similarities = extract_similarities_from_dataframe(df_with_similarity)
        logger.debug("Similarity Scores: %s", {k: f"{v:.4f}" for k, v in similarities.items()})
        
        return df_with_similarity, similarities, True
    
    def _read_s3_object(self, s3_uri: str, as_bytes: bool = False) -> Union[str, bytes]:
        """
        Read an object from S3.
        
        Args:
            s3_uri: S3 URI of the object
            as_bytes: Whether to return the object as bytes
            
        Returns:
            Union[str, bytes]: The object content as string (default) or bytes if as_bytes=True
        """
        from urllib.parse import urlparse
        
        # Validate S3 URI
        if not s3_uri or not isinstance(s3_uri, str):
            raise ValueError("S3 URI must be a non-empty string")
        if not s3_uri.startswith('s3://'):
            raise ValueError(f"Invalid S3 URI format: {s3_uri}")
        
        # Parse the S3 URI
        parsed_uri = urlparse(s3_uri)
        bucket_name = parsed_uri.netloc
        object_key = parsed_uri.path.lstrip('/')
        
        if not bucket_name or not object_key:
            raise ValueError(f"Invalid S3 URI - missing bucket or key: {s3_uri}")
        
        try:
            # Get the object from S3
            response = self.s3_storage_client.get_object(Bucket=bucket_name, Key=object_key)
            
            # Read the content of the object
            if as_bytes:
                content = response['Body'].read()
            else:
                content = response['Body'].read().decode('utf-8')
            return content
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"AWS S3 error reading {s3_uri}: {error_code} - {e.response['Error']['Message']}")
            raise
        except UnicodeDecodeError as e:
            logger.error(f"Failed to decode S3 object as UTF-8: {s3_uri}")
            raise ValueError(f"S3 object is not valid UTF-8 text: {e}") from e
        except Exception as e:
            logger.error(f"Error reading S3 object {s3_uri}: {type(e).__name__} - {e}")
            raise


    def delete_test_blueprint(self) -> bool:
        """Delete the test blueprint from BDA.
        
        Returns:
            bool: True if deletion was successful
            
        Raises:
            ValueError: If test blueprint ARN is not set
            ClientError: If AWS API call fails
        """
        if not self.test_blueprint_arn:
            raise ValueError("Test blueprint ARN not set. Nothing to delete.")
        
        try:
            logger.info(f"Cleanup - deleting development blueprint {self.test_blueprint_arn}")
            self.bedrock_data_automation_client.delete_blueprint(
                blueprintArn=self.test_blueprint_arn
            )
            logger.info(f"Successfully deleted blueprint {self.test_blueprint_arn}")
            return True

        except ClientError as e:
            logger.error(f"AWS API error deleting blueprint: {e.response['Error']['Message']}")
            raise
        except Exception as e:
            logger.error(f"Error deleting blueprint {self.test_blueprint_arn}: {type(e).__name__} - {e}")
            raise
