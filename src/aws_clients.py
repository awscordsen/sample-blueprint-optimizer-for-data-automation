import boto3
from botocore.config import Config
from dotenv import load_dotenv
import os
import json
import logging
from typing import Optional, Dict, Any, List, Tuple

from src.path_security import sanitize_filename, validate_path_within_directory

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class AWSClients:
    """Class to manage AWS service clients using environment variables"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AWSClients, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if getattr(self, '_initialized', False):
            return

        try:
            # Get configuration from environment variables
            self.region = os.getenv('AWS_REGION', 'us-west-2')
            logger.info(f"Using AWS region: {self.region}")
            
            self.account_id = os.getenv('ACCOUNT')
            
            # Parse environment variables with explicit error handling
            try:
                max_retries = int(os.getenv('AWS_MAX_RETRIES', '3'))
            except ValueError:
                logger.warning("Invalid AWS_MAX_RETRIES value, using default 3")
                max_retries = 3
                
            try:
                connect_timeout = int(os.getenv('AWS_CONNECT_TIMEOUT', '500'))
            except ValueError:
                logger.warning("Invalid AWS_CONNECT_TIMEOUT value, using default 500")
                connect_timeout = 500
                
            try:
                read_timeout = int(os.getenv('AWS_READ_TIMEOUT', '1000'))
            except ValueError:
                logger.warning("Invalid AWS_READ_TIMEOUT value, using default 1000")
                read_timeout = 1000

            # Configure session
            self.session = boto3.Session(
                region_name=self.region,
            )

            # Configure client
            config = Config(
                retries=dict(
                    max_attempts=max_retries
                ),
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
            )

            # Initialize clients
            self._bda_client = self.session.client('bedrock-data-automation', config=config)
            self._bda_runtime_client = self.session.client('bedrock-data-automation-runtime', config=config)
            self._bedrock_runtime = self.session.client('bedrock-runtime', config=config)
            self._s3_client = self.session.client('s3', config=config)

            self._initialized = True
            logger.info(f"AWS clients initialized with region: {self.region}")

        except ValueError as e:
            logger.error(f"Configuration error initializing AWS clients: {e}")
            raise
        except Exception as e:
            logger.error(f"Error initializing AWS clients: {type(e).__name__}: {e}")
            raise

    @property
    def bda_client(self):
        return self._bda_client

    @property
    def bda_runtime_client(self):
        return self._bda_runtime_client

    @property
    def bedrock_runtime(self):
        return self._bedrock_runtime

    @property
    def s3_client(self):
        return self._s3_client
        
    def download_blueprint(self, blueprint_id: str, project_arn: str, project_stage: str = "LIVE", output_path: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Download a blueprint based on its ID.
        
        Args:
            blueprint_id (str): The ID of the blueprint to download
            project_arn (str): The ARN of the project containing the blueprint
            project_stage (str, optional): The stage of the project. Defaults to "LIVE".
            output_path (str, optional): Path to save the blueprint schema. If None, a default path will be used.
            
        Returns:
            Tuple[str, Dict[str, Any]]: Tuple containing the path to the saved schema file and the blueprint details
        """
        try:
            logger.info(f"Downloading blueprint with ID: {blueprint_id}")  # nosec # nosemgrep: python.lang.security.audit.logging.logger-credential-leak - blueprint_id validated by caller
            
            # Get all blueprints from the project
            blueprints = self._get_project_blueprints(project_arn, project_stage)
            
            if not blueprints:
                raise ValueError(f"No blueprints found in project")
                
            # Find the blueprint with the specified ID
            blueprint = self._find_blueprint_by_id(blueprints, blueprint_id)
            
            if not blueprint:
                raise ValueError(f"No blueprint found with ID: {blueprint_id}")
                
            logger.info(f"Found blueprint: {blueprint.get('blueprintName', 'Unknown')}")
            
            # Get the blueprint details
            response = self._bda_client.get_blueprint(
                blueprintArn=blueprint.get('blueprintArn'),
                blueprintStage=blueprint.get('blueprintStage', 'LIVE')
            )
            
            # Extract schema string from response
            blueprint_details = response.get('blueprint', {})
            schema_str = blueprint_details.get('schema')
            
            if not schema_str:
                raise ValueError("No schema found in blueprint response")
            
            # Determine output path if not provided
            output_dir = "output/blueprints"
            # Validate output_dir is a safe relative path
            if os.path.isabs(output_dir) or '..' in output_dir:
                raise ValueError("Invalid output directory")
            
            if not output_path:
                blueprint_name = blueprint_details.get('blueprintName', 'unknown')
                # Sanitize blueprint_name to prevent path traversal
                safe_blueprint_name = sanitize_filename(blueprint_name)
                safe_blueprint_id = sanitize_filename(blueprint_id)
                os.makedirs(output_dir, exist_ok=True)  # Safe: output_dir is hardcoded and validated
                output_path = os.path.join(output_dir, f"{safe_blueprint_name}_{safe_blueprint_id}.json")  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - inputs sanitized via sanitize_filename()
            else:
                # Validate user-provided output_path stays within allowed directory
                output_path = validate_path_within_directory(output_path, output_dir)
                # Create directory if it doesn't exist - validate first
                parent_dir = os.path.dirname(output_path)
                if parent_dir and not os.path.realpath(parent_dir).startswith(os.path.realpath(output_dir)):
                    raise ValueError("Path traversal detected in parent directory")
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)
            
            # Defense in depth: verify path is within output_dir before writing
            abs_output_dir = os.path.realpath(output_dir)
            abs_output_path = os.path.realpath(output_path)
            if not abs_output_path.startswith(abs_output_dir + os.sep) and abs_output_path != abs_output_dir:
                raise ValueError("Path traversal detected in download_blueprint_by_id")
            
            # Write schema string directly to file using validated absolute path
            with open(abs_output_path, 'w') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - path validated above via realpath check
                f.write(schema_str)
            
            logger.info(f"Blueprint schema saved to {output_path}")
            return output_path, blueprint_details
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error downloading blueprint: {type(e).__name__}")
            raise
    
    def _get_project_blueprints(self, project_arn: str, project_stage: str) -> List[Dict[str, Any]]:
        """
        Get all blueprints from a data automation project.
        
        Args:
            project_arn (str): ARN of the project
            project_stage (str): Project stage ('DEVELOPMENT' or 'LIVE')
            
        Returns:
            List[Dict[str, Any]]: List of blueprints
            
        Raises:
            ValueError: If project_arn or project_stage is invalid
        """
        if not project_arn or not project_stage:
            raise ValueError("project_arn and project_stage are required")
            
        try:
            # Call the API to get project details
            response = self._bda_client.get_data_automation_project(
                projectArn=project_arn,
                projectStage=project_stage
            )
            
            # Extract blueprints from the response
            blueprints = []
            if response and 'project' in response:
                custom_config = response['project'].get('customOutputConfiguration', {})
                blueprints = custom_config.get('blueprints', [])
                
                logger.info(f"Found {len(blueprints)} blueprints in project")
                return blueprints
            else:
                logger.warning("No project data found in response")
                return []
                
        except self._bda_client.exceptions.ResourceNotFoundException as e:
            logger.error(f"Project not found: {project_arn}")
            raise ValueError(f"Project not found: {project_arn}") from e
        except Exception as e:
            logger.error(f"Error getting project blueprints: {type(e).__name__}")
            raise
    
    def _find_blueprint_by_id(self, blueprints: List[Dict[str, Any]], blueprint_id: str) -> Optional[Dict[str, Any]]:
        """
        Find a blueprint by its ID from a list of blueprints.
        
        Args:
            blueprints (List[Dict[str, Any]]): List of blueprints
            blueprint_id (str): The blueprint ID to search for
            
        Returns:
            Optional[Dict[str, Any]]: The matching blueprint or None if not found
            
        Raises:
            ValueError: If blueprint_id is empty
        """
        if not blueprint_id:
            raise ValueError("blueprint_id is required")
            
        if not blueprints:
            return None
            
        # Find blueprint where blueprint_id is in the ARN
        return next(
            (bp for bp in blueprints if blueprint_id in bp.get('blueprintArn', '')),
            None
        )

    def download_blueprint_by_arn(self, blueprint_arn: str, blueprint_stage: str = "LIVE", output_path: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Download a blueprint directly by its ARN.
        
        Args:
            blueprint_arn (str): The ARN of the blueprint to download
            blueprint_stage (str, optional): The stage of the blueprint. Defaults to "LIVE".
            output_path (str, optional): Path to save the blueprint schema. If None, a default path will be used.
            
        Returns:
            Tuple[str, Dict[str, Any]]: Tuple containing the path to the saved schema file and the blueprint details
        """
        try:
            logger.info("Downloading blueprint by ARN")
            
            # Get the blueprint details directly
            response = self._bda_client.get_blueprint(
                blueprintArn=blueprint_arn,
                blueprintStage=blueprint_stage
            )
            
            # Extract schema string from response
            blueprint_details = response.get('blueprint', {})
            schema_str = blueprint_details.get('schema')
            
            if not schema_str:
                raise ValueError("No schema found in blueprint response")
            
            # Determine output path if not provided
            output_dir = "output/blueprints"
            # Validate output_dir is a safe relative path
            if os.path.isabs(output_dir) or '..' in output_dir:
                raise ValueError("Invalid output directory")
            
            if not output_path:
                blueprint_name = blueprint_details.get('blueprintName', 'unknown')
                blueprint_id = blueprint_arn.split('/')[-1]
                # Sanitize names to prevent path traversal
                safe_blueprint_name = sanitize_filename(blueprint_name)
                safe_blueprint_id = sanitize_filename(blueprint_id)
                os.makedirs(output_dir, exist_ok=True)  # Safe: output_dir is hardcoded and validated
                output_path = os.path.join(output_dir, f"{safe_blueprint_name}_{safe_blueprint_id}.json")  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - inputs sanitized via sanitize_filename()
            else:
                # Validate user-provided output_path stays within allowed directory
                output_path = validate_path_within_directory(output_path, output_dir)
                # Create directory if it doesn't exist - validate first
                parent_dir = os.path.dirname(output_path)
                if parent_dir and not os.path.realpath(parent_dir).startswith(os.path.realpath(output_dir)):
                    raise ValueError("Path traversal detected in parent directory")
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)
            
            # Defense in depth: verify path is within output_dir before writing
            abs_output_dir = os.path.realpath(output_dir)
            abs_output_path = os.path.realpath(output_path)
            if not abs_output_path.startswith(abs_output_dir + os.sep) and abs_output_path != abs_output_dir:
                raise ValueError("Path traversal detected in download_blueprint_by_arn")
            
            # Write schema string directly to file using validated absolute path
            with open(abs_output_path, 'w') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - path validated above via realpath check
                f.write(schema_str)
            
            logger.info(f"Blueprint schema saved to {output_path}")
            return output_path, blueprint_details
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error downloading blueprint by ARN: {type(e).__name__}")
            raise

