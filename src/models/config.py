"""
Configuration models for the BDA optimization application.
"""
import os
from typing import List, Optional
from pydantic import BaseModel, Field

from src.path_security import validate_path_within_directory, validate_file_extension


class InputField(BaseModel):
    """
    Represents a field in the input data that needs to be extracted.
    """
    instruction: str = Field(description="The instruction for extracting this field")
    data_point_in_document: bool = Field(description="Whether this field exists in the document")
    field_name: str = Field(description="The name of the field to extract")
    expected_output: str = Field(description="The expected output for this field")


class BDAConfig(BaseModel):
    """
    Configuration for the BDA optimization process.
    """
    project_arn: str = Field(description="ARN of the project")
    blueprint_id: str = Field(description="ID of the blueprint")
    data_automation_profile_arn: str = Field(
        description="ARN of the data automation profile",
        alias="dataAutomation_profilearn"  # Support legacy field name
    )
    project_stage: str = Field(description="Stage of the project (e.g., 'LIVE')")
    input_document: str = Field(description="S3 URI for the input document")
    inputs: List[InputField] = Field(description="List of fields to extract")
    
    model_config = {
        "populate_by_name": True  # Allow both field name and alias
    }

    @classmethod
    def from_file(cls, file_path: str, allowed_dir: str = ".") -> "BDAConfig":
        """
        Load configuration from a JSON file.
        
        Args:
            file_path: Path to the JSON file
            allowed_dir: Base directory that file_path must be within
            
        Returns:
            BDAConfig: Loaded configuration
            
        Raises:
            ValueError: If path traversal is detected or file extension is invalid
            FileNotFoundError: If the config file does not exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        import json
        import logging
        logger = logging.getLogger(__name__)
        
        # Validate path is within allowed directory
        safe_path = validate_path_within_directory(file_path, allowed_dir)
        validate_file_extension(safe_path, ['.json'])
        
        if not os.path.isfile(safe_path):
            raise FileNotFoundError(f"Config file not found: {file_path}")
        
        try:
            # Path is validated by validate_path_within_directory above
            with open(safe_path, 'r') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal
                data = json.load(f)
            return cls(**data)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file: {file_path}")
            raise ValueError(f"Invalid JSON in config file: {e}") from e
        except Exception as e:
            logger.error(f"Error loading config from {file_path}: {type(e).__name__}")
            raise
    
    def to_file(self, file_path: str, allowed_dir: str = ".") -> None:
        """
        Save configuration to a JSON file.
        
        Args:
            file_path: Path to save the JSON file
            allowed_dir: Base directory that file_path must be within
            
        Raises:
            ValueError: If path traversal is detected or file extension is invalid
        """
        import json
        # Validate path is within allowed directory
        safe_path = validate_path_within_directory(file_path, allowed_dir)
        validate_file_extension(safe_path, ['.json'])
        
        # Create directory if needed
        os.makedirs(os.path.dirname(safe_path) or '.', exist_ok=True)
        
        # Path is validated by validate_path_within_directory above
        with open(safe_path, 'w') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal
            json.dump(self.model_dump(), f, indent=2)
