"""
Schema models for the BDA optimization application.
"""
import os
from typing import Dict, Any, Optional, List, Tuple
from pydantic import BaseModel, Field
import logging

from src.path_security import validate_path_within_directory, validate_file_extension

logger = logging.getLogger(__name__)


class SchemaProperty(BaseModel):
    """
    Represents a property in the JSON schema.
    """
    type: str = Field(description="The data type of the property")
    inferenceType: str = Field(description="The inference type (e.g., 'explicit')")
    instruction: str = Field(description="The instruction for extracting this property")


class Schema(BaseModel):
    """
    Represents the JSON schema for the blueprint.
    """
    schema: str = Field(default="http://json-schema.org/draft-07/schema#", alias="$schema", description="The JSON schema version")
    description: str = Field(description="Description of the document")
    class_: str = Field(alias="class", description="The document class")
    type: str = Field(default="object", description="The schema type")
    definitions: Dict[str, Any] = Field(default_factory=dict, description="Schema definitions")
    properties: Dict[str, Any] = Field(description="Schema properties (can be SchemaProperty instances or nested structures)")

    @classmethod
    def from_file(cls, file_path: str, allowed_dir: str = ".") -> "Schema":
        """
        Load schema from a JSON file.
        Handles both flat and nested blueprint structures.
        
        Args:
            file_path: Path to the JSON file
            allowed_dir: Base directory that file_path must be within
            
        Returns:
            Schema: Loaded schema (preserves original structure)
            
        Raises:
            ValueError: If path traversal is detected or file extension is invalid
        """
        import json
        # Validate path is within allowed directory
        safe_path = validate_path_within_directory(file_path, allowed_dir)
        validate_file_extension(safe_path, ['.json'])
        
        # Path validated by validate_path_within_directory above
        with open(safe_path, 'r') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal
            data = json.load(f)
        
        # Convert properties to SchemaProperty instances if they exist
        if "properties" in data:
            properties = {}
            for field_name, prop_def in data["properties"].items():
                if isinstance(prop_def, dict) and all(key in prop_def for key in ["type", "inferenceType", "instruction"]):
                    # This is a flat property that can be converted to SchemaProperty
                    properties[field_name] = SchemaProperty(**prop_def)
                else:
                    # This might be a nested property, keep as-is for now
                    # The Schema model will handle it appropriately
                    properties[field_name] = prop_def
            data["properties"] = properties
        
        return cls(**data)
    
    def to_file(self, file_path: str, allowed_dir: str = ".") -> None:
        """
        Save schema to a JSON file.
        Preserves nested structure if present.
        
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
        
        # Get the schema as a dictionary
        schema_dict = self.model_dump(by_alias=True)
        
        # Convert SchemaProperty instances back to dictionaries for JSON serialization
        if "properties" in schema_dict:
            properties = {}
            for field_name, prop_def in schema_dict["properties"].items():
                # Check if it's a Pydantic model with model_dump method
                is_pydantic_model = hasattr(prop_def, 'model_dump')
                if is_pydantic_model:
                    properties[field_name] = prop_def.model_dump()
                else:
                    # Already a dictionary (nested structure)
                    properties[field_name] = prop_def
            schema_dict["properties"] = properties
        
        # Path validated by validate_path_within_directory above
        with open(safe_path, 'w') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal
            json.dump(schema_dict, f, indent=4)
    
    def update_instruction(self, field_name: str, instruction: str) -> None:
        """
        Update the instruction for a field.
        Supports both flat field names and dot-notation paths for nested fields.
        
        Args:
            field_name: Name of the field (can be dot-notation for nested fields)
            instruction: New instruction
            
        Raises:
            KeyError: If field_name is not found in properties
            TypeError: If property type doesn't support instruction updates
        """
        if field_name not in self.properties:
            raise KeyError(f"Field '{field_name}' not found in schema properties")
        
        prop = self.properties[field_name]
        if isinstance(prop, SchemaProperty):
            prop.instruction = instruction
        elif isinstance(prop, dict) and "instruction" in prop:
            # Update instruction in dict format (not sensitive data)
            prop["instruction"] = instruction  # nosemgrep: python.lang.security.audit.sensitive-data-leak
        else:
            raise TypeError(f"Property '{field_name}' does not support instruction updates")
    
    def is_nested(self) -> bool:
        """
        Check if this schema contains nested structures.
        
        Returns:
            True if schema contains nested structures, False otherwise
            
        Raises:
            RuntimeError: If schema analysis fails
        """
        from src.services.schema_converter import SchemaFlattener
        try:
            flattener = SchemaFlattener()
            return flattener.is_nested_schema(self.model_dump(by_alias=True))
        except Exception as e:
            logger.error(f"Failed to check if schema is nested: {e}")
            raise RuntimeError("Failed to analyze schema structure") from e
    
    def flatten_for_optimization(self) -> Tuple['Schema', Dict[str, str]]:
        """
        Create flattened version for optimization.
        
        Returns:
            Tuple containing:
            - Flattened Schema instance with dot-notation field names
            - Path mapping for reconstruction
            
        Raises:
            RuntimeError: If schema flattening fails
        """
        from src.services.schema_converter import SchemaFlattener
        
        try:
            flattener = SchemaFlattener()
            schema_dict = self.model_dump(by_alias=True)
            flattened_dict, path_mapping = flattener.flatten_schema(schema_dict)
        except Exception as e:
            logger.error(f"Failed to flatten schema: {str(e)}")
            raise RuntimeError("Failed to flatten schema for optimization") from e
        
        # Convert flattened properties to SchemaProperty instances
        flattened_properties = {}
        for field_name, prop_def in flattened_dict.get("properties", {}).items():
            try:
                flattened_properties[field_name] = SchemaProperty(**prop_def)
            except Exception as e:
                # If we can't create a SchemaProperty, it means this field is malformed
                # Skip it and continue with other fields
                logger.warning(f"Skipping malformed field '{field_name}': {str(e)}")
                continue
        
        # Create new Schema instance with flattened properties
        try:
            flattened_schema = Schema(
                **{k: v for k, v in flattened_dict.items() if k != "properties"},
                properties=flattened_properties
            )
        except Exception as e:
            logger.error(f"Failed to create flattened Schema instance: {str(e)}")
            raise RuntimeError("Failed to create flattened schema") from e
        
        return flattened_schema, path_mapping
    
    def unflatten_from_optimization(self, flat_schema: 'Schema', path_mapping: Dict[str, str]) -> 'Schema':
        """
        Reconstruct nested schema from optimized flat version.
        
        Args:
            flat_schema: Flattened Schema instance with optimized instructions
            path_mapping: Path mapping from flattening operation
            
        Returns:
            Schema instance with original nested structure and optimized instructions
            
        Raises:
            RuntimeError: If schema reconstruction fails
        """
        from src.services.schema_converter import SchemaUnflattener
        
        if not path_mapping:
            # No mapping means it was already flat, return the flat schema
            return flat_schema
        
        try:
            unflattener = SchemaUnflattener()
            flat_dict = flat_schema.model_dump(by_alias=True)
            nested_dict = unflattener.unflatten_schema(flat_dict, path_mapping)
            
            # Convert nested properties back to proper structure
            return Schema(**nested_dict)
        except Exception as e:
            logger.error(f"Failed to unflatten schema: {e}")
            raise RuntimeError("Failed to reconstruct nested schema") from e
