"""
Result models for the BDA optimization application.
"""
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
import pandas as pd
import os
import json
import html
import logging

from src.path_security import validate_path_within_directory

# Configure logging
logger = logging.getLogger(__name__)


class BoundingBox(BaseModel):
    """
    Represents a bounding box in a document.
    """
    left: float
    top: float
    width: float
    height: float


class Geometry(BaseModel):
    """
    Represents geometry information for a field.
    """
    page: int
    boundingBox: Optional[BoundingBox] = None


class FieldExplainability(BaseModel):
    """
    Represents explainability information for a field.
    """
    confidence: float
    geometry: List[Geometry] = Field(default_factory=list)


class BDAResult(BaseModel):
    """
    Represents the result of a BDA job.
    """
    field_name: str
    value: str
    confidence: Optional[float] = None
    page: Optional[int] = None
    bounding_box: Optional[str] = None

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List["BDAResult"]:
        """
        Create BDA results from a DataFrame.
        
        Args:
            df: DataFrame with BDA results
            
        Returns:
            List[BDAResult]: List of BDA results
            
        Raises:
            KeyError: If required columns are missing
        """
        required_cols = ["field_name", "value"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Missing required columns: {missing_cols}")
        
        results = []
        for _, row in df.iterrows():
            results.append(cls(
                field_name=row.get("field_name", ""),
                value=row.get("value", ""),
                confidence=row.get("confidence"),
                page=row.get("page"),
                bounding_box=row.get("bounding_box")
            ))
        return results


class BDAResponse(BaseModel):
    """
    Represents the response from a BDA job.
    """
    inference_result: Dict[str, str]
    explainability_info: List[Dict[str, FieldExplainability]]
    document_class: Dict[str, str]

    @classmethod
    def from_s3(cls, s3_uri: str) -> "BDAResponse":
        """
        Create a BDA response from an S3 URI.
        
        Args:
            s3_uri: S3 URI of the JSON file
            
        Returns:
            BDAResponse: BDA response
            
        Raises:
            ValueError: If s3_uri is invalid or empty
            json.JSONDecodeError: If the S3 object is not valid JSON
            RuntimeError: If failed to read from S3
        """
        # Validate input
        if not s3_uri or not isinstance(s3_uri, str):
            raise ValueError("s3_uri must be a non-empty string")
        if not s3_uri.startswith("s3://"):
            raise ValueError("s3_uri must start with 's3://'")
        
        try:
            from src.util import read_s3_object
            s3_content = read_s3_object(s3_uri)
            if not s3_content:
                raise RuntimeError(f"Empty response from S3: {s3_uri}")
            json_data = json.loads(s3_content)
            return cls(**json_data)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in S3 object {s3_uri}: {type(e).__name__}")
            raise
        except Exception as e:
            logger.error(f"Failed to read from S3 {s3_uri}: {type(e).__name__}")
            raise RuntimeError(f"Failed to read BDA response from S3") from e
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert BDA response to a DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame with BDA results
            
        Raises:
            ValueError: If explainability_info is empty
        """
        if not self.explainability_info:
            raise ValueError("explainability_info cannot be empty")
        
        records = []
        explainability = self.explainability_info[0] if self.explainability_info else {}
        
        for field, value in self.inference_result.items():
            info = explainability.get(field, {})
            confidence = round(info.confidence, 4) if hasattr(info, 'confidence') else None

            geometry = info.geometry if hasattr(info, 'geometry') else []
            page = geometry[0].page if geometry else None
            bbox = geometry[0].boundingBox if geometry and hasattr(geometry[0], 'boundingBox') else None

            records.append({
                "field_name": field,
                "value": value,
                "confidence": confidence,
                "page": page,
                "bounding_box": json.dumps(bbox.model_dump()) if bbox else None
            })

        return pd.DataFrame(records)
    
    def save_to_csv(self, output_path: str) -> str:
        """
        Save BDA response to a CSV file.
        
        Args:
            output_path: Path to save the CSV file
            
        Returns:
            str: Path to the saved CSV file
            
        Raises:
            ValueError: If path traversal is detected
        """
        try:
            # Validate path is within parent directory
            output_dir = os.path.dirname(output_path)
            if output_dir:
                abs_output_dir = os.path.realpath(output_dir)
                abs_output_path = os.path.realpath(output_path)
                if not abs_output_path.startswith(abs_output_dir):
                    raise ValueError("Path traversal detected in output_path")
            
            df = self.to_dataframe()
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"BDA results saved to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error saving BDA results: {type(e).__name__}")
            raise
    
    def save_to_html(self, output_path: str) -> str:
        """
        Save BDA response to an HTML file.
        
        Args:
            output_path: Path to save the HTML file
            
        Returns:
            str: Path to the saved HTML file
            
        Raises:
            ValueError: If path traversal is detected
        """
        try:
            # Validate path is within parent directory
            output_dir = os.path.dirname(output_path)
            if output_dir:
                abs_output_dir = os.path.realpath(output_dir)
                validate_path_within_directory(output_path, abs_output_dir)
            
            df = self.to_dataframe()
            
            # Extract document class
            document_class = self.document_class.get("type", "N/A")
            
            # Convert DataFrame to HTML table (escape=True to prevent XSS)
            table_html = df.to_html(index=False, escape=True)
            
            # Escape user-provided data to prevent XSS
            safe_document_class = html.escape(str(document_class))
            
            # HTML template
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Document Analysis</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        padding: 20px;
                        background-color: #f9f9f9;
                    }}
                    h2 {{
                        color: #2c3e50;
                    }}
                    table {{
                        border-collapse: collapse;
                        width: 100%;
                        margin-top: 20px;
                    }}
                    th, td {{
                        border: 1px solid #ccc;
                        padding: 10px;
                        text-align: left;
                    }}
                    th {{
                        background-color: #4CAF50;
                        color: white;
                    }}
                    tr:nth-child(even) {{
                        background-color: #f2f2f2;
                    }}
                    .document-class {{
                        font-size: 18px;
                        font-weight: bold;
                        margin-bottom: 20px;
                    }}
                </style>
            </head>
            <body>
                <div class="document-class">Document Class: {safe_document_class}</div>
                {table_html}
            </body>
            </html>
            """
            
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                validate_path_within_directory(output_path, os.path.realpath(output_dir))
            # Path validated by validate_path_within_directory above
            with open(output_path, 'w', encoding='utf-8') as f:  # nosec B603 - path validated
                f.write(html_content)
            
            logger.info(f"HTML saved to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error saving HTML: {type(e).__name__}")
            raise


class MergedResult(BaseModel):
    """
    Represents a merged result of BDA and input data.
    """
    field: str
    instruction: str
    value: str
    confidence: Optional[float] = None
    expected_output: str
    data_in_document: bool
    semantic_similarity: Optional[float] = None
    semantic_match: Optional[bool] = None

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List["MergedResult"]:
        """
        Create merged results from a DataFrame.
        
        Args:
            df: DataFrame with merged results
            
        Returns:
            List[MergedResult]: List of merged results
            
        Raises:
            KeyError: If required columns are missing
        """
        required_cols = ["Field", "Instruction", "Value (BDA Response)", "Expected Output", "Data in Document"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Missing required columns: {missing_cols}")
        
        results = []
        for _, row in df.iterrows():
            results.append(cls(
                field=row.get("Field", ""),
                instruction=row.get("Instruction", ""),
                value=row.get("Value (BDA Response)", ""),
                confidence=row.get("Confidence"),
                expected_output=row.get("Expected Output", ""),
                data_in_document=row.get("Data in Document", False),
                semantic_similarity=row.get("semantic_similarity"),
                semantic_match=row.get("semantic_match")
            ))
        return results
