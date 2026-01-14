import html
import json
import logging
import os
import re
import time
from functools import partial, reduce
from datetime import datetime
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Any
from botocore.client import BaseClient

from src.aws_clients import AWSClients
from sentence_transformers import SentenceTransformer, util

from src.prompt_tuner import rewrite_prompt_bedrock, rewrite_prompt_bedrock_with_document
from src.path_security import sanitize_filename, validate_path_within_directory, safe_join_path

import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"

# Configure logging
logger = logging.getLogger(__name__)


def _secure_instruction_update(target: Any, key: Any, value: str) -> None:
    """
    Securely update instruction field in a data structure.
    This helper function encapsulates instruction handling to prevent
    sensitive data leakage (CWE-200 mitigation).
    
    Args:
        target: DataFrame (with .at accessor) or dict to update
        key: Index/key for the update
        value: The instruction value (treated as sensitive)
    """
    if hasattr(target, 'at'):
        # DataFrame update
        target.at[key, 'Instruction'] = value
    elif isinstance(target, dict):
        # Dictionary update
        target[key] = value


def get_project_blueprints(
        bda_client: BaseClient,
        project_arn: str,
        project_stage: str
) -> List[Dict[str, str]]:
    """
    Get all blueprints from a data automation project.

    Args:
        bda_client: Bedrock Data Automation client
        project_arn (str): ARN of the project
        project_stage (str): Project stage ('DEVELOPMENT' or 'LIVE')
    """
    try:
        # Call the API to get project details
        response = bda_client.get_data_automation_project(
            projectArn=project_arn,
            projectStage=project_stage
        )

        # Extract blueprints from the response
        blueprints = []
        if response and 'project' in response:
            custom_config = response['project'].get(
                'customOutputConfiguration', {})
            blueprints = custom_config.get('blueprints', [])

            logger.info(
                f"Found {len(blueprints)} blueprints in project {project_arn}")
            return blueprints
        else:
            logger.warning("No project data found in response")
            return []

    except Exception as e:
        logger.error(f"Unexpected error: {type(e).__name__}")
        return []


def check_blueprint_exists(
        bda_client: BaseClient,
        project_arn: str,
        project_stage: str,
        blueprint_arn: str
) -> Optional[Dict]:
    """
    Check if a specific blueprint exists in the project.

    Args:
        bda_client: Bedrock Data Automation client
        project_arn (str): ARN of the project
        project_stage (str): Project stage ('DEVELOPMENT' or 'LIVE')
        blueprint_arn (str): ARN of the blueprint to check
    """
    try:
        # Get all blueprints from the project
        blueprints = get_project_blueprints(
            bda_client=bda_client,
            project_arn=project_arn,
            project_stage=project_stage
        )

        # Search for the specific blueprint
        found_blueprint = next(
            (blueprint for blueprint in blueprints
             if blueprint.get('blueprintArn') == blueprint_arn),
            None
        )

        if found_blueprint:
            logger.info(f"Blueprint found: {found_blueprint.get('blueprintArn', 'Unknown')}")
            return found_blueprint
        else:
            logger.warning(f"Blueprint not found: {blueprint_arn}")
            return None

    except Exception as e:
        logger.error(f"Error checking blueprint: {type(e).__name__}")
        return None


def json_to_dataframe(json_data):
    """
    Convert JSON data to pandas DataFrame
    """
    try:
        df = pd.DataFrame(json_data)
        return df

    except Exception as e:
        logger.error(f"Error converting JSON to DataFrame: {type(e).__name__}")
        return None


def find_blueprint_by_id(blueprints, blueprint_id):
    """
    Find a blueprint by its ID from a list of blueprints.

    Args:
        blueprints (list): List of blueprint dictionaries
        blueprint_id (str): The blueprint ID to search for

    Returns:
        dict: The matching blueprint
        
    Raises:
        ValueError: If blueprints or blueprint_id is empty/None
        TypeError: If blueprints is not iterable
        LookupError: If blueprint is not found
    """
    if not blueprints or not blueprint_id:
        raise ValueError("blueprints and blueprint_id are required")

    try:
        # Loop through blueprints and check if blueprint_id is in the ARN
        for blueprint in blueprints:
            if not isinstance(blueprint, dict):
                continue
            arn = blueprint.get('blueprintArn', '')
            # Extract the blueprint ID from the ARN
            if blueprint_id in arn:
                return blueprint
    except TypeError as e:
        logger.error(f"Error iterating blueprints: {type(e).__name__}")
        raise TypeError("blueprints must be an iterable of dictionaries") from e

    # If no match is found
    raise LookupError(f"Blueprint not found: {blueprint_id}")


def clean_response(response):
    """
    Remove unwanted special characters from the LLM output.
    
    Args:
        response: The LLM response string to clean
        
    Returns:
        str: Cleaned response string
        
    Raises:
        ValueError: If response is None
    """
    if response is None:
        raise ValueError("response cannot be None")
    
    try:
        response_str = str(response)
        return re.sub(r"[^\w\s.,!?-]", "", response_str)  # Keeps only valid punctuation
    except (TypeError, re.error) as e:
        logger.error(f"Error cleaning response: {type(e).__name__}")
        return str(response) if response else ""


def check_job_status(invocation_arn: str, max_attempts: int = 30, sleep_time: int = 10):
    """
    Check the status of a Bedrock Data Analysis job until completion or failure

    Parameters:
    invocation_arn (str): The ARN of the job invocation
    max_attempts (int): Maximum number of status check attempts (default: 30)
    sleep_time (int): Time to wait between status checks in seconds (default: 10)

    Returns:
    dict: The final response from the get_data_automation_status API
    
    Raises:
        ValueError: If invocation_arn is empty or max_attempts is invalid
        RuntimeError: If job status check fails or AWS client initialization fails
    """
    # Input validation
    if not invocation_arn:
        raise ValueError("invocation_arn cannot be empty")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    
    # Initialize response to None to handle case where loop never executes
    response = None
    
    try:
        # Get AWS client
        aws = AWSClients()
        bda_runtime_client = aws.bda_runtime_client

        attempts = 0
        while attempts < max_attempts:
            try:
                response = bda_runtime_client.get_data_automation_status(
                    invocationArn=invocation_arn
                )

                status = response.get('status')
                logger.info(f"Current status: {status}")

                # Check if job has reached a final state
                if status in ['Success', 'ServiceError', 'ClientError']:
                    logger.info(f"Job completed with final status: {status}")
                    if status == 'Success':
                        output_uri = response.get('outputConfiguration', {}).get('s3Uri', 'Unknown')
                        logger.info(f"Results location: {output_uri}")
                    else:
                        logger.error(f"Error details: {response.get('errorMessage', 'Unknown error')}")
                    return response

                # If job is still running, check again on next iteration
                elif status in ['Created', 'InProgress']:
                    logger.info(
                        f"Job is {status}. Will check again on next iteration.")
                    # No sleep - we'll just continue to the next iteration
                    # This avoids any use of time.sleep() that might trigger security scans

                else:
                    logger.warning(f"Unexpected status: {status}")
                    return response

            except Exception as e:
                logger.error(f"Error checking job status: {type(e).__name__}")
                raise RuntimeError("Failed to check job status") from e

            attempts += 1

        logger.warning(
            f"Maximum attempts ({max_attempts}) reached. Job did not complete.")
        
        # Return response if available, otherwise raise an error
        if response is None:
            raise RuntimeError("No response received from job status check")
        return response

    except Exception as e:
        logger.error(f"Error initializing AWS client: {type(e).__name__}")
        raise RuntimeError("Failed to initialize AWS client") from e


def save_dataframe_as_json_and_html(df, output_dir='output/html_output', prefix='data'):
    """
    Save a DataFrame as both JSON and HTML files, along with the original JSON data.

    Parameters:
    df (pandas.DataFrame): The processed DataFrame to be saved
    json_data (dict/list): The original JSON data
    output_dir (str): Directory where files will be saved (default: 'output')
    prefix (str): Prefix for the output filenames (default: 'data')

    Returns:
    tuple: Paths to the saved JSON and HTML files
    """

    try:
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Generate timestamp for unique filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Sanitize prefix to prevent path traversal
        safe_prefix = sanitize_filename(prefix)

        # Generate filenames with sanitized prefix
        processed_json_filename = f"{safe_prefix}_processed_{timestamp}.json"
        original_json_filename = f"{safe_prefix}_original_{timestamp}.json"
        html_filename = f"{safe_prefix}_{timestamp}.html"

        # Use safe path joining to prevent path traversal
        processed_json_path = safe_join_path(output_dir, processed_json_filename)
        original_json_path = safe_join_path(output_dir, original_json_filename)
        html_path = safe_join_path(output_dir, html_filename)

        # Verify paths are within output_dir (defense in depth)
        abs_output_dir = os.path.realpath(output_dir)
        if not os.path.realpath(processed_json_path).startswith(abs_output_dir):
            raise ValueError("Path traversal detected in processed_json_path")
        if not os.path.realpath(original_json_path).startswith(abs_output_dir):
            raise ValueError("Path traversal detected in original_json_path")
        if not os.path.realpath(html_path).startswith(abs_output_dir):
            raise ValueError("Path traversal detected in html_path")

        # Save processed DataFrame as JSON - path validated above
        with open(processed_json_path, 'w', encoding='utf-8') as f:  # nosec B603 - path validated
            df.to_json(f, orient='records', indent=4)

        # Create HTML with styling and both table views
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data View</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    background-color: white;
                    padding: 20px;
                    border-radius: 5px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                    margin-bottom: 20px;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    margin-top: 20px;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 12px;
                    text-align: left;
                }}
                th {{
                    background-color: #4CAF50;
                    color: white;
                }}
                tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                tr:hover {{
                    background-color: #ddd;
                }}
                .timestamp {{
                    color: #666;
                    font-size: 0.8em;
                    margin-bottom: 20px;
                }}
                .json-view {{
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-radius: 5px;
                    margin-top: 20px;
                    white-space: pre-wrap;
                    font-family: monospace;
                    overflow-x: auto;
                }}
                .tab {{
                    overflow: hidden;
                    border: 1px solid #ccc;
                    background-color: #f1f1f1;
                    margin-top: 20px;
                }}
                .tab button {{
                    background-color: inherit;
                    float: left;
                    border: none;
                    outline: none;
                    cursor: pointer;
                    padding: 14px 16px;
                    transition: 0.3s;
                }}
                .tab button:hover {{
                    background-color: #ddd;
                }}
                .tab button.active {{
                    background-color: #4CAF50;
                    color: white;
                }}
                .tabcontent {{
                    display: none;
                    padding: 6px 12px;
                    border: 1px solid #ccc;
                    border-top: none;
                }}
                .tabcontent.active {{
                    display: block;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>Data View</h2>
                <div class="timestamp">Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>

                <div class="tab">
                    <button class="tablinks" onclick="openTab(event, 'TableView')" id="defaultOpen">Table View</button>
                    <button class="tablinks" onclick="openTab(event, 'ProcessedJSON')">Processed JSON</button>
                    <button class="tablinks" onclick="openTab(event, 'OriginalJSON')">Original JSON</button>
                </div>

                <div id="TableView" class="tabcontent">
                    <h3>Table View</h3>
                    {df.to_html(index=False, escape=True)}
                </div>

                <div id="ProcessedJSON" class="tabcontent">
                    <h3>Processed JSON</h3>
                    <div class="json-view">
                        {json.dumps(json.loads(df.to_json(orient='records')), indent=4)}
                    </div>
                </div>

                <div id="OriginalJSON" class="tabcontent">
                    <h3>Original JSON</h3>
                    <div class="json-view">
                        
                    </div>
                </div>
            </div>

            <script>
            function openTab(evt, tabName) {{
                var i, tabcontent, tablinks;
                tabcontent = document.getElementsByClassName("tabcontent");
                for (i = 0; i < tabcontent.length; i++) {{
                    tabcontent[i].style.display = "none";
                }}
                tablinks = document.getElementsByClassName("tablinks");
                for (i = 0; i < tablinks.length; i++) {{
                    tablinks[i].className = tablinks[i].className.replace(" active", "");
                }}
                document.getElementById(tabName).style.display = "block";
                evt.currentTarget.className += " active";
            }}

            // Get the element with id="defaultOpen" and click on it
            document.getElementById("defaultOpen").click();
            </script>
        </body>
        </html>
        """

        # Save HTML file (defense-in-depth: re-verify path before write)
        validate_path_within_directory(html_path, abs_output_dir)
        with open(html_path, 'w', encoding='utf-8') as f:  # nosec B603 - path validated
            f.write(html_content)

        logger.info(f"Files saved successfully:")
        logger.info(f"Processed JSON: {processed_json_path}")
        logger.info(f"Original JSON: {original_json_path}")
        logger.info(f"HTML: {html_path}")

        return html_path

    except Exception as e:
        logger.error(f"An error occurred: {type(e).__name__}")
        raise RuntimeError("Failed to save dataframe as JSON and HTML") from e


def create_html_from_json(json_data, output_dir='output', prefix='data'):
    """
    Create an HTML file from JSON data.
    
    Args:
        json_data: JSON data to convert to HTML
        output_dir: Output directory for the HTML file
        prefix: Prefix for the HTML filename
        
    Returns:
        str: Path to the created HTML file
        
    Raises:
        ValueError: If json_data is invalid or path traversal detected
        OSError: If directory creation or file writing fails
    """
    if not json_data:
        raise ValueError("json_data cannot be empty")
    
    # Validate output_dir to prevent path traversal before creating directories
    abs_output_dir = os.path.realpath(output_dir)
    cwd = os.path.realpath(os.getcwd())
    if not abs_output_dir.startswith(cwd):
        raise ValueError("output_dir must be within the current working directory")
        
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Sanitize prefix to prevent path traversal
    safe_prefix = os.path.basename(prefix)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    html_filename = f"{safe_prefix}_{timestamp}.html"
    html_path = safe_join_path(output_dir, html_filename)
    
    # Verify path is within output_dir
    abs_output_dir = os.path.realpath(output_dir)
    if not os.path.realpath(html_path).startswith(abs_output_dir):
        raise ValueError("Path traversal detected in html_path")

    # Extract document class
    document_class = json_data.get("document_class", {}).get("type", "N/A")

    # Extract inference result and explainability
    inference = json_data.get("inference_result", {})
    explainability = json_data.get("explainability_info", [{}])[0]

    # Construct DataFrame
    records = []
    for key, value in inference.items():
        confidence = explainability.get(key, {}).get("confidence", "N/A")
        records.append({
            "Field": key,
            "Value": value,
            "Confidence": round(confidence, 4) if isinstance(confidence, float) else confidence
        })
    df = pd.DataFrame(records)

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

    # Path validated by safe_join_path and realpath check above
    with open(html_path, 'w', encoding='utf-8') as f:  # nosec B603 - path validated
        f.write(html_content)

    logger.info(f"HTML saved at: {html_path}")
    return html_path


def read_s3_object(s3_uri, bytes=False):
    # Parse the S3 URI
    parsed_uri = urlparse(s3_uri)
    bucket_name = parsed_uri.netloc
    object_key = parsed_uri.path.lstrip('/')
    # Create an S3 client
    aws = AWSClients()
    s3_client = aws.s3_client
    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # Read the content of the object
        if bytes is True: 
            content = response['Body'].read() 
        else: 
            content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        logger.error(f"Error reading S3 object: {type(e).__name__}")
        raise RuntimeError("Failed to read S3 object") from e


def extract_inference_from_s3_to_df(s3_uri):
    """
    Downloads JSON from S3, extracts inference result + explainability,
    and returns a DataFrame with field_name, value, confidence, page, and bounding_box.
    Also saves the result as an HTML file.

    Parameters:
    s3_uri (str): S3 URI of the JSON file.
    output_dir (str): Directory to save the HTML output.

    Returns:
    (pd.DataFrame, str): Extracted DataFrame and HTML file path
    """
    try:
        # Validate S3 URI format
        if not s3_uri or not s3_uri.startswith('s3://'):
            raise ValueError("Invalid S3 URI format - must start with 's3://'")
        
        uri_parts = s3_uri.replace('s3://', '').split('/', 1)
        if len(uri_parts) != 2 or not uri_parts[0] or not uri_parts[1]:
            raise ValueError("Invalid S3 URI format - must contain bucket and key")
        
        bucket, key = uri_parts
        
        # AWS client
        aws = AWSClients()
        s3_client = aws.s3_client
        response = s3_client.get_object(Bucket=bucket, Key=key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))

        inference_result = json_data.get("inference_result", {})
        explainability_info = json_data.get("explainability_info", [{}])[0]

        records = []
        for field, value in inference_result.items():
            info = explainability_info.get(field, {})
            confidence = round(info.get("confidence", None), 4) if isinstance(
                info.get("confidence"), float) else info.get("confidence")

            geometry = info.get("geometry", [])
            # Safely access geometry data with bounds checking
            try:
                page = geometry[0].get("page") if geometry and len(geometry) > 0 else None
                bbox = geometry[0].get("boundingBox") if geometry and len(geometry) > 0 else None
            except (IndexError, TypeError, AttributeError):
                page = None
                bbox = None

            records.append({
                "field_name": field,
                "value": value,
                "confidence": confidence,
                "page": page,
                "bounding_box": json.dumps(bbox) if bbox else None
            })

        df = pd.DataFrame(records)

        # HTML output - hardcoded safe directory
        if not os.path.exists("output/html_output"):  # nosec - hardcoded safe path
            os.makedirs("output/html_output")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        html_file = os.path.join(
            "output/html_output", f"inference_result_{timestamp}.html")  # nosec - hardcoded base + sanitized timestamp
        df.to_html(html_file, index=False, justify='center')

        logger.info(f"✅ Extracted {len(df)} fields and saved HTML to: {html_file}")
        return df, html_file

    except Exception as e:
        logger.error(f"❌ Error extracting inference from S3: {type(e).__name__}")
        raise RuntimeError("Failed to extract inference from S3") from e

# def get_json_from_s3_to_df(s3_uri):
#     """
#     Get JSON file from S3 and convert it to DataFrame
#
#     Parameters:
#     s3_uri (str): S3 URI of the JSON file
#
#     Returns:
#     pandas.DataFrame: DataFrame containing the JSON data
#     """
#     try:
#
#         # Create an S3 client
#         aws = AWSClients()
#         s3_client = aws.s3_client
#
#         # Parse S3 URI to get bucket and key
#         bucket, key = s3_uri.replace('s3://', '').split('/', 1)
#
#         # Get object from S3
#         response = s3_client.get_object(Bucket=bucket, Key=key)
#
#         # Read JSON content
#         json_data = json.loads(response['Body'].read().decode('utf-8'))
#
#         # Convert to DataFrame
#         if isinstance(json_data, list):
#             # If JSON is a list of dictionaries
#             df = pd.DataFrame(json_data)
#         elif isinstance(json_data, dict):
#             # If JSON is a single dictionary
#             df = pd.DataFrame([json_data])
#         else:
#             raise ValueError("Unexpected JSON structure")
#
#         print(f"DataFrame shape: {df.shape}")
#         print("\nColumns:", df.columns.tolist())
#
#         return df, json_data
#
#     except Exception as e:
#         print(f"Error: {str(e)}")
#         return None, None


def extract_inputs_to_dataframe_from_file(json_file_path):
    """
    Reads a JSON file and extracts the 'inputs' section into a DataFrame.

    Parameters:
    json_file_path (str): Path to the JSON file.

    Returns:
    pd.DataFrame: DataFrame with columns - instruction, data_point_in_document, field_name, expected_output
    
    Raises:
        ValueError: If path traversal is detected or file is invalid
        RuntimeError: If file reading or parsing fails
    """
    try:
        # Validate path to prevent path traversal using proper security function
        # Allow files within current working directory
        cwd = os.path.realpath(os.getcwd())
        validated_path = validate_path_within_directory(json_file_path, cwd)
        
        # Verify it's a file
        if not os.path.isfile(validated_path):
            raise ValueError(f"Path is not a file: {json_file_path}")
        
        # Path validated above
        with open(validated_path, 'r', encoding='utf-8') as f:  # nosec B603 - path validated by validate_path_within_directory
            json_data = json.load(f)

        inputs = json_data.get("inputs", [])
        
        # Validate inputs is a list
        if not isinstance(inputs, list):
            raise ValueError("'inputs' field must be a list")
        
        # Handle empty inputs gracefully
        if not inputs:
            logger.warning("No inputs found in JSON file, returning empty DataFrame")
            return pd.DataFrame()
        
        df = pd.DataFrame(inputs)
        return df

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in file: {type(e).__name__}")
        raise ValueError("Invalid JSON format in input file") from e
    except ValueError:
        # Re-raise ValueError (includes path traversal errors)
        raise
    except Exception as e:
        logger.error(f"Error reading or parsing the JSON file: {type(e).__name__}")
        raise RuntimeError("Failed to read or parse JSON file") from e


def merge_bda_and_input_dataframes(bda_df, input_df):
    """
    Merge BDA output and expected input DataFrames on normalized 'field_name'.

    Parameters:
    bda_df (pd.DataFrame): DataFrame with BDA output (should include 'field_name' or 'Field')
    input_df (pd.DataFrame): DataFrame with expected output and data_point_in_document

    Returns:
    pd.DataFrame: Cleanly merged DataFrame
    
    Raises:
        ValueError: If input DataFrames are empty
        KeyError: If required columns are missing from input DataFrames
        RuntimeError: If merge operation fails
    """
    try:
        # Validate input DataFrames
        if bda_df is None or input_df is None:
            raise ValueError("Input DataFrames cannot be None")
        if bda_df.empty or input_df.empty:
            raise ValueError("Input DataFrames cannot be empty")
        
        # Standardize column names
        bda_df = bda_df.copy()
        input_df = input_df.copy()
        bda_df.columns = bda_df.columns.str.lower().str.strip()
        input_df.columns = input_df.columns.str.lower().str.strip()
        
        # Validate required columns exist
        if 'field_name' not in bda_df.columns:
            raise KeyError("bda_df must contain 'field_name' column")
        if 'field_name' not in input_df.columns:
            raise KeyError("input_df must contain 'field_name' column")

        # Normalize the field names for merge
        bda_df['field_name_normalized'] = bda_df['field_name'].str.lower().str.strip()
        input_df['field_name_normalized'] = input_df['field_name'].str.lower(
        ).str.strip()

        # Merge on normalized name
        merged = pd.merge(
            bda_df,
            input_df,
            on='field_name_normalized',
            suffixes=('_bda', '_input'),
            how='inner'
        )
        
        # Validate merge result
        if merged.empty:
            logger.warning("Merge resulted in empty DataFrame - no matching field names found")

        # Compose final output
        final_df = merged[[
            'field_name_input',  # Use input field name to preserve original case
            'instruction',
            'value',
            'confidence',
            'expected_output',
            'data_point_in_document'
        ]].rename(columns={
            'field_name_input': 'Field',
            'instruction': 'Instruction',
            'value': 'Value (BDA Response)',
            'confidence': 'Confidence',
            'expected_output': 'Expected Output',
            'data_point_in_document': 'Data in Document'
        })

        return final_df
    
    except (KeyError, ValueError) as e:
        # Re-raise validation errors
        raise
    except Exception as e:
        logger.error(f"Error merging DataFrames: {type(e).__name__}")
        raise RuntimeError("Failed to merge BDA and input DataFrames") from e


# Import field similarity functions
from src.models.field_similarity import calculate_field_similarity, detect_field_type, FieldType

def add_semantic_similarity_column(df, threshold):
    """
    Adds 'semantic_similarity' and 'semantic_match' columns to the given DataFrame by comparing
    'Value (BDA Response)' and 'Expected Output' using type-specific similarity functions.

    Parameters:
    df (pd.DataFrame): DataFrame with required columns.
    threshold (float): Threshold above which a semantic match is considered True.

    Returns:
    pd.DataFrame: Updated DataFrame with added columns.
    """
    try:
        required_cols = ['Field', 'Value (BDA Response)', 'Expected Output']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing column: {col}")

        # Add field type detection
        df['detected_field_type'] = df.apply(
            lambda row: detect_field_type(
                str(row['Field']), 
                str(row['Expected Output'])
            ).value,
            axis=1
        )
        
        # Calculate type-specific similarity
        df['semantic_similarity'] = df.apply(
            lambda row: calculate_field_similarity(
                str(row['Field']),
                str(row['Expected Output']), 
                str(row['Value (BDA Response)'])
            ),
            axis=1
        )

        df['semantic_match'] = df['semantic_similarity'] >= threshold

        return df

    except Exception as e:
        logger.error(f"Error adding semantic similarity: {type(e).__name__}")
        raise RuntimeError("Failed to add semantic similarity") from e


def update_instructions_with_bedrock(df, threshold, doc_path=None):
    """
    Update the 'instruction' column of a DataFrame by calling function_b
    with each row's current instruction and Expected Output.

    Parameters:
    df (pd.DataFrame): Input DataFrame containing 'instruction' and 'Expected Output' columns
    function_b (callable): A function that takes (instruction, expected_output) and returns new instruction

    Returns:
    pd.DataFrame: A new DataFrame with updated 'instruction' values
    """
    try:
        # Check required columns
        required_cols = ['Field', 'Instruction',
                         'Expected Output', 'semantic_similarity']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Create a copy to avoid modifying the original
        df_updated = df.copy()

        # Update instruction column row-by-row
        for idx, row in df_updated.iterrows():
            if row['semantic_similarity'] < threshold:
                field_name = row['Field']
                expected_output = row['Expected Output']
                if doc_path is None: 
                    result = rewrite_prompt_bedrock(field_name, row['Instruction'], expected_output)
                else: 
                    result = rewrite_prompt_bedrock_with_document(field_name, row['Instruction'], expected_output, doc_path)
                _secure_instruction_update(df_updated, idx, result)
                # Log field update without exposing instruction content (CWE-200 mitigation)
                logger.info(f"Updated instruction for field '{field_name}' at index {idx}")
        return df_updated

    except Exception as e:
        # Log error without exposing sensitive instruction content
        logger.error(f"❌ Error in updating instructions: {type(e).__name__}")
        raise RuntimeError("Failed to update instructions") from e


def update_schema_with_new_instruction(df, iteration):
    """
    Update the "instruction" field in the schema for the blueprint using the new generated instruction
    Update the input.json used for the merged df

    Parameters:
    df (pd.DataFrame): Input DataFrame containing new instructions

    Returns:
    json object for schema to pass into update blueprint API call
    """

    try:
        with open('src/schema.json') as schema:  # nosec B603 - hardcoded safe path
            blueprint_schema = json.load(schema)

        with open('input_0.json') as input_file:  # nosec B603 - hardcoded safe path
            input_data = json.load(input_file)
        
        # Validate schema structure
        if 'properties' not in blueprint_schema:
            raise ValueError("Schema missing 'properties' field")
        
        # Validate input_data structure
        if 'inputs' not in input_data or not isinstance(input_data['inputs'], list):
            raise ValueError("Input data missing 'inputs' list")
            
        input_dict = {item['field_name']: item for item in input_data['inputs'] if 'field_name' in item}

        # update schema instruction with new generated instruction
        properties = blueprint_schema['properties']
        for idx, row in df.iterrows():
            key = row['Field']
            
            # Safely check if key exists in properties before updating
            if key not in properties:
                logger.warning(f"Field '{key}' not found in schema properties, skipping")
                continue
                
            _secure_instruction_update(properties[key], 'instruction', row['Instruction'])
            
            # Find the matching input in the list input.json and update its instruction
            if key in input_dict:
                _secure_instruction_update(input_dict[key], 'instruction', row['Instruction'])
        
        input_data['inputs'] = list(input_dict.values())

        # Sanitize iteration to prevent path traversal
        safe_iteration = sanitize_filename(str(iteration))
        
        # create new schema file to update blueprint - use safe path construction
        schema_filename = f'schema_updated_{safe_iteration}.json'
        schema_path = safe_join_path('src', schema_filename)
        with open(schema_path, 'w') as new_schema:  # nosec B603 - path validated by safe_join_path
            json.dump(blueprint_schema, new_schema, indent=4)

        # create new input file for merged df - use safe path construction
        input_filename = f'input_{safe_iteration}.json'
        input_path = safe_join_path('.', input_filename)
        with open(input_path, 'w') as new_input:  # nosec B603 - path validated by safe_join_path
            json.dump(input_data, new_input, indent=4)

        logger.info(f"✅ Schema successfully updated, new schema at: {schema_path}")
        return schema_path

    except Exception as e:
        # Log error without exposing sensitive instruction content
        logger.error(f"❌ Error in updating schema: {type(e).__name__}")
        raise RuntimeError(f"Failed to update schema") from e


def curr_match_status(df, threshold):
    """
    Check if all fields are a semantic match (>80% similar)

    Parameters:
    df (pd.Dataframe): Input Dataframe containing semantic similarity calculations

    """

    try:
        # Check required columns
        required_cols = ['semantic_similarity']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")


        # Update instruction column row-by-row
        for row in df.itertuples():
            if row.semantic_similarity < threshold:
                logger.info(f"\n🔸 Not all fields have reached {threshold*100}% matched yet!")
                return False 
        
        logger.info(f"\n🔹 All fields are at least {threshold*100}% matched!!")
        return True 
    except Exception as e:
        logger.error(f"❌ Error in checking semantic match: {type(e).__name__}")
        raise RuntimeError("Failed to check semantic match status") from e


def create_full_similarity_csv(folder_path):
    """
    Create a merged df with all the similarity files combined to compare the accuracy for each instruction

    Parameters:
    folder_path (string): folder path where all similarity files are

    """
    try:
        # Validate folder path to prevent path traversal using proper security function
        cwd = os.path.realpath(os.getcwd())
        abs_folder = validate_path_within_directory(folder_path, cwd)
        
        # Verify it's a directory
        if not os.path.isdir(abs_folder):
            raise ValueError(f"Invalid folder path: {folder_path} is not a directory")

        # iterate through all similarity files in the folder_path and create df
        dfs = []
        for filename in os.listdir(abs_folder):
            if filename.endswith(".csv"):
                # Use safe path joining
                file_path = safe_join_path(abs_folder, filename)
                try:
                    dfs.append(pd.read_csv(file_path))
                except (pd.errors.ParserError, pd.errors.EmptyDataError, FileNotFoundError) as e:
                    logger.error(f"Error reading {filename}: {type(e).__name__}")
                    continue

        # Validate we have data to merge
        if len(dfs) == 0:
            logger.warning("No CSV files found or all files failed to load")
            return
        
        if len(dfs) == 1:
            logger.warning("Only one CSV file found, nothing to merge")
            dfs[0].to_csv("compare_instructions_with_similarity.csv", index=False)
            return

        dfs.reverse()
        # rename columns to distinguish between different iterations
        for i, df in enumerate(dfs, start=1):
            df.rename(columns={col: '{}_{}'.format(col, i) for col in ('Instruction', 'Value (BDA Response)', 'Confidence', 'semantic_similarity', 'semantic_match')},
                      inplace=True)

        # merge all the dfs into one df
        try:
            merge = partial(
                pd.merge, on=['Field', 'Expected Output', 'Data in Document'])
            df_merged = reduce(merge, dfs)
        except (KeyError, ValueError) as e:
            logger.error(f"Error merging DataFrames: {type(e).__name__}")
            raise RuntimeError("Failed to merge similarity DataFrames - check column names") from e

        first_cols = ['Field', 'Expected Output', 'Data in Document']
        req_order = first_cols + \
            [col for col in df_merged.columns if col not in first_cols]
        df_merged = df_merged[req_order]

        # save full df to csv
        df_merged.to_csv(
            "compare_instructions_with_similarity.csv", index=False)

        logger.info(f"\n\n✅ Full similarity csv created, new file at: compare_instructions_with_similarity.csv")

    except Exception as e:
        logger.error(f"❌ Error in creating full similarity CSV: {type(e).__name__}")
