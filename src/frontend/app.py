"""
FastAPI application for BDA optimizer web interface.
"""
from fastapi import FastAPI, Request, Form, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, StreamingResponse, RedirectResponse
import asyncio
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import html
import json
import os
import sys
import boto3
import botocore.exceptions
import uuid
import logging
from datetime import datetime
import shlex

from src.path_security import validate_path_within_directory

# Configure logging
logger = logging.getLogger(__name__)


def sanitize_config_for_template(config: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitize config values to prevent XSS when rendered in templates."""
    def sanitize_value(value):
        if isinstance(value, str):
            return html.escape(value)
        elif isinstance(value, dict):
            return {k: sanitize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [sanitize_value(item) for item in value]
        return value
    
    return sanitize_value(config)


# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app_sequential_pydantic import main as run_optimizer

# Initialize FastAPI app
app = FastAPI(title="BDA Optimizer UI")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Get the base directory for the application
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mount templates and static files with restricted paths
templates_dir = os.path.join(BASE_DIR, "src", "frontend", "templates")
static_dir = os.path.join(BASE_DIR, "src", "frontend", "static")
react_build_dir = os.path.join(BASE_DIR, "src", "frontend", "react", "dist")

# Ensure directories exist and are within the project
if not os.path.exists(templates_dir) or not templates_dir.startswith(BASE_DIR):
    raise ValueError(f"Templates directory not found or outside project: {templates_dir}")

templates = Jinja2Templates(directory=templates_dir)

# Mount static files only if directory exists and is within project
if os.path.exists(static_dir) and static_dir.startswith(BASE_DIR):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Mount React build (when available) with path validation
if os.path.exists(react_build_dir) and react_build_dir.startswith(BASE_DIR):
    app.mount("/react", StaticFiles(directory=react_build_dir, html=True), name="react")

# Ensure static directory exists within project bounds
os.makedirs(static_dir, exist_ok=True)

# Test endpoint for CORS
@app.get("/api/test")
async def test_cors():
    return {"message": "CORS is working"}



# Pydantic models matching input_0.json structure
class Instruction(BaseModel):
    instruction: str
    field_name: str
    expected_output: str
    data_point_in_document: bool = True
    inference_type: str = "explicit"

class OptimizerConfig(BaseModel):
    project_arn: str
    blueprint_id: str
    document_name: str
    dataAutomation_profilearn: str
    project_stage: str
    input_document: str
    bda_s3_output_location: str
    inputs: List[Instruction]

@app.get("/")
async def home(request: Request):
    """Redirect to React app if available, otherwise serve original UI."""
    if os.path.exists(react_build_dir) and react_build_dir.startswith(BASE_DIR):
        return RedirectResponse(url="/react")
    return await legacy_home(request)

@app.get("/legacy")
async def legacy_home(request: Request):
    """Render the home page with the current configuration."""
    try:
        # Always load input_0.json from project root
        config_path = os.path.join(BASE_DIR, "input_0.json")
        if not config_path.startswith(BASE_DIR):
            raise ValueError("Configuration file path outside project bounds")
            
        with open(config_path, "r") as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - config_path validated above
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.warning(f"Could not load config: {type(e).__name__}")
        # If input_0.json can't be loaded, return an empty config
        empty_config = {
            "project_arn": "",
            "blueprint_id": "",
            "document_name": "",
            "dataAutomation_profilearn": "",
            "project_stage": "LIVE",
            "input_document": "",
            "bda_s3_output_location": "",
            "inputs": []
        }
        return templates.TemplateResponse(  # nosec # nosemgrep: python.flask.security.xss.audit.direct-use-of-jinja2 - Jinja2 auto-escapes by default
            "index.html",
            {"request": request, "config": empty_config}
        )
    except Exception as e:
        logger.error(f"Unexpected error loading config: {type(e).__name__}")
        raise HTTPException(status_code=500, detail="Failed to load configuration")
    
    try:
        # Sanitize config values to prevent XSS
        safe_config = sanitize_config_for_template(config)
        
        return templates.TemplateResponse(  # nosec # nosemgrep: python.flask.security.xss.audit.direct-use-of-jinja2 - Jinja2 auto-escapes, config sanitized
            "index.html",
            {"request": request, "config": safe_config}
        )
    except Exception as e:
        logger.error(f"Error sanitizing config: {type(e).__name__}")
        raise HTTPException(status_code=500, detail="Failed to process configuration")

@app.post("/api/update-config")
@app.post("/update-config")
async def update_config(config: OptimizerConfig):
    """Update the input_0.json file with new configuration."""
    try:
        config_path = os.path.join(BASE_DIR, "input_0.json")
        if not config_path.startswith(BASE_DIR):
            raise ValueError("Configuration file path outside project bounds")
            
        with open(config_path, "w") as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - config_path validated above
            json.dump(config.dict(), f, indent=2)
        return {"status": "success", "message": "Configuration updated successfully"}
    except Exception as e:
        logger.error(f"Failed to update config: {type(e).__name__}")
        raise HTTPException(status_code=500, detail="Failed to update configuration")

class OptimizerSettings(BaseModel):
    threshold: float = 0.6
    maxIterations: int = 2
    model: str = "anthropic.claude-3-sonnet-20240229-v1:0"
    useDoc: bool = True
    clean: bool = True

# Global variable to store the optimizer process
optimizer_process = None

@app.post("/api/clean-logs")
@app.post("/clean-logs")
async def clean_logs():
    """Clean all log files."""
    try:
        import shutil
        
        # Get logs directory with validation
        log_dir = os.path.join(BASE_DIR, "logs")
        if not log_dir.startswith(BASE_DIR):
            raise ValueError("Log directory path outside project bounds")
        
        # Check if directory exists
        if os.path.exists(log_dir):
            # Remove all files in the directory
            for file in os.listdir(log_dir):
                file_path = os.path.join(log_dir, file)
                if os.path.isfile(file_path) and file_path.startswith(log_dir):
                    os.unlink(file_path)
        
        return {"status": "success", "message": "All logs cleaned successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/run-optimizer")
@app.post("/run-optimizer")
async def run_optimization(settings: OptimizerSettings):
    """Run the optimizer with the current configuration and settings."""
    global optimizer_process
    
    try:
        import subprocess
        import time
        import threading
        
        # Clean logs if requested
        if settings.clean:
            # Clean all log files with path validation
            log_dir = os.path.join(BASE_DIR, "logs")
            if not log_dir.startswith(BASE_DIR):
                raise ValueError("Log directory path outside project bounds")
                
            if os.path.exists(log_dir):
                for file in os.listdir(log_dir):
                    file_path = os.path.join(log_dir, file)
                    if os.path.isfile(file_path) and file_path.startswith(log_dir):
                        os.unlink(file_path)
        
        # Create logs directory if it doesn't exist
        log_dir = os.path.join(BASE_DIR, "logs")
        if not log_dir.startswith(BASE_DIR):
            raise ValueError("Log directory path outside project bounds")
        os.makedirs(log_dir, exist_ok=True)
        
        # Create a log file with timestamp
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        log_file_path = os.path.join(log_dir, f"optimizer-{timestamp}.log")
        log_file_name = f"optimizer-{timestamp}.log"
        
        # Write initial content to log file
        with open(log_file_path, "w") as log_file:
            log_file.write(f"Optimizer run at {timestamp}\n")
            log_file.write(f"Model: {settings.model}\n")
            log_file.write(f"Threshold: {settings.threshold}\n")
            log_file.write(f"Max iterations: {settings.maxIterations}\n")
            log_file.write(f"Use document strategy: {settings.useDoc}\n")
            log_file.write(f"Clean previous runs: {settings.clean}\n\n")
            log_file.write("Starting optimizer process...\n")
            log_file.flush()
        
        # Validate and sanitize input parameters
        # Validate threshold (should be float between 0 and 1)
        try:
            threshold_val = float(settings.threshold)
            if not (0.0 <= threshold_val <= 1.0):
                raise ValueError("Threshold must be between 0.0 and 1.0")
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Invalid threshold value")
        
        # Validate max iterations (should be positive integer)
        try:
            max_iter_val = int(settings.maxIterations)
            if max_iter_val <= 0 or max_iter_val > 100:  # Reasonable upper limit
                raise ValueError("Max iterations must be between 1 and 100")
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Invalid max iterations value")
        
        # Validate model name (whitelist approach for security)
        allowed_models = [
            "anthropic.claude-3-sonnet-20240229-v1:0",
            "anthropic.claude-3-5-sonnet-20241022-v2:0",
            "anthropic.claude-3-haiku-20240307-v1:0",
            "anthropic.claude-3-opus-20240229-v1:0",
            "anthropic.claude-3-7-sonnet-20250219-v1:0",
            "anthropic.claude-3-5-haiku-20241022-v1:0",
            "anthropic.claude-opus-4-20250514-v1:0",
            "anthropic.claude-sonnet-4-20250514-v1:0",
            "amazon.titan-text-premier-v1:0",
            "amazon.nova-pro-v1:0",
            "amazon.nova-lite-v1:0",
            "amazon.nova-micro-v1:0"
        ]
        if settings.model not in allowed_models:
            raise HTTPException(status_code=400, detail=f"Invalid model selection: {settings.model}")
        
        # Safely quote all parameters
        _threshold = shlex.quote(str(threshold_val))
        _model = shlex.quote(settings.model)
        _maxIterations = shlex.quote(str(max_iter_val))
        
        # Build command with settings from request
        _useDoc = ""
        if settings.useDoc:
            _useDoc = "--use-doc"
        
        _clean = ""
        if settings.clean:
            _clean = "--clean"
        
        # Define a function to run the optimizer in a separate thread
        def run_optimizer_process():
            nonlocal log_file_path
            with open(log_file_path, "a") as log_file:
                global optimizer_process
                try:
                    # Build command arguments list with sanitized parameters
                    cmd_args = [
                        "./run_sequential_pydantic.sh",
                        "--threshold", _threshold,
                        "--model", _model,
                        "--max-iterations", _maxIterations
                    ]
                    
                    # Add optional arguments only if they're not empty
                    if _useDoc:
                        cmd_args.append(_useDoc)
                    if _clean:
                        cmd_args.append(_clean)
                    
                    # Log the actual command being executed
                    log_file.write(f"Executing command: {' '.join(cmd_args)}\n")
                    log_file.flush()
                    
                    # Execute the command with output redirected to the log file
                    # Security: All inputs are validated above (threshold: float 0-1, 
                    # maxIterations: int 1-100, model: whitelist only) and sanitized 
                    # with shlex.quote(). Using list args (no shell=True).
                    # nosemgrep: python.lang.security.audit.dangerous-subprocess-use
                    optimizer_process = subprocess.Popen(  # nosec B603 B607
                        cmd_args,  # Validated and sanitized command arguments
                        stdout=log_file,
                        stderr=log_file,
                        cwd=BASE_DIR,  # Use validated base directory
                        shell=False  # Explicitly disable shell execution - prevents command injection
                    )
                    
                    # Write the process ID to the log file for debugging
                    log_file.write(f"\nOptimizer process started with PID: {optimizer_process.pid}\n")
                    log_file.flush()
                    
                    # Wait for process to complete
                    optimizer_process.wait()
                    
                    # Write completion message
                    log_file.write("\nOptimizer process completed.\n")
                    
                    # Ensure all child processes are terminated
                    try:
                        import psutil
                        parent = psutil.Process(optimizer_process.pid)
                        children = parent.children(recursive=True)
                        
                        # Terminate children
                        for child in children:
                            try:
                                child.kill()
                                logger.debug(f"Killed child process {child.pid}")
                            except psutil.NoSuchProcess:
                                pass
                        
                        # Also try to kill any related processes using pkill
                        try:
                            # Use the subprocess module that's already imported at the top level
                            result = subprocess.run(["pkill", "-f", "app_sequential_pydantic.py"], check=False)  # nosec B603 # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
                            result = subprocess.run(["pkill", "-f", "run_sequential_pydantic.sh"], check=False)  # nosec B603 # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
                            logger.debug("Killed any remaining optimizer processes using pkill")
                        except Exception as e:
                            logger.warning(f"Error killing processes with pkill: {type(e).__name__}")
                    except Exception as e:
                        log_file.write(f"\nError cleaning up processes: {str(e)}\n")
                    
                except Exception as e:
                    # Log any errors
                    log_file.write(f"\nError in optimizer process: {str(e)}\n")
                finally:
                    # Reset the process reference
                    optimizer_process = None
        
        # Start the optimizer in a separate thread
        optimizer_thread = threading.Thread(target=run_optimizer_process)
        optimizer_thread.daemon = True
        optimizer_thread.start()
        
        # Return immediately with the log file path
        # Also include the timestamp for easier matching
        return {
            "status": "running", 
            "message": "Optimization started",
            "log_file": log_file_name,
            "timestamp": timestamp
        }
    except Exception as e:
        # Reset the process reference on error
        optimizer_process = None
        import traceback
        logger.error(f"Optimizer failed: {type(e).__name__}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")

@app.get("/api/optimizer-status")
@app.get("/optimizer-status")
async def optimizer_status():
    """Check if the optimizer process is still running."""
    global optimizer_process
    
    try:
        # If optimizer_process is None, it's not running
        if optimizer_process is None:
            return {"status": "not_running"}
        
        # Check if the process is still running
        if optimizer_process.poll() is None:
            # Process is still running
            return {"status": "running"}
        else:
            # Process has completed
            return {"status": "completed", "return_code": optimizer_process.returncode}
    except Exception as e:
        logger.warning(f"Error checking optimizer status: {type(e).__name__}")
        # If there's an error, assume it's not running
        return {"status": "not_running"}

@app.post("/api/stop-optimizer")
@app.post("/stop-optimizer")
async def stop_optimization():
    """Stop the running optimizer process."""
    global optimizer_process
    
    try:
        import signal
        import psutil
        import os
        import subprocess
        
        # Use pkill to kill all processes related to the optimizer
        # This is more robust than trying to find and kill processes individually
        try:
            # Kill all processes with app_sequential_pydantic.py in the command line
            subprocess.run(["pkill", "-f", "app_sequential_pydantic.py"], check=False)  # nosec B603 # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
            # Kill all processes with run_sequential_pydantic.sh in the command line
            subprocess.run(["pkill", "-f", "run_sequential_pydantic.sh"], check=False)  # nosec B603 # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
            logger.debug("Killed optimizer processes using pkill")
        except Exception as e:
            logger.warning(f"Error using pkill: {type(e).__name__}")
        
        # Also try the ps approach as a fallback
        try:
            # Find all processes with the name "python" or "python3"
            # This will help us find all related Python processes
            result = subprocess.run(  # nosec B603 # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
                ["ps", "-ef"],
                capture_output=True,
                text=True
            )
            
            # Look for python processes that might be running the optimizer
            for line in result.stdout.splitlines():
                if "app_sequential_pydantic.py" in line or "run_sequential_pydantic.sh" in line:
                    try:
                        # Extract PID from the ps output
                        parts = line.split()
                        if len(parts) > 1:
                            process_pid = int(parts[1])
                            # Kill the process
                            os.kill(process_pid, signal.SIGKILL)
                            logger.debug(f"Killed process {process_pid}")
                    except Exception as e:
                        logger.debug(f"Error killing process: {type(e).__name__}")
        except Exception as e:
            logger.warning(f"Error using ps approach: {type(e).__name__}")
        
        # If optimizer_process is not None, try to kill it directly
        if optimizer_process:
            try:
                # Get the process and all its children
                parent = psutil.Process(optimizer_process.pid)
                children = parent.children(recursive=True)
                
                # Terminate children first
                for child in children:
                    try:
                        child.kill()  # Use kill instead of terminate for more forceful termination
                    except psutil.NoSuchProcess:
                        pass
                
                # Kill the main process
                optimizer_process.kill()  # Use kill instead of terminate
                
                # Wait for process to actually terminate
                try:
                    optimizer_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
                
                # If process is still running, use SIGKILL
                if optimizer_process.poll() is None:
                    os.kill(optimizer_process.pid, signal.SIGKILL)
            except Exception as e:
                logger.warning(f"Error killing optimizer_process: {type(e).__name__}")
        
        # Reset the process reference
        optimizer_process = None
        
        # Add a message to the current log file if it exists
        log_dir = os.path.join(BASE_DIR, "logs")
        if not log_dir.startswith(BASE_DIR):
            raise ValueError("Log directory path outside project bounds")
            
        if os.path.exists(log_dir):
            log_files = [f for f in os.listdir(log_dir) if 
                        (f.startswith("optimizer-") or f.startswith("bda_optimizer_")) and 
                        f.endswith(".log")]
            if log_files:
                log_files.sort(reverse=True)  # Most recent first
                latest_log = os.path.join(log_dir, log_files[0])
                if latest_log.startswith(log_dir):  # Additional validation
                    with open(latest_log, "a") as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - latest_log validated above
                        f.write("\n\nOptimizer process was manually stopped by user.\n")
        
        return {"status": "success", "message": "Optimizer processes stopped successfully"}
    except Exception as e:
        logger.error(f"Error in stop_optimization: {type(e).__name__}")
        return {"status": "error", "message": "Error stopping optimizer"}

@app.get("/api/view-log/{log_file}")
@app.get("/view-log/{log_file}")
async def view_log(log_file: str):
    """View a log file."""
    try:
        # Validate log file name to prevent directory traversal
        if ".." in log_file or "/" in log_file or "\\" in log_file:
            raise HTTPException(status_code=400, detail="Invalid log file name")
            
        log_dir = os.path.join(BASE_DIR, "logs")
        if not log_dir.startswith(BASE_DIR):
            raise ValueError("Log directory path outside project bounds")
            
        log_path = os.path.join(log_dir, log_file)
        
        # Ensure the resolved path is still within the log directory
        if not log_path.startswith(log_dir):
            raise HTTPException(status_code=400, detail="Invalid log file path")
        
        # Debug logging
        logger.debug(f"Requested log file: {log_file}")
        logger.debug(f"Full log path: {log_path}")
        
        # List available log files
        if os.path.exists(log_dir):
            available_logs = [f for f in os.listdir(log_dir) if f.endswith(".log")]
        else:
            available_logs = []
        
        # If the exact file doesn't exist, try to find a similar one
        if not os.path.exists(log_path) or not os.path.isfile(log_path):
            # Try to find a log file with a similar timestamp
            similar_logs = [f for f in available_logs if f.startswith(log_file[:15])]
            if similar_logs:
                # Use the first similar log file
                log_file = similar_logs[0]
                log_path = os.path.join(log_dir, log_file)
                # Re-validate the new path
                if not log_path.startswith(log_dir):
                    raise HTTPException(status_code=400, detail="Invalid similar log file path")
                logger.debug(f"Using similar log file instead: {log_file}")
            else:
                # If no similar log file is found, return a 404 error
                raise HTTPException(status_code=404, detail=f"Log file not found. Available logs: {available_logs}")
        
        # Read the log file
        with open(log_path, "r") as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - log_path validated above
            content = f.read()
        
        return {"content": content}
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error in view_log: {type(e).__name__}")
        # Return a more detailed error message
        raise HTTPException(
            status_code=500, 
            detail="Error reading log file. Please check if the file exists and is readable."
        )

class DocumentUploadRequest(BaseModel):
    bucket_name: str
    s3_prefix: Optional[str] = ""

@app.post("/api/upload-document")
async def upload_document(  # nosemgrep: python.flask.security.dangerous-file-upload
    # File upload with extension validation, size limits, and path traversal checks below
    file: UploadFile = File(...),
    bucket_name: str = Form(...),
    s3_prefix: str = Form("")
):
    """Upload a document to S3 and return the S3 URI."""
    try:
        # Validate file
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file selected")
        
        # Security: Validate file extension to prevent dangerous file uploads (CWE-434)
        # NOTE: Currently only PDF format is supported for document processing.
        # This is intentional - do not expand to other formats without updating
        # the document processing pipeline (BDA, prompt_tuner, etc.)
        ALLOWED_EXTENSIONS = {
            '.pdf'
        }
        
        # Security: Validate content type matches allowed MIME types
        # NOTE: PDF-only support is intentional (see ALLOWED_EXTENSIONS comment above)
        ALLOWED_CONTENT_TYPES = {
            'application/pdf'
        }
        
        file_extension = os.path.splitext(file.filename)[1].lower()
        if file_extension not in ALLOWED_EXTENSIONS:
            logger.warning(f"Rejected file upload with disallowed extension: {file_extension}")
            raise HTTPException(
                status_code=400, 
                detail=f"File type '{file_extension}' is not allowed. Allowed types: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
            )
        
        # Validate content type if provided
        if file.content_type and file.content_type not in ALLOWED_CONTENT_TYPES:
            logger.warning(f"Rejected file upload with disallowed content type: {file.content_type}")  # nosec # nosemgrep: python.lang.security.audit.logging - content_type is from request metadata
            raise HTTPException(
                status_code=400,
                detail=f"Content type '{file.content_type}' is not allowed"
            )
        
        # Security: Validate filename doesn't contain path traversal characters
        if '..' in file.filename or '/' in file.filename or '\\' in file.filename:
            logger.warning(f"Rejected file upload with suspicious filename: {file.filename[:50]}")  # nosec # nosemgrep: python.lang.security.audit.logging - truncated filename for safety
            raise HTTPException(status_code=400, detail="Invalid filename")
        
        # Validate file size (max 100MB) - nosec B108 # nosemgrep: python.lang.security.audit.dangerous-file-upload - extension, content-type, size all validated
        max_size = 100 * 1024 * 1024  # 100MB
        file_content = await file.read()  # nosemgrep: python.lang.security.audit.dangerous-file-upload - file validated above
        if len(file_content) > max_size:
            raise HTTPException(status_code=400, detail="File size exceeds 100MB limit")
        
        # Reset file pointer
        await file.seek(0)
        
        # Generate unique filename to avoid conflicts
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        s3_key = f"{s3_prefix.rstrip('/')}/{timestamp}_{unique_id}_{file.filename}" if s3_prefix else f"{timestamp}_{unique_id}_{file.filename}"
        
        # Initialize S3 client
        try:
            s3_client = boto3.client('s3')
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to initialize S3 client: {str(e)}")
        
        # Check if bucket exists and is accessible
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Cannot access bucket '{bucket_name}': {str(e)}")
        
        # Upload file to S3
        # Security: File extension validated above (ALLOWED_EXTENSIONS), filename sanitized, size limited
        try:
            s3_client.upload_fileobj(  # nosec B106 - extension/size/filename validated above
                file.file,
                bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': file.content_type or 'application/octet-stream',
                    'Metadata': {
                        'original_filename': file.filename,
                        'upload_timestamp': timestamp,
                        'uploaded_by': 'bda-optimizer'
                    }
                }
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to upload file to S3: {str(e)}")
        
        # Generate S3 URI
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        
        return {
            "status": "success",
            "message": "File uploaded successfully",
            "s3_uri": s3_uri,
            "bucket_name": bucket_name,
            "s3_key": s3_key,
            "file_size": len(file_content),
            "content_type": file.content_type
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {type(e).__name__}")

@app.get("/api/list-s3-buckets")
async def list_s3_buckets():
    """List available S3 buckets for the current AWS account."""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_buckets()
        
        buckets = []
        for bucket in response.get('Buckets', []):
            try:
                # Try to get bucket location
                location_response = s3_client.get_bucket_location(Bucket=bucket.get('Name', ''))
                region = location_response.get('LocationConstraint') or 'us-east-1'
                
                bucket_name = bucket.get('Name', '')
                creation_date = bucket.get('CreationDate')
                buckets.append({
                    'name': bucket_name,
                    'creation_date': creation_date.isoformat() if creation_date else '',
                    'region': region
                })
            except Exception as e:
                # If we can't get bucket details, still include it but with limited info
                bucket_name = bucket.get('Name', '')
                creation_date = bucket.get('CreationDate')
                buckets.append({
                    'name': bucket_name,
                    'creation_date': creation_date.isoformat() if creation_date else '',
                    'region': 'unknown',
                    'error': str(e)
                })
        
        return {
            "status": "success",
            "buckets": buckets
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list S3 buckets: {str(e)}")

@app.post("/api/validate-s3-access")
async def validate_s3_access(request: DocumentUploadRequest):
    """Validate S3 bucket access and permissions."""
    try:
        s3_client = boto3.client('s3')
        
        # Check if bucket exists and is accessible
        try:
            s3_client.head_bucket(Bucket=request.bucket_name)
        except Exception as e:
            return {
                "status": "error",
                "message": f"Cannot access bucket '{request.bucket_name}': {str(e)}",
                "has_read_access": False,
                "has_write_access": False
            }
        
        # Test read access
        has_read_access = False
        try:
            s3_client.list_objects_v2(Bucket=request.bucket_name, MaxKeys=1)
            has_read_access = True
        except botocore.exceptions.ClientError:
            pass
        
        # Test write access by attempting to put a small test object
        has_write_access = False
        test_key = f"{request.s3_prefix.rstrip('/')}/bda-optimizer-test-{uuid.uuid4()}" if request.s3_prefix else f"bda-optimizer-test-{uuid.uuid4()}"
        try:
            s3_client.put_object(
                Bucket=request.bucket_name,
                Key=test_key,
                Body=b"test",
                Metadata={'test': 'true'}
            )
            # Clean up test object
            s3_client.delete_object(Bucket=request.bucket_name, Key=test_key)
            has_write_access = True
        except botocore.exceptions.ClientError:
            pass
        
        return {
            "status": "success",
            "bucket_name": request.bucket_name,
            "has_read_access": has_read_access,
            "has_write_access": has_write_access,
            "message": "Bucket access validated"
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to validate S3 access: {str(e)}",
            "has_read_access": False,
            "has_write_access": False
        }

class BlueprintRequest(BaseModel):
    project_arn: str
    blueprint_id: str
    project_stage: str = "LIVE"

@app.post("/api/test-blueprint")
async def test_blueprint(request: BlueprintRequest):
    """Test endpoint to verify React-FastAPI communication without AWS calls"""
    return {
        "status": "success",
        "blueprint_name": "Test Blueprint",
        "output_path": "/test/path",
        "properties": [
            {
                "field_name": "test_field",
                "instruction": "Test instruction",
                "expected_output": "",
                "inference_type": "explicit"
            }
        ]
    }

@app.post("/api/fetch-blueprint")
@app.post("/fetch-blueprint")
async def fetch_blueprint(request: BlueprintRequest):
    """Fetch a blueprint from AWS BDA and extract its properties.
    
    Security: This endpoint processes JSON blueprint schemas from AWS BDA.
    Input validation and audit logging are implemented to prevent:
    - Oversized payloads (DoS prevention)
    - Malformed input processing
    - Unauthorized access attempts (logged for audit)
    """
    # Security: Audit log for blueprint fetch operations
    logger.info(f"Blueprint fetch request - blueprint_id: {request.blueprint_id[:100] if request.blueprint_id else 'None'}, "
                f"project_arn: {request.project_arn[:150] if request.project_arn else 'None'}")
    
    # Security: Input validation - limit input sizes to prevent DoS
    MAX_BLUEPRINT_ID_LENGTH = 500
    MAX_PROJECT_ARN_LENGTH = 500
    
    if request.blueprint_id and len(request.blueprint_id) > MAX_BLUEPRINT_ID_LENGTH:
        logger.warning(f"Blueprint ID exceeds maximum length: {len(request.blueprint_id)}")
        raise HTTPException(status_code=400, detail="Blueprint ID exceeds maximum allowed length")
    
    if request.project_arn and len(request.project_arn) > MAX_PROJECT_ARN_LENGTH:
        logger.warning(f"Project ARN exceeds maximum length: {len(request.project_arn)}")
        raise HTTPException(status_code=400, detail="Project ARN exceeds maximum allowed length")
    
    try:
        logger.debug(f"Fetching blueprint: {request.blueprint_id[:50] if request.blueprint_id else 'None'}")
        
        from src.aws_clients import AWSClients
        import json
        
        # Initialize AWS clients
        logger.debug("Initializing AWS clients...")
        aws_clients = AWSClients()
        logger.debug("AWS clients initialized successfully")
        
        # Download the blueprint directly by ARN
        logger.debug("Downloading blueprint...")
        # Try to download blueprint - first by ID, then by constructing ARN
        output_path = None
        blueprint_details = None
        
        if request.blueprint_id.startswith('arn:aws:bedrock'):
            # Blueprint ID is already an ARN
            blueprint_arn = request.blueprint_id
            output_path, blueprint_details = aws_clients.download_blueprint_by_arn(
                blueprint_arn=blueprint_arn,
                blueprint_stage=request.project_stage
            )
        else:
            # Blueprint ID is just an ID, try to find it in project first
            first_error = None
            try:
                output_path, blueprint_details = aws_clients.download_blueprint(
                    blueprint_id=request.blueprint_id,
                    project_arn=request.project_arn,
                    project_stage=request.project_stage
                )
            except Exception as e:
                first_error = e
                logger.debug(f"Blueprint not found in project, constructing ARN and trying direct access")
                
                # Construct ARN from project ARN and blueprint ID
                project_parts = request.project_arn.split(':')
                if len(project_parts) >= 5:
                    region = project_parts[3]
                    account = project_parts[4]
                    blueprint_arn = f"arn:aws:bedrock:{region}:{account}:blueprint/{request.blueprint_id}"
                    
                    # Try direct ARN access
                    try:
                        output_path, blueprint_details = aws_clients.download_blueprint_by_arn(
                            blueprint_arn=blueprint_arn,
                            blueprint_stage=request.project_stage
                        )
                    except Exception as arn_error:
                        logger.error(f"Both blueprint fetch methods failed. First: {type(first_error).__name__}, Second: {type(arn_error).__name__}")
                        raise HTTPException(status_code=500, detail="Failed to fetch blueprint from AWS")
                else:
                    raise HTTPException(status_code=400, detail="Invalid project ARN format")
        logger.debug(f"Blueprint downloaded to: {output_path}")
        
        # Validate output_path to prevent path traversal before reading
        # Use the project's path security module for proper validation
        output_dir = os.path.dirname(output_path) if os.path.dirname(output_path) else "output"
        abs_output_path = validate_path_within_directory(output_path, output_dir)
        if not os.path.isfile(abs_output_path):
            raise ValueError(f"Invalid output path: {output_path}")
        
        # Read the schema file
        logger.debug("Reading schema file...")
        
        # Security: Limit schema file size to prevent DoS
        MAX_SCHEMA_SIZE = 10 * 1024 * 1024  # 10MB limit
        try:
            file_size = os.path.getsize(abs_output_path)
            if file_size > MAX_SCHEMA_SIZE:
                logger.warning(f"Schema file exceeds maximum size: {file_size} bytes")
                raise HTTPException(status_code=400, detail="Schema file exceeds maximum allowed size")
        except OSError as e:
            logger.error(f"Error checking file size: {type(e).__name__}")
            raise HTTPException(status_code=500, detail="Error accessing schema file")
        
        # Path is validated by validate_path_within_directory above
        try:
            with open(abs_output_path, 'r') as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - abs_output_path validated above
                schema_content = f.read()
                logger.debug(f"Schema content length: {len(schema_content)}")
        except (IOError, OSError) as e:
            logger.error(f"Error reading schema file: {type(e).__name__}")
            raise HTTPException(status_code=500, detail="Error reading schema file")
        
        # Try to parse as JSON
        try:
            schema = json.loads(schema_content)
            logger.debug("Schema parsed successfully as JSON")
        except json.JSONDecodeError as e:
            logger.error(f"Schema is not valid JSON: {type(e).__name__}")
            # If it's not JSON, return empty properties
            return {
                "status": "success",
                "blueprint_name": blueprint_details.get('blueprintName', 'Unknown'),
                "output_path": output_path,
                "properties": []
            }
        
        # Check if this is a nested blueprint and flatten if needed
        from src.models.schema import Schema
        import tempfile
        
        # Security: Validate schema structure before processing
        MAX_PROPERTIES = 1000  # Limit number of properties to prevent DoS
        MAX_FIELD_NAME_LENGTH = 500
        MAX_INSTRUCTION_LENGTH = 10000
        
        if isinstance(schema, dict) and 'properties' in schema:
            if len(schema['properties']) > MAX_PROPERTIES:
                logger.warning(f"Schema has too many properties: {len(schema['properties'])}")
                raise HTTPException(status_code=400, detail="Schema exceeds maximum allowed properties")
            
            # Validate field names and instructions
            for field_name, field_data in schema['properties'].items():
                if len(str(field_name)) > MAX_FIELD_NAME_LENGTH:
                    logger.warning(f"Field name exceeds maximum length: {field_name[:50]}...")
                    raise HTTPException(status_code=400, detail="Field name exceeds maximum allowed length")
                if isinstance(field_data, dict):
                    instruction = field_data.get('instruction', '')
                    if len(str(instruction)) > MAX_INSTRUCTION_LENGTH:
                        logger.warning(f"Instruction exceeds maximum length for field: {field_name}")
                        raise HTTPException(status_code=400, detail="Field instruction exceeds maximum allowed length")
        
        # Create temporary file in output directory for Schema processing (path security requires it)
        temp_dir = "output/temp"
        os.makedirs(temp_dir, exist_ok=True)
        temp_schema_path = os.path.join(temp_dir, f"temp_schema_{os.getpid()}.json")
        with open(temp_schema_path, 'w') as temp_file:
            json.dump(schema, temp_file)
        
        properties = []  # Initialize properties before try block
        try:
            # Load schema using our Schema class
            schema_obj = Schema.from_file(temp_schema_path, allowed_dir=temp_dir)
            
            # Check if nested and flatten if needed
            if schema_obj.is_nested():
                logger.debug("Detected nested blueprint - flattening for UI display")
                flattened_schema, path_mapping = schema_obj.flatten_for_optimization()
                
                # Extract properties from flattened schema
                properties = []
                for field_name, field_data in flattened_schema.properties.items():
                    properties.append({
                        'field_name': field_name,
                        'instruction': field_data.instruction or '',
                        'expected_output': '',  # Empty by default, to be filled in by the user
                        'inference_type': field_data.inferenceType or 'explicit'
                    })
                logger.debug(f"Flattened nested blueprint: {len(properties)} fields")
                
            else:
                logger.debug("Flat blueprint detected - processing normally")
                # Extract properties from regular schema
                properties = []
                if isinstance(schema, dict) and 'properties' in schema:
                    for field_name, field_data in schema['properties'].items():
                        properties.append({
                            'field_name': field_name,
                            'instruction': field_data.get('instruction', ''),
                            'expected_output': '',  # Empty by default, to be filled in by the user
                            'inference_type': field_data.get('inferenceType', 'explicit')
                        })
                    logger.debug(f"Extracted {len(properties)} properties")
                else:
                    logger.debug("No properties found in schema")
                    
        finally:
            # Clean up temporary file
            if os.path.exists(temp_schema_path):
                os.unlink(temp_schema_path)
        
        # Return the blueprint details and properties
        # Security: Audit log successful blueprint fetch
        logger.info(f"Blueprint fetch successful - blueprint_name: {blueprint_details.get('blueprintName', 'Unknown')}, "
                    f"properties_count: {len(properties)}")
        
        return {
            "status": "success",
            "blueprint_name": blueprint_details.get('blueprintName', 'Unknown'),
            "output_path": output_path,
            "properties": properties
        }
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        import traceback
        logger.error(f"Blueprint fetch failed: {type(e).__name__}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch blueprint: {type(e).__name__}: {str(e)}")

@app.get("/api/final-schema")
@app.get("/final-schema")
async def get_final_schema():
    """Get the final schema generated by the optimizer."""
    try:
        import os
        import glob
        import json
        
        # Get the output/schemas directory with validation
        schemas_dir = os.path.join(BASE_DIR, "output", "schemas")
        if not schemas_dir.startswith(BASE_DIR):
            raise ValueError("Schemas directory path outside project bounds")
        
        # Check if the directory exists
        if not os.path.exists(schemas_dir):
            return {"status": "error", "message": "Schemas directory not found"}
        
        # Look for the most recent run directory
        run_dirs = glob.glob(os.path.join(schemas_dir, "run_*"))
        if not run_dirs:
            return {"status": "error", "message": "No run directories found"}
        
        # Sort by modification time (most recent first)
        run_dirs.sort(key=os.path.getmtime, reverse=True)
        latest_run_dir = run_dirs[0]
        
        # Validate that the run directory is within schemas_dir
        if not latest_run_dir.startswith(schemas_dir):
            return {"status": "error", "message": "Invalid run directory path"}
        
        # Look for schema_final.json in the latest run directory
        final_schema_path = os.path.join(latest_run_dir, "schema_final.json")
        
        if os.path.exists(final_schema_path) and final_schema_path.startswith(schemas_dir):
            # Read the schema file
            with open(final_schema_path, "r") as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - final_schema_path validated above
                schema_content = f.read()
            
            return {"status": "success", "schema": schema_content}
        else:
            # If schema_final.json doesn't exist, look for the highest numbered schema file
            schema_files = glob.glob(os.path.join(latest_run_dir, "schema_*.json"))
            if not schema_files:
                return {"status": "error", "message": "No schema files found"}
            
            # Extract numbers from filenames and find the highest
            schema_numbers = []
            for schema_file in schema_files:
                # Validate schema file path
                if not schema_file.startswith(schemas_dir):
                    continue
                    
                filename = os.path.basename(schema_file)
                if filename.startswith("schema_") and filename.endswith(".json"):
                    try:
                        # Extract the number part (schema_N.json -> N)
                        number_part = filename[7:-5]  # Remove "schema_" and ".json"
                        if number_part.isdigit():
                            schema_numbers.append(int(number_part))
                    except (ValueError, IndexError):
                        pass
            
            if schema_numbers:
                highest_schema = max(schema_numbers)
                highest_schema_path = os.path.join(latest_run_dir, f"schema_{highest_schema}.json")
                
                # Validate the highest schema path
                if highest_schema_path.startswith(schemas_dir):
                    # Read the highest numbered schema file
                    with open(highest_schema_path, "r") as f:  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal - highest_schema_path validated above
                        schema_content = f.read()
                    
                    return {"status": "success", "schema": schema_content}
            
            return {"status": "error", "message": "No valid schema files found"}
    except Exception as e:
        logger.error(f"Error getting final schema: {type(e).__name__}")
        return {"status": "error", "message": "Failed to get final schema"}

@app.get("/api/list-logs")
@app.get("/list-logs")
async def list_logs():
    """List all available log files."""
    try:
        log_dir = os.path.join(BASE_DIR, "logs")
        if not log_dir.startswith(BASE_DIR):
            raise ValueError("Log directory path outside project bounds")
            
        os.makedirs(log_dir, exist_ok=True)
        
        # Get all log files (both new and old naming patterns)
        log_files = [f for f in os.listdir(log_dir) if 
                    (f.startswith("optimizer-") or f.startswith("bda_optimizer_")) and 
                    f.endswith(".log")]
        log_files.sort(reverse=True)  # Most recent first
        
        return {"log_files": log_files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
