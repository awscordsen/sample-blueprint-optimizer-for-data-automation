"""
Path security utilities to prevent path traversal attacks (CWE-22).
"""
import os
import re
from typing import Optional


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a filename to prevent path traversal attacks.
    
    Args:
        filename: The filename to sanitize
        
    Returns:
        A safe filename with only alphanumeric characters, underscores, hyphens, and dots
        
    Raises:
        ValueError: If the filename is empty or invalid after sanitization
    """
    if not filename or not isinstance(filename, str):
        raise ValueError("Filename must be a non-empty string")
    
    # Remove any path separators and parent directory references
    safe_name = os.path.basename(filename)
    
    # Only allow safe characters: alphanumeric, underscore, hyphen, dot
    safe_name = re.sub(r'[^\w\-.]', '_', safe_name)
    
    # Remove any leading dots to prevent hidden files
    safe_name = safe_name.lstrip('.')
    
    if not safe_name:
        raise ValueError(f"Invalid filename '{filename}': empty after sanitization")
    
    return safe_name


def validate_path_within_directory(path: str, allowed_base_dir: str) -> str:
    """
    Validate that a path is within the allowed directory.
    
    Args:
        path: The path to validate
        allowed_base_dir: The base directory that path must be within
        
    Returns:
        The validated absolute path
        
    Raises:
        ValueError: If the path would escape the allowed directory or inputs are invalid
    """
    if not path or not isinstance(path, str):
        raise ValueError("Path must be a non-empty string")
    if not allowed_base_dir or not isinstance(allowed_base_dir, str):
        raise ValueError("Allowed base directory must be a non-empty string")
    
    # Resolve to absolute path
    abs_path = os.path.realpath(path)
    abs_base = os.path.realpath(allowed_base_dir)
    
    # Ensure the path is within the allowed base directory
    if not abs_path.startswith(abs_base + os.sep) and abs_path != abs_base:
        raise ValueError(
            f"Path traversal detected: path must be within {allowed_base_dir}"
        )
    
    return abs_path


def safe_join_path(base_dir: str, *paths: str) -> str:
    """
    Safely join paths, ensuring the result stays within base_dir.
    
    Args:
        base_dir: The base directory
        *paths: Path components to join
        
    Returns:
        The joined path, validated to be within base_dir
        
    Raises:
        ValueError: If the resulting path would escape base_dir
    """
    if not paths:
        return validate_path_within_directory(base_dir, base_dir)
    
    # Sanitize each path component
    safe_paths = []
    for p in paths:
        if p:
            safe_paths.append(sanitize_filename(p))
    
    # Join the paths (components sanitized above, result validated below)
    joined = os.path.join(base_dir, *safe_paths)  # nosec B108 # nosemgrep: python.lang.security.audit.path-traversal
    
    # Validate the result
    return validate_path_within_directory(joined, base_dir)


def validate_file_extension(filename: str, allowed_extensions: list) -> bool:
    """
    Validate that a file has an allowed extension.
    
    Args:
        filename: The filename to check
        allowed_extensions: List of allowed extensions (e.g., ['.json', '.csv'])
        
    Returns:
        True if extension is allowed
        
    Raises:
        ValueError: If extension is not allowed or inputs are invalid
    """
    if not filename or not isinstance(filename, str):
        raise ValueError("Filename must be a non-empty string")
    if not allowed_extensions or not isinstance(allowed_extensions, list):
        raise ValueError("Allowed extensions must be a non-empty list")
    
    _, ext = os.path.splitext(filename.lower())
    allowed_lower = [e.lower() for e in allowed_extensions]
    
    if ext not in allowed_lower:
        raise ValueError(
            f"Invalid file extension '{ext}'. Allowed: {allowed_extensions}"
        )
    return True
