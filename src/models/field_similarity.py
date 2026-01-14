"""
Field type detection and specialized similarity functions for different field types.
"""
from enum import Enum
import logging
import re
from typing import Optional
import datetime
from dateutil import parser as date_parser
import numpy as np
from sentence_transformers import SentenceTransformer, util

# Configure logger for audit trail
logger = logging.getLogger(__name__)

# Maximum input length to prevent resource exhaustion
MAX_INPUT_LENGTH = 10000

# Module-level cached model instance (lazy initialization)
_sentence_transformer_model: Optional[SentenceTransformer] = None


def _get_sentence_transformer_model() -> SentenceTransformer:
    """
    Get the cached SentenceTransformer model, loading it on first use.
    Uses module-level singleton pattern to ensure the model is only loaded once.
    
    Returns:
        SentenceTransformer: Cached model instance
        
    Raises:
        RuntimeError: If model loading fails
    """
    global _sentence_transformer_model
    if _sentence_transformer_model is None:
        try:
            logger.info("Loading SentenceTransformer model (one-time initialization)")
            _sentence_transformer_model = SentenceTransformer('all-MiniLM-L6-v2')
        except Exception as e:
            logger.error(f"Failed to load SentenceTransformer model: {type(e).__name__}")
            raise RuntimeError(f"Failed to initialize SentenceTransformer model: {e}") from e
    return _sentence_transformer_model


class FieldType(Enum):
    """
    Enum for different field types.
    """
    TEXT = "text"
    DATE = "date"
    NUMERIC = "numeric"
    EMAIL = "email"
    PHONE = "phone"
    ADDRESS = "address"


def detect_field_type(field_name: str, expected_output: str, schema_type: str = "string") -> FieldType:
    """
    Detect the field type based on field name, expected output, and schema type.
    
    Args:
        field_name: Name of the field
        expected_output: Expected output value
        schema_type: Type from schema.json
        
    Returns:
        FieldType: Detected field type
    """
    # Input validation
    if not field_name or not isinstance(field_name, str):
        return FieldType.TEXT
    if not expected_output or not isinstance(expected_output, str):
        return FieldType.TEXT
    
    # Convert field name to lowercase for case-insensitive matching
    field_name_lower = field_name.lower()
    
    # Check for name fields (which should be text, not date)
    name_keywords = ["name", "vendor", "company", "organization", "client", "customer", "supplier"]
    if any(keyword in field_name_lower for keyword in name_keywords):
        return FieldType.TEXT
    
    # Check for date fields
    date_keywords = ["date", "day", "month", "year", "dob", "birth", "expiry", "expiration", "start", "end"]
    if any(keyword in field_name_lower for keyword in date_keywords):
        return FieldType.DATE
    
    # Check for numeric fields
    numeric_keywords = ["amount", "price", "cost", "fee", "number", "count", "quantity", "total", "sum", "percent", "rate"]
    if any(keyword in field_name_lower for keyword in numeric_keywords):
        return FieldType.NUMERIC
    
    # Check for email fields
    email_keywords = ["email", "e-mail", "mail"]
    if any(keyword in field_name_lower for keyword in email_keywords):
        return FieldType.EMAIL
    
    # Check for phone fields
    phone_keywords = ["phone", "mobile", "cell", "telephone", "fax"]
    if any(keyword in field_name_lower for keyword in phone_keywords):
        return FieldType.PHONE
    
    # Check for address fields
    address_keywords = ["address", "street", "city", "state", "zip", "postal", "country"]
    if any(keyword in field_name_lower for keyword in address_keywords):
        return FieldType.ADDRESS
    
    # If no match by field name, try to detect from expected output format
    try:
        # Check if expected output looks like an email
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', expected_output):
            return FieldType.EMAIL
        
        # Check if expected output looks like a phone number
        if re.match(r'^\+?[\d\s\(\)-]{7,}$', expected_output):
            return FieldType.PHONE
        
        # Check if expected output looks like a number
        if re.match(r'^[$€£¥]?\s*\d+([.,]\d+)?%?$', expected_output):
            return FieldType.NUMERIC
        
        # Check if expected output looks like a date
        # This check is moved lower in priority to avoid false positives
        try:
            date_parser.parse(expected_output)
            # If parsing succeeds and the string contains separators like /, -, or spaces
            if re.search(r'[/\-\s]', expected_output):
                return FieldType.DATE
        except (ValueError, TypeError):
            pass
    except (TypeError, re.error) as e:
        logger.debug(f"Pattern matching failed: {type(e).__name__}")
    
    # Default to text
    return FieldType.TEXT


def calculate_date_similarity(date1_str: str, date2_str: str) -> float:
    """
    Calculate similarity between two dates.
    
    Args:
        date1_str: First date as string
        date2_str: Second date as string
        
    Returns:
        float: Similarity score between 0 and 1
    """
    # Input validation
    if date1_str is None or date2_str is None:
        return 0.0
    
    try:
        # Convert to string if not already
        date1_str = str(date1_str).strip()
        date2_str = str(date2_str).strip()
        
        # Handle empty strings
        if not date1_str or not date2_str:
            return 0.0
        
        # Parse dates
        date1 = date_parser.parse(date1_str)
        date2 = date_parser.parse(date2_str)
        
        # Calculate difference in days
        diff_days = abs((date1 - date2).days)
        
        # Normalize to 0-1 range (closer to 1 is more similar)
        # Using a sigmoid-like function that gives high similarity for small differences
        # and rapidly decreases for larger differences
        similarity = 1.0 / (1.0 + (diff_days / 7.0))  # 7 days difference gives 0.5 similarity
        
        return similarity
    except (ValueError, TypeError, AttributeError, OverflowError) as e:
        # Fallback to text similarity if date parsing fails
        logger.debug(f"Date parsing failed, falling back to semantic similarity: {type(e).__name__}")
        return calculate_semantic_similarity(date1_str, date2_str)


def calculate_numeric_similarity(num1_str: str, num2_str: str) -> float:
    """
    Calculate similarity between two numeric values.
    
    Args:
        num1_str: First number as string
        num2_str: Second number as string
        
    Returns:
        float: Similarity score between 0 and 1
    """
    # Input validation - handle None values
    if num1_str is None or num2_str is None:
        return 0.0
    
    try:
        # Convert to string safely
        num1_str = str(num1_str).strip()
        num2_str = str(num2_str).strip()
        
        # Handle empty strings
        if not num1_str or not num2_str:
            return 0.0
        
        # Clean and parse numbers
        num1_clean = re.sub(r'[^\d.]', '', num1_str.replace(',', '.'))
        num2_clean = re.sub(r'[^\d.]', '', num2_str.replace(',', '.'))
        
        # Handle empty cleaned strings
        if not num1_clean or not num2_clean:
            return calculate_semantic_similarity(num1_str, num2_str)
        
        num1 = float(num1_clean)
        num2 = float(num2_clean)
        
        # Handle zero values to avoid division by zero
        if num1 == 0 and num2 == 0:
            return 1.0
        elif num1 == 0 or num2 == 0:
            return 0.0
        
        # Calculate relative difference
        max_val = max(abs(num1), abs(num2))
        min_val = min(abs(num1), abs(num2))
        
        # Similarity based on ratio (always between 0 and 1)
        similarity = min_val / max_val
        
        return similarity
    except (ValueError, TypeError, AttributeError, re.error, ZeroDivisionError) as e:
        # Fallback to text similarity if numeric parsing fails
        logger.debug(f"Numeric parsing failed, falling back to semantic similarity: {type(e).__name__}")
        return calculate_semantic_similarity(str(num1_str), str(num2_str))


def calculate_email_similarity(email1: str, email2: str) -> float:
    """
    Calculate similarity between two email addresses.
    
    Note: Uses calculate_semantic_similarity which accesses the module-level
    cached SentenceTransformer model (_sentence_transformer_model).
    
    Args:
        email1: First email
        email2: Second email
        
    Returns:
        float: Similarity score between 0 and 1
    """
    # Input validation - handle None values
    if email1 is None or email2 is None:
        return 0.0
    
    try:
        # Convert to string safely
        email1 = str(email1).lower().strip()
        email2 = str(email2).lower().strip()
        
        # Handle empty strings
        if not email1 or not email2:
            return 0.0
        
        # Exact match
        if email1 == email2:
            return 1.0
        
        # Split into username and domain
        try:
            username1, domain1 = email1.split('@')
            username2, domain2 = email2.split('@')
            
            # Domain match is weighted higher (0.6) than username match (0.4)
            domain_similarity = 1.0 if domain1 == domain2 else 0.0
            # Model is cached at module level via _sentence_transformer_model
            username_similarity = calculate_semantic_similarity(username1, username2)
            
            return 0.6 * domain_similarity + 0.4 * username_similarity
        except ValueError:
            # If splitting fails, use text similarity
            # Model is cached at module level via _sentence_transformer_model
            return calculate_semantic_similarity(email1, email2)
    except (AttributeError, TypeError) as e:
        # Fallback to text similarity
        logger.debug(f"Email similarity calculation failed: {type(e).__name__}")
        # Model is cached at module level via _sentence_transformer_model
        return calculate_semantic_similarity(str(email1), str(email2))


def calculate_phone_similarity(phone1: str, phone2: str) -> float:
    """
    Calculate similarity between two phone numbers.
    
    Args:
        phone1: First phone number
        phone2: Second phone number
        
    Returns:
        float: Similarity score between 0 and 1
    """
    # Input validation - handle None values
    if phone1 is None or phone2 is None:
        return 0.0
    
    try:
        # Convert to string safely
        phone1 = str(phone1).strip()
        phone2 = str(phone2).strip()
        
        # Handle empty strings
        if not phone1 or not phone2:
            return 0.0
        
        # Normalize phone numbers (remove non-digit characters)
        digits1 = re.sub(r'\D', '', phone1)
        digits2 = re.sub(r'\D', '', phone2)
        
        # Handle empty digit strings
        if not digits1 or not digits2:
            return calculate_semantic_similarity(phone1, phone2)
        
        # Exact match after normalization
        if digits1 == digits2:
            return 1.0
        
        # If one is a substring of the other (e.g., with/without country code)
        if digits1 in digits2 or digits2 in digits1:
            # Calculate similarity based on length ratio
            max_len = max(len(digits1), len(digits2))
            if max_len == 0:
                return 0.0
            return min(len(digits1), len(digits2)) / max_len
        
        # Calculate digit-by-digit similarity
        # Focus on the last digits which are usually more important
        min_len = min(len(digits1), len(digits2))
        if min_len < 4:
            return 0.0
        
        # Compare last N digits
        last_digits_to_compare = min(min_len, 8)  # Compare up to last 8 digits
        last_digits1 = digits1[-last_digits_to_compare:]
        last_digits2 = digits2[-last_digits_to_compare:]
        
        # Count matching digits
        matches = sum(d1 == d2 for d1, d2 in zip(last_digits1, last_digits2))
        
        return matches / last_digits_to_compare
    except (AttributeError, TypeError, re.error, ZeroDivisionError) as e:
        # Fallback to text similarity
        logger.debug(f"Phone similarity calculation failed: {type(e).__name__}")
        # Model is cached at module level via _sentence_transformer_model
        return calculate_semantic_similarity(str(phone1), str(phone2))


def calculate_address_similarity(addr1: str, addr2: str) -> float:
    """
    Calculate similarity between two addresses.
    
    Note: Uses calculate_semantic_similarity which accesses the module-level
    cached SentenceTransformer model (_sentence_transformer_model).
    
    Args:
        addr1: First address
        addr2: Second address
        
    Returns:
        float: Similarity score between 0 and 1
    """
    # Input validation - handle None values
    if addr1 is None or addr2 is None:
        return 0.0
    
    try:
        # Preprocess addresses with error handling
        addr1_processed = preprocess_address(addr1)
        addr2_processed = preprocess_address(addr2)
        
        # Handle empty strings after preprocessing
        if not addr1_processed or not addr2_processed:
            return 0.0
        
        # For addresses, semantic similarity works well
        # Model is cached at module level via _sentence_transformer_model
        return calculate_semantic_similarity(addr1_processed, addr2_processed)
    except (AttributeError, TypeError) as e:
        logger.debug(f"Address similarity calculation failed: {type(e).__name__}")
        # Fallback to semantic similarity with original strings
        return calculate_semantic_similarity(str(addr1), str(addr2))


def preprocess_address(address: str) -> str:
    """
    Preprocess address by normalizing common abbreviations.
    
    Args:
        address: Address string
        
    Returns:
        str: Preprocessed address
    """
    try:
        # Convert to lowercase
        address = str(address).lower()
        
        # Normalize common abbreviations
        replacements = {
            'st.': 'street',
            'st ': 'street ',
            'rd.': 'road',
            'rd ': 'road ',
            'ave.': 'avenue',
            'ave ': 'avenue ',
            'blvd.': 'boulevard',
            'blvd ': 'boulevard ',
            'apt.': 'apartment',
            'apt ': 'apartment ',
            'ste.': 'suite',
            'ste ': 'suite ',
            'n.': 'north',
            'n ': 'north ',
            's.': 'south',
            's ': 'south ',
            'e.': 'east',
            'e ': 'east ',
            'w.': 'west',
            'w ': 'west ',
        }
        
        for abbr, full in replacements.items():
            address = address.replace(abbr, full)
        
        return address
    except (AttributeError, TypeError) as e:
        logger.debug(f"Error preprocessing address: {type(e).__name__}")
        return str(address) if address else ""


def calculate_semantic_similarity(text1: str, text2: str) -> float:
    """
    Calculate semantic similarity between two texts using sentence embeddings.
    
    Includes input validation and length limits to prevent resource exhaustion.
    
    Args:
        text1: First text
        text2: Second text
        
    Returns:
        float: Similarity score between 0 and 1
        
    Raises:
        ValueError: If input exceeds maximum allowed length
    """
    try:
        # Handle empty strings
        if not text1 or not text2:
            return 0.0 if (not text1 and text2) or (text1 and not text2) else 1.0
        
        # Convert to string if not already
        text1 = str(text1)
        text2 = str(text2)
        
        # Input validation: enforce maximum length to prevent resource exhaustion
        if len(text1) > MAX_INPUT_LENGTH or len(text2) > MAX_INPUT_LENGTH:
            logger.warning("Input text exceeds maximum length limit for embedding calculation")
            raise ValueError(f"Input text exceeds maximum length of {MAX_INPUT_LENGTH} characters")
        
        # Exact match
        if text1.lower() == text2.lower():
            return 1.0
        
        # Log embedding calculation for audit trail (without sensitive data)
        logger.debug("Calculating semantic similarity for text comparison")
        
        # Get cached model instance
        model = _get_sentence_transformer_model()
        
        # Encode texts
        embeddings = model.encode([text1, text2], convert_to_tensor=True)
        
        # Calculate cosine similarity
        similarity = util.cos_sim(embeddings[0], embeddings[1])
        
        return float(similarity.item())
    except Exception as e:
        logger.error(f"Error in semantic similarity calculation: {type(e).__name__}")
        
        # Fallback to simple string matching
        text1 = str(text1).lower()
        text2 = str(text2).lower()
        
        if text1 == text2:
            return 1.0
        elif text1 in text2 or text2 in text1:
            return 0.8
        else:
            return 0.0


def calculate_field_similarity(field_name: str, expected: str, actual: str, field_type: Optional[FieldType] = None) -> float:
    """
    Calculate similarity based on detected or provided field type.
    
    Note: For text fields, uses calculate_semantic_similarity which accesses 
    the module-level cached SentenceTransformer model (_sentence_transformer_model).
    
    Args:
        field_name: Name of the field
        expected: Expected output value
        actual: Actual output value
        field_type: Field type (optional)
        
    Returns:
        float: Similarity score between 0 and 1
    """
    # Handle None or empty values
    if expected is None or actual is None:
        return 0.0 if (expected is None and actual is not None) or (expected is not None and actual is None) else 1.0
    
    expected = str(expected).strip()
    actual = str(actual).strip()
    
    # Exact match check
    if expected.lower() == actual.lower():
        return 1.0
    
    # Detect field type if not provided
    if field_type is None:
        field_type = detect_field_type(field_name, expected)
    
    # Select appropriate similarity function
    # All semantic similarity calls use the module-level cached model
    if field_type == FieldType.DATE:
        return calculate_date_similarity(expected, actual)
    elif field_type == FieldType.NUMERIC:
        return calculate_numeric_similarity(expected, actual)
    elif field_type == FieldType.EMAIL:
        return calculate_email_similarity(expected, actual)
    elif field_type == FieldType.PHONE:
        return calculate_phone_similarity(expected, actual)
    elif field_type == FieldType.ADDRESS:
        return calculate_address_similarity(expected, actual)
    else:  # Default to semantic similarity for text
        # Model is cached at module level via _sentence_transformer_model
        return calculate_semantic_similarity(expected, actual)
