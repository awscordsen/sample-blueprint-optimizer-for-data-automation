"""
Field type detection for the BDA optimization application.
"""
import re
from typing import Literal

# Define field types
FieldType = Literal["text", "date", "numeric", "email", "phone", "address", "unknown"]

def detect_field_type(field_name: str, expected_output: str) -> FieldType:
    """
    Detect the likely type of a field based on name and expected output.
    
    Args:
        field_name: Name of the field
        expected_output: Expected output value
        
    Returns:
        FieldType: Detected field type
    """
    # CWE-20: Input validation for field_name parameter
    # Validates type, nullness, and emptiness to prevent runtime errors
    if field_name is None or not isinstance(field_name, str):
        return "unknown"
    if not field_name.strip():
        return "unknown"
    
    # CWE-20: Input validation for expected_output parameter
    if expected_output is None or not isinstance(expected_output, str):
        expected_output = ""
    
    # Check for date patterns
    date_patterns = [
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # MM/DD/YYYY, DD/MM/YYYY
        r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',    # YYYY/MM/DD
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b'  # Month DD, YYYY
    ]
    
    # Check for numeric patterns
    numeric_patterns = [
        r'^\d+$',                          # Integers
        r'^\d+\.\d+$',                     # Decimals
        r'^\$\d+(?:\.\d{2})?$',            # Currency
        r'^\d{1,3}(?:,\d{3})*(?:\.\d+)?$'  # Formatted numbers
    ]
    
    # Check for email patterns
    email_patterns = [
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'  # Basic email pattern
    ]
    
    # Check for phone patterns
    # ReDoS-safe patterns: simplified to avoid nested quantifiers and optional groups
    # Length limit enforced before regex matching to prevent exponential backtracking
    phone_patterns = [
        r'^\+?1?[0-9]{10}$',           # US/Canada phone (10 digits, optional +1)
        r'^\+?[0-9]{7,15}$'            # International (7-15 digits per E.164 standard)
    ]
    
    # Check field name for type hints
    # nosec CWE-20: field_name is validated at function entry (type, null, empty checks)
    try:
        name_lower = field_name.lower()  # noqa: CWE-20
    except AttributeError:
        return "unknown"
    
    if any(term in name_lower for term in ['date', 'day', 'month', 'year', 'time']):
        return "date"
    elif any(term in name_lower for term in ['amount', 'price', 'cost', 'fee', 'total', 'sum', 'number']):
        return "numeric"
    elif any(term in name_lower for term in ['email', 'mail']):
        return "email"
    elif any(term in name_lower for term in ['phone', 'fax', 'mobile', 'cell']):
        return "phone"
    elif any(term in name_lower for term in ['address', 'street', 'city', 'state', 'zip', 'postal']):
        return "address"
    
    # Check expected output for patterns
    for pattern in date_patterns:
        if re.search(pattern, expected_output):
            return "date"
    
    for pattern in numeric_patterns:
        if re.search(pattern, expected_output):
            return "numeric"
    
    for pattern in email_patterns:
        if re.search(pattern, expected_output):
            return "email"
    
    # ReDoS protection: normalize phone input and check length before regex matching
    # Strip common phone formatting characters before length check
    phone_digits = re.sub(r'[^0-9+]', '', expected_output)
    if 7 <= len(phone_digits) <= 16:  # Valid phone length range (including optional +)
        for pattern in phone_patterns:
            if re.search(pattern, phone_digits):
                return "phone"
    
    # Default to text if no specific type detected
    return "text"
