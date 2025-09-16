from loguru import logger
from datetime import datetime
from typing import Dict, Any, List, Optional


def _validate_attributes(attributes: Dict[str, Any]) -> None:
    """
    Validate the structure and content of attributes dictionary.
    
    This function performs validation checks on an attributes dictionary to ensure
    it has the correct structure and contains all required fields.
    
    Args:
        attributes (Dict[str, Any]): A dictionary containing attribute data with 
            expected structure including a 'data' key containing a list of 
            dictionaries with 'attributeName' and 'attributeValue' fields.
    
    Returns:
        None
    
    Raises:
        ValueError: If attributes is not a dictionary type.
        ValueError: If the 'data' key is missing or empty in attributes.
        ValueError: If required fields ('attributeName', 'attributeValue') are 
            missing from the first item in the data list.
    """
    if not isinstance(attributes, dict):
        raise ValueError(
            f"Invalid attribute type: expected dict, got {type(attributes)}"
        )

    required_fields = ["attributeName", "attributeValue"]

    if not attributes["data"]:
        raise ValueError(f"Missing `data` in `attributes`")

    attributes_data_keys = attributes["data"][0].keys()
    if not all(k in attributes_data_keys for k in required_fields):
        missing_fields = [k for k in required_fields if k not in attributes_data_keys]

        raise ValueError(f"Missing required fields: {missing_fields}")
    return None


def _validate_acm(acm: Dict[str, Any]) -> bool:
    """
    Validate that an ACM (Access Control Model) dictionary contains all required fields.
    
    Args:
        acm (Dict[str, Any]): Dictionary representing an ACM object to validate.
    
    Returns:
        bool: True if all required fields are present, False otherwise.
    """
    required_fields = ["portion", "banner"]

    if not all(field in acm for field in required_fields):
        missing = [f for f in required_fields if f not in acm]

        logger.warning(f"Missing ACM fields: {missing}")
        return False

    return True


def _validate_required_fields(obj: Dict[str, Any]) -> bool:
    """
    Validate that a data object contains all required fields for processing.
    
    Checks for the presence and validity of essential fields including 'id',
    'acm' (which undergoes ACM validation), and 'attributes'. Logs appropriate
    warnings or errors for missing or invalid fields.
    
    Args:
        obj (Dict[str, Any]): The data object to validate
    
    Returns:
        bool: True if all required fields are present and valid, False otherwise.
    """
    if not obj.get("id"):
        logger.warning("Raw object is missing 'id' attribute")
        return False

    if not _validate_acm(obj.get("acm", {})):
        logger.error(f"Failed ACM validation for object {obj.get('id')}")
        return False

    if not obj.get("attributes"):
        logger.warning(f"No attributes found for object {obj.get('id')}")
        return False

    return True


def handle_special_cases_raw(raw_object: Dict[str, Any]) -> None:
    """
    Handle special cases for attributes that need custom processing logic on raw objects.

    This function modifies the raw object's attributes in place to handle:
    - Target Restriction: Convert to boolean
    - Military Symbology Code: Validate length and nullify if invalid

    Args:
        raw_object: The raw object containing attributes.data to process
    """
    try:
        attributes_data = raw_object.get("attributes", {}).get("data", [])

        # Use a list to track attributes to remove (to avoid modifying list during iteration)
        attributes_to_remove = []

        for i, attr in enumerate(attributes_data):
            attr_name = attr.get("attributeName")
            attr_value = attr.get("attributeValue")

            if not attr_name:
                continue

            # Handle Target Restriction - ensure boolean value
            if attr_name == "Target Restriction":
                if attr_value is not None:
                    attr["attributeValue"] = bool(attr_value)
                    logger.debug(
                        f"Converted Target Restriction to boolean: {attr['attributeValue']}"
                    )

            # Handle Military Symbology Code - validate length
            elif attr_name == "Military Symbology Code":
                if attr_value is None or (
                    isinstance(attr_value, str) and len(attr_value) != 15
                ):
                    # Mark this attribute for removal instead of setting to None
                    attributes_to_remove.append(i)
                    logger.debug(
                        f"Removing invalid Military Symbology Code (length: {len(attr_value) if attr_value else 'None'})"
                    )

        # Remove invalid attributes in reverse order to maintain indices
        for i in reversed(attributes_to_remove):
            attributes_data.pop(i)

    except Exception as e:
        logger.error(
            f"Error handling special cases for object {raw_object.get('id', 'unknown')}: {e}"
        )


def preprocess_raw_data(raw_objects: List[Dict[str, Any]]):
    """
    Preprocesses a list of raw data objects by validating and filtering them.
    
    This function performs a multi-step validation and preprocessing pipeline:
    1. Validates required fields for each object
    2. Validates object attributes 
    3. Handles special cases for attribute processing
    4. Returns only objects that pass all validation steps
    
    Args:
        raw_objects (List[Dict[str, Any]]): List of raw data objects to preprocess.
            Each object should be a dictionary containing an 'id' field and 
            optional 'attributes' field.
    
    Returns:
        List[Dict[str, Any]]: List of successfully processed objects that passed
            all validation steps. Objects that fail validation are excluded from
            the result.
    """
    processed_data = []

    for obj in raw_objects:
        # 1. First validate required fields
        if not _validate_required_fields(obj):
            continue

        # 2. Then validate attributes
        attributes = obj.get("attributes", {})

        try:
            _validate_attributes(attributes)
        except ValueError as e:
            logger.error(
                f"Attribute validation failed for object {obj.get('id')}: {str(e)}"
            )
            continue

        # 3. Handle special cases for attribute processing
        handle_special_cases_raw(obj)

        # 4. If all validations pass, format and add to processed data
        processed_data.append(obj)
    return processed_data


def prepare_dates(source_objects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert date strings to Unix timestamps in source objects.
    This function processes a list of source objects and converts date strings
    to Unix timestamps (integers) in two specific locations:
    
    1. The 'timestamp' field within 'lastVerified' objects
    2. The 'attributeValue' field for attributes with name "date of introduction"
    
    The function supports multiple date formats:
    - ISO format with microseconds: "%Y-%m-%dT%H:%M:%S.%fZ"
    - ISO format without microseconds: "%Y-%m-%dT%H:%M:%SZ" 
    - Date only format: "%Y-%m-%d"
    
    Args:
        source_objects (List[Dict[str, Any]]): A list of dictionary objects
            containing date fields to be converted.
    
    Returns:
        List[Dict[str, Any]]: The same list of objects with date strings
            converted to Unix timestamps where applicable. Objects are
            modified in-place.
    """
    def to_unix(date_str: str) -> Optional[int]:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                return int(datetime.strptime(date_str, fmt).timestamp())
            except Exception:
                continue
        return None

    for obj in source_objects:
        # lastVerified.timestamp
        ts = obj.get("lastVerified", {}).get("timestamp")
        
        if isinstance(ts, str):
            unix_ts = to_unix(ts)
            if unix_ts is not None:
                obj["lastVerified"]["timestamp"] = unix_ts

        # Date Of Introduction in attributes
        for attr in obj.get("attributes", {}).get("data", []):
            if attr.get("attributeName", "").strip().lower() == "date of introduction":
                date_str = attr.get("attributeValue")
                
                if isinstance(date_str, str):
                    unix_ts = to_unix(date_str)
                    if unix_ts is not None:
                        attr["attributeValue"] = unix_ts

    logger.info("Dates prepared successfully.")
    return source_objects