from loguru import logger
from datetime import datetime
from typing import Dict, Any, List, Optional

def _validate_attributes(attributes: Dict[str, Any]) -> None:
    """Validate attribute structure and values"""
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
    """Validate the ACM structure"""
    required_fields = ["portion", "banner"]

    if not all(field in acm for field in required_fields):
        missing = [f for f in required_fields if f not in acm]

        logger.warning(f"Missing ACM fields: {missing}")
        return False

    return True


def _validate_required_fields(obj: Dict[str, Any]) -> bool:
    """Validate required object fields"""
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
    Preprocess and validate raw objects

    Args:
        raw_objects: List of raw input objects

    Returns:
        Dict mapping object IDs to preprocessed objects
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
    Converts date strings in 'lastVerified.timestamp' and 'Date Of Introduction' attributes to Unix timestamps.
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