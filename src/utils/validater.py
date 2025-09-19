from loguru import logger
from typing import Dict, Any, List
from jsonschema import validate, ValidationError
from src.load.configs_loader import load_standard_object_schema


def validate_standard_object(
    standard_object: dict,
    schema: dict,
) -> bool:
    """
    Validate a standard object against the JSON schema with detailed error reporting.

    Args:
        standard_object (dict): The processed standard object to validate
        schema_path (str): Path to the JSON schema file

    Returns:
        bool: True if validation passes, False otherwise
    """
    try:
        # Validate the object
        validate(instance=standard_object, schema=schema)

        logger.info("VALIDATION PASSED: Standard object conforms to schema")
        logger.info(f"   - Object ID: {standard_object.get('id', 'Unknown')}")
        return True

    except ValidationError as e:
        logger.error("VALIDATION FAILED: Object does not conform to schema")
        logger.error(f"Object ID: {standard_object.get('id', 'Unknown')}")
        logger.error(
            f"   Error Located: {' -> '.join(str(x) for x in e.absolute_path) if e.absolute_path else 'Root level'}"
        )
        logger.error(f"   Error Message: {e.message}")

        # Try to provide more context about the failing value
        if e.absolute_path:
            failing_value = standard_object
            try:
                for path_element in e.absolute_path:
                    failing_value = failing_value[path_element]
                logger.warning(f"   Failing Value: {failing_value}")
                logger.warning(f"   Value Type: {type(failing_value).__name__}")
            except (KeyError, TypeError, IndexError):
                logger.error("   Could not retrieve failing value")

        # Show the schema requirement that failed
        if hasattr(e, "schema"):
            schema_info = e.schema
            if isinstance(schema_info, dict):
                if "type" in schema_info:
                    logger.warning(f"   Expected Type: {schema_info['type']}")
                if "required" in schema_info:
                    logger.warning(f"   Required Fields: {schema_info['required']}")

        return False

    except Exception as e:
        logger.error(f"VALIDATION FAILED: Unexpected error during validation")
        logger.error(f"   Error Type: {type(e).__name__}")
        logger.error(f"   Error Message: {str(e)}")
        return False


def run_validations(
    cleaned_objects: List[Dict[str, Any]],
    schema_path: str,
) -> Dict[str, Any]:
    """
    Validate all standard objects against the JSON schema.

    Args:
        cleaned_objects: List of cleaned standard objects to validate
        schema_path: Path to the JSON schema file

    Returns:
        Dict containing validation summary and details
    """
    schema = load_standard_object_schema(schema_path)
    if not schema:
        logger.warning("Cannot validate objects without a valid schema.")
        return {
            "all_valid": False,
            "total_objects": 0,
            "valid_count": 0,
            "failed_count": 0,
            "failed_objects": [],
        }

    if not cleaned_objects:
        logger.warning("No standard objects to validate.")
        return {
            "all_valid": False,
            "total_objects": 0,
            "valid_count": 0,
            "failed_count": 0,
            "failed_objects": [],
        }

    logger.info(f"Validating {len(cleaned_objects)} standard objects...")

    validation_results = []
    failed_objects = []

    for i, obj in enumerate(cleaned_objects):
        try:
            is_valid = validate_standard_object(obj, schema)
            validation_results.append(is_valid)

            if not is_valid:
                failed_objects.append(
                    {
                        "index": i,
                        "id": obj.get("id", "Unknown"),
                        "name": obj.get("name", "Unknown"),
                    }
                )
        except Exception as e:
            logger.error(f"Unexpected error validating object {i}: {e}")
            validation_results.append(False)
            failed_objects.append(
                {
                    "index": i,
                    "id": obj.get("id", "Unknown"),
                    "name": obj.get("name", "Unknown"),
                    "error": str(e),
                }
            )

    # Calculate summary
    total_objects = len(cleaned_objects)
    valid_count = sum(validation_results)
    failed_count = len(failed_objects)
    all_valid = all(validation_results)

    # Log summary
    if all_valid:
        logger.info(
            f"ALL VALIDATION PASSED: {total_objects}/{total_objects} objects conform to schema"
        )
        logger.info("Ready to proceed with all valid standard objects.")
    else:
        logger.warning(f" VALIDATION SUMMARY:")
        logger.warning(f"   Total objects: {total_objects}")
        logger.warning(f"   Valid objects: {valid_count}")
        logger.warning(f"   Failed objects: {failed_count}")
        logger.warning(f"   Success rate: {(valid_count/total_objects)*100:.1f}%")

        # Log details of failed objects (limit to first 10 to avoid spam)
        max_display = 10
        logger.warning(
            f"Failed object details (showing first {min(failed_count, max_display)}):"
        )
        for i, failed in enumerate(failed_objects[:max_display]):
            error_msg = (
                f" - Error: {failed.get('error', 'Schema validation failed')}"
                if "error" in failed
                else ""
            )
            logger.warning(
                f"   {i+1}. Index {failed['index']}: {failed['id']} ({failed['name']}){error_msg}"
            )

        if failed_count > max_display:
            logger.warning(
                f"   ... and {failed_count - max_display} more failed objects"
            )

    return {
        "all_valid": all_valid,
        "total_objects": total_objects,
        "valid_count": valid_count,
        "failed_count": failed_count,
        "failed_objects": failed_objects,
        "validation_results": validation_results,
    }