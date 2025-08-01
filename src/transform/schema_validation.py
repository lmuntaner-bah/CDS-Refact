import json
from collections import Counter
from typing import Dict, Any, List, Tuple
from jsonschema import validate, ValidationError, FormatChecker
from src.utils.config_loader import get_input_path
from src.utils.logger import setup_module_logger, send_alert, log_performance


# Initialize logger for this module
logger = setup_module_logger(__file__)


def load_source_schema() -> Dict[str, Any]:
    """
    Load the source schema from a JSON file.
    
    Returns:
        Dict[str, Any]: The loaded JSON schema
    
    Raises:
        FileNotFoundError: If the schema file doesn't exist
        json.JSONDecodeError: If the JSON file is malformed
    """
    schema_file = get_input_path("schema")
    
    try:
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")
        
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        
        logger.info(f"Successfully loaded source schema from {schema_file}")
        return schema
    
    except FileNotFoundError as e:
        error_msg = f"Schema file not found: {e}"
        logger.error(error_msg)
        
        send_alert("schema_validation", "ERROR", error_msg, {
            "file_path": schema_file,
            "exception_type": type(e).__name__,
            "exception": str(e)
        })
        raise
    except json.JSONDecodeError as e:
        error_msg = f"Error parsing JSON schema: {e}"
        logger.error(error_msg)
        
        send_alert("schema_validation", "ERROR", error_msg, {
            "file_path": schema_file,
            "exception_type": type(e).__name__,
            "exception": str(e)
        })
        raise


@log_performance
def validate_source_object(source_obj: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate a single source object against the source schema.
    
    Args:
        source_obj (Dict[str, Any]): The source object to validate
        schema (Dict[str, Any]): The JSON schema to validate against
    
    Returns:
        Tuple[bool, List[str]]: (is_valid, list_of_error_messages)
    """
    validation_errors = []
    
    try:
        # First, validate with jsonschema (handles most validation rules)
        validate(instance=source_obj, schema=schema, format_checker=FormatChecker())
        
        # Additional custom validations:
        # Validate ACM structure consistency
        if "acm" in source_obj:
            acm = source_obj["acm"]
            
            if "classif" in acm and "classif_type" in acm:
                # Check classification consistency
                classif = acm["classif"]
                classif_type = acm["classif_type"]
                
                if classif_type == "US" and classif not in "U":
                    validation_errors.append(f"Invalid classification '{classif}' for US type")
        
        # Validate attributes structure
        if "attributes" in source_obj:
            attributes = source_obj["attributes"]
            
            if "data" in attributes and isinstance(attributes["data"], list):
                for i, attr in enumerate(attributes["data"]):
                    if "attributeName" in attr and "attributeValue" in attr:
                        # Check for empty or None values
                        if not attr["attributeValue"] or attr["attributeValue"].strip() == "":
                            validation_errors.append(f"Empty attribute value for '{attr['attributeName']}' at index {i}")
        
        # Validate location coordinates if present
        if "latestKnownLocation" in source_obj:
            location = source_obj["latestKnownLocation"]
            
            if "geometry" in location and "coordinates" in location["geometry"]:
                coords = location["geometry"]["coordinates"]
                
                if isinstance(coords, list) and len(coords) == 2:
                    longitude, latitude = coords
                    if not (-180 <= longitude <= 180):
                        validation_errors.append(f"Invalid longitude value: {longitude}")
                    
                    if not (-90 <= latitude <= 90):
                        validation_errors.append(f"Invalid latitude value: {latitude}")
        
        is_valid = len(validation_errors) == 0
        return is_valid, validation_errors
    
    except ValidationError as e:
        validation_errors.append(f"Schema validation error: {e.message} at path: {e.absolute_path}")
        return False, validation_errors
    except Exception as e:
        validation_errors.append(f"Unexpected validation error: {str(e)}")
        return False, validation_errors


@log_performance
def validate_source_data(source_objects: List[Dict[str, Any]], schema_path: str = "../schemas/source_schema.json") -> Tuple[bool, Dict[str, Any]]:
    """
    Validate all source objects against the source schema.
    
    This function validates each source object in the list against the provided schema,
    logs any validation errors, and returns a comprehensive validation report.
    
    Args:
        source_objects (List[Dict[str, Any]]): List of source objects to validate
        schema_path (str): Path to the JSON schema file
    
    Returns:
        Tuple[bool, Dict[str, Any]]: (all_valid, validation_report)
            - all_valid: True if all objects are valid, False otherwise
            - validation_report: Dictionary containing detailed validation results
    """
    
    try:
        # Load the schema
        schema = load_source_schema()
        
        validation_report: Dict[str, Any] = {
            "total_objects": len(source_objects),
            "valid_objects": 0,
            "invalid_objects": 0,
            "validation_errors": {},
            "summary": {
                "all_valid": False,
                "error_types": {},
                "most_common_errors": []
            }
        }
        
        all_errors = []
        
        for i, source_obj in enumerate(source_objects):
            try:
                is_valid, errors = validate_source_object(source_obj, schema)
                
                if is_valid:
                    validation_report["valid_objects"] += 1
                    logger.debug(f"Object {i} (ID: {source_obj.get('id', 'unknown')}) passed validation")
                else:
                    validation_report["invalid_objects"] += 1
                    obj_id = source_obj.get('id', f'object_{i}')
                    
                    validation_report["validation_errors"][obj_id] = errors
                    all_errors.extend(errors)
                    
                    logger.warning(f"Object {i} (ID: {obj_id}) failed validation with {len(errors)} errors:")
                    for error in errors:
                        logger.warning(f"  - {error}")
            except Exception as e:
                validation_report["invalid_objects"] += 1
                obj_id = source_obj.get('id', f'object_{i}')
                
                error_msg = f"Unexpected error during validation: {str(e)}"
                validation_report["validation_errors"][obj_id] = [error_msg]
                
                all_errors.append(error_msg)
                logger.error(f"Object {i} (ID: {obj_id}) validation failed: {error_msg}")
        
        # Generate summary statistics
        validation_report["summary"]["all_valid"] = validation_report["invalid_objects"] == 0
        
        # Count error types
        error_type_counts = {}
        for error in all_errors:
            # Extract error type from error message
            if "schema validation" in error.lower():
                error_type = "schema_validation"
            elif "coordinate" in error.lower() or "longitude" in error.lower() or "latitude" in error.lower():
                error_type = "coordinate_validation"
            elif "empty" in error.lower():
                error_type = "empty_values"
            else:
                error_type = "other"
            
            error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1
        
        validation_report["summary"]["error_types"] = error_type_counts
        
        # Get most common errors (top 5)
        error_counter = Counter(all_errors)
        validation_report["summary"]["most_common_errors"] = error_counter.most_common(5)
        
        # Log summary
        if validation_report["summary"]["all_valid"]:
            logger.info(f"All {validation_report['total_objects']} source objects passed validation")
        else:
            logger.warning(f"Validation completed: {validation_report['valid_objects']}/{validation_report['total_objects']} objects valid")
            logger.warning(f"Invalid objects: {validation_report['invalid_objects']}")
            logger.warning(f"Error types found: {list(error_type_counts.keys())}")
        
        return validation_report["summary"]["all_valid"], validation_report
    except Exception as e:
        error_msg = f"Failed to validate source data: {str(e)}"
        logger.error(error_msg)
        
        send_alert("schema_validation", "ERROR", error_msg, {
                "exception_type": type(e).__name__,
                "exception": str(e)
            })
        
        return False, {
            "total_objects": len(source_objects) if source_objects else 0,
            "valid_objects": 0,
            "invalid_objects": len(source_objects) if source_objects else 0,
            "validation_errors": {"global_error": [error_msg]},
            "summary": {
                "all_valid": False,
                "error_types": {"validation_failure": 1},
                "most_common_errors": [(error_msg, 1)]
            }
        }