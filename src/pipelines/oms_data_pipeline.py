import os
import json
from loguru import logger
from datetime import datetime
from typing import Dict, Any, List
from src.utils.validater import run_validations
from src.utils.attribute_drift_detector import detect_unexpected_attribute_names
from src.extract.object_extractor import fetch_all_objects
from src.transform.preprocessor import prepare_dates
from src.transform.object_parser import process_objects, clean_object
from src.load.configs_loader import load_attribute_mapping, load_classification_config, load_valid_attribute_names


def _save_standard_objects(
    output_path: str, cleaned_objects: List[Dict[str, Any]]
) -> None:
    """
    Save each cleaned standard object to a separate JSON file.

    This function saves each cleaned standard object to a JSON file with a filename
    based on the object ID and current timestamp. It handles file writing errors
    and ensures proper JSON formatting.

    Args:
        cleaned_objects (List[Dict[str, Any]]): List of cleaned standard objects to save

    Returns:
        None: The function does not return a value, but logs the results of the save operation.

    Raises:
        OSError: If the output directory cannot be created or accessed
    """
    # Generate timestamp for this batch
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for i, obj in enumerate(cleaned_objects):
        try:
            # Get object ID, fallback to index if ID is missing
            obj_id = obj.get("id", f"object_{i}")

            # Create filename with object ID and timestamp
            filename = f"{obj_id}_{timestamp}.json"
            file_path = os.path.join(output_path, filename)

            # Ensure the object is JSON serializable
            if not isinstance(obj, dict):
                raise ValueError(f"Object {i} is not a valid dictionary")

            # Write JSON file with proper formatting
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(obj, f, indent=2)

            logger.info(f"Successfully saved object {obj_id} to {filename}")
        except (ValueError, TypeError) as e:
            error_msg = f"Object {i} serialization error: {e}"
            logger.error(error_msg)
            raise
        except OSError as e:
            error_msg = f"File write error for object {i}: {e}"
            logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Unexpected error saving object {i}: {e}"
            logger.error(error_msg)
            raise


def run_pipeline(
    data_path: str,
    output_path: str,
    schema_path: str,
    restrictions_path: str,
    attribute_mapping_path: str,
    attribute_report_path: str,
    valid_attributes_path: str,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]] | None:
    """
    Orchestrates the main data processing workflow:
    - Fetches source objects from a local JSON file.
    - Processes objects using the parser logic to generate standard objects.
    - Saves processed (standard) objects locally.

    Args:
        data_path (str): Path to the JSON file containing raw input objects.
        output_path (str): Directory path to save the processed standard objects.
        schema_path (str): Path to the JSON schema file for validation.
        restrictions_path (str): Path to the JSON configuration file for classification restrictions.
        attribute_mapping_path (str): Path to the YAML configuration file for attribute mapping.
        attribute_report_path (str): Path to save the attribute report for unexpected attributes.
        valid_attributes_path (str): Path to the JSON file containing valid attribute names.

    Returns:
        tuple[List[Dict[str, Any]], Dict[str, Any]]:
            - cleaned_standard_objects: List of processed standard objects.
            - validation_summary: Validation summary dictionary.

    Raises:
        Exception: Logs any errors encountered during processing and returns None.
    """
    try:
        logger.info("Starting data fetching and processing pipeline...")

        # Get the source objects
        source_objects = fetch_all_objects(data_path)
        logger.info(f"Fetched {len(source_objects)} objects")

        # Load attribute mapping, restrictions, and valid attribute names from configuration file
        attribute_mapping = load_attribute_mapping(attribute_mapping_path)
        valid_attribute_names = load_valid_attribute_names(valid_attributes_path)
        restrictions_config = load_classification_config(restrictions_path)

        # Detect unexpected attribute names across all source objects
        detect_unexpected_attribute_names(
            source_objects, valid_attribute_names, attribute_report_path
        )
        
        # Prepare dates in source objects
        source_objects = prepare_dates(source_objects)

        # Transform each source object into the standard format
        standard_objects = process_objects(
            source_objects, attribute_mapping, restrictions_config
        )
        logger.info(f"Processed {len(standard_objects)} objects into standard format")

        # Apply the cleanup function to all standard objects
        cleaned_standard_objects = [clean_object(obj) for obj in standard_objects]
        logger.info("Cleaned standard objects by removing any empty containers")

        # Validate all cleaned standard objects and get summary
        validation_summary = run_validations(cleaned_standard_objects, schema_path)

        # Save the cleaned standard objects to JSON files
        _save_standard_objects(output_path, cleaned_standard_objects)

        # TODO: Add the function that loads the data into databricks

        return cleaned_standard_objects, validation_summary
    except Exception as e:
        logger.error(f"Error in pipeline execution: {e}")
        return None