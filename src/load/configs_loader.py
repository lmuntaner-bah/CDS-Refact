import json
import yaml
from loguru import logger
from typing import Dict, Any


def load_valid_attribute_names(file_path: str) -> list:
    """
    Loads the list of valid attribute names from a YAML file.

    Args:
    - file_path (str): Path to the YAML file containing valid attribute names.

    Returns:
    - list: A list of valid attribute names.
    """
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            logger.info("Loaded valid attribute names successfully.")
            return data.get("valid_attribute_names", [])
    except Exception as e:
        logger.error(f"Error loading valid attribute names: {e}")
        return []


def load_attribute_mapping(config_file: str) -> Dict[str, Dict[str, str]]:
    """
    Load attribute mapping configuration from a YAML file.

    Returns:
        Dict[str, Dict[str, str]]: The attribute mapping dictionary

    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        yaml.YAMLError: If the YAML file is malformed
        KeyError: If required keys are missing from the configuration
    """
    try:
        if not config_file:
            raise FileNotFoundError(f"Configuration file not found: {config_file}")

        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        if "attribute_mapping" not in config:
            raise KeyError("'attribute_mapping' key not found in configuration file")

        attribute_mapping = config["attribute_mapping"]

        # Validate the structure
        for attr_name, mapping in attribute_mapping.items():
            if not isinstance(mapping, dict):
                raise ValueError(
                    f"Invalid mapping for attribute '{attr_name}': expected dict, got {type(mapping)}"
                )

            if "field" not in mapping or "container" not in mapping:
                raise KeyError(
                    f"Missing required keys ('field', 'container') for attribute '{attr_name}'"
                )

        logger.info(f"Successfully loaded attribute mapping from {config_file}")
        return attribute_mapping

    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration: {e}")
        raise
    except (KeyError, ValueError) as e:
        logger.error(f"Invalid configuration structure: {e}")
        raise


def load_classification_config(config_path: str) -> Dict[str, Any]:
    """
    Load classification configuration from a YAML file.
    
    Args:
        config_path (str): Path to the YAML configuration file.
    
    Returns:
        Dict[str, Any]: The 'restrictions' section from the configuration file
                       containing classification parameters and settings.
    """
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
            if "restrictions" not in config:
                raise ValueError("Missing 'restrictions' key in classification config")

            logger.info("Classification configuration loaded successfully.")
            return config["restrictions"]
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")


def load_standard_object_schema(schema_path: str) -> dict:
    """
    Load the JSON schema from a file.

    Args:
        schema_path (str): Path to the JSON schema
    
    Returns:
        dict: The loaded JSON schema
    """
    try:
        with open(schema_path, "r") as schema_file:
            schema = json.load(schema_file)
        logger.info(f"Schema loaded successfully from {schema_path}")
        return schema
    except FileNotFoundError:
        logger.error(f"Schema file not found at {schema_path}")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in schema file: {e}")
        return {}