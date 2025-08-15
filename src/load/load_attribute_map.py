import yaml
from typing import Dict
from src.utils.logger import setup_module_logger, send_alert

# Initialize logger for this module
logger = setup_module_logger(__file__)


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
            error_msg = f"Configuration file not found: {config_file}"
            logger.error(error_msg)

            send_alert(
                "config_loader",
                "ERROR",
                error_msg,
                {"file_path": config_file, "exception": "FileNotFoundError"},
            )
            raise FileNotFoundError(error_msg)

        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        if "attribute_mapping" not in config:
            error_msg = f"'attribute_mapping' key not found in configuration file: {config_file}"
            logger.error(error_msg)

            send_alert(
                "config_loader",
                "ERROR",
                error_msg,
                {"file_path": config_file, "exception": "KeyError"},
            )
            raise KeyError(error_msg)

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
        error_msg = "Configuration file not found"
        logger.error(error_msg)

        send_alert(
            "config_loader",
            "ERROR",
            error_msg,
            {"file_path": config_file, "exception": str(e)},
        )
        raise

    except yaml.YAMLError as e:
        error_msg = "Error parsing YAML configuration"
        logger.error(error_msg)

        send_alert(
            "config_loader",
            "ERROR",
            error_msg,
            {"file_path": config_file, "exception": str(e)},
        )
        raise

    except (KeyError, ValueError) as e:
        error_msg = "Invalid configuration structure"
        logger.error(error_msg)

        send_alert(
            "config_loader",
            "ERROR",
            error_msg,
            {"file_path": config_file, "exception": str(e)},
        )
        raise
