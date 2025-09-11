import os
import json
from loguru import logger
from typing import Dict, Any, List


def fetch_all_objects(directory: str) -> List[Dict[str, Any]]:
    """
    Load all JSON files from a specified directory into a list of dictionaries.

    Args:
        directory (str): Path to the directory containing JSON files.

    Returns:
        List[Dict[str, Any]]: List of dictionaries, each representing the contents of a JSON file.

    Note:
        This function mimics querying data from an API by reading local files.
        In production, replace this logic with actual API calls as needed.
    """
    try:
        json_list = []

        for current_file in os.scandir(directory):
            if current_file.is_file() and current_file.name.endswith(".json"):
                with open(current_file.path, "r") as json_file:
                    json_data = json.load(json_file)
                    json_list.append(json_data)

        logger.info("Source objects loaded successfully.")
        return json_list
    except FileNotFoundError as e:
        logger.error(f"Directory not found: {directory} - {str(e)}")
        return []