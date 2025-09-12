import yaml
from loguru import logger
from difflib import get_close_matches
from typing import Dict, Any, List


def capture_unexpected_attributes(
    source_objects: List[Dict[str, Any]], valid_attribute_names: List[str]
) -> Dict[str, Any]:
    """
    Captures unexpected attribute names from source objects.

    Args:
    - source_objects (List[Dict[str, Any]]): A list of JSON objects to check.
    - valid_attribute_names (List[str]): A list of valid attribute names.

    Returns:
    - Dict[str, Any]: Dictionary containing unexpected attributes and their details.
    """
    unexpected_attributes = {}
    total_attributes_checked = 0
    objects_with_issues = []

    for obj_index, json_object in enumerate(source_objects):
        attributes = json_object.get("attributes", {}).get("data", [])
        object_unexpected = []

        for attr_index, attribute in enumerate(attributes):
            total_attributes_checked += 1
            attribute_name = attribute.get("attributeName")

            if attribute_name not in valid_attribute_names:
                if attribute_name not in unexpected_attributes:
                    unexpected_attributes[attribute_name] = {"count": 0, "objects": []}

                unexpected_attributes[attribute_name]["count"] += 1
                unexpected_attributes[attribute_name]["objects"].append(
                    {
                        "object_index": obj_index,
                        "attribute_index": attr_index,
                        "object_id": json_object.get("id", "unknown"),
                    }
                )
                object_unexpected.append(attribute_name)

        if object_unexpected:
            objects_with_issues.append(
                {
                    "object_index": obj_index,
                    "object_id": json_object.get("id", "unknown"),
                    "unexpected_attributes": object_unexpected,
                }
            )

    return {
        "unexpected_attributes": unexpected_attributes,
        "objects_with_issues": objects_with_issues,
        "total_attributes_checked": total_attributes_checked,
        "total_objects_checked": len(source_objects),
    }


def add_fuzzy_matching(
    unexpected_attributes: Dict[str, Any], valid_attribute_names: List[str]
) -> None:
    """
    Adds fuzzy matching suggestions to unexpected attributes.

    Args:
    - unexpected_attributes (Dict[str, Any]): Dictionary of unexpected attributes to enhance.
    - valid_attribute_names (List[str]): List of valid attribute names for matching.
    """

    for unexpected_name in unexpected_attributes.keys():
        similar_names = get_close_matches(
            unexpected_name, valid_attribute_names, n=3, cutoff=0.6
        )
        unexpected_attributes[unexpected_name]["similar_valid_names"] = similar_names


def save_analysis_report(
    analysis_results: Dict[str, Any],
    attribute_report_path: str,
) -> bool:
    """
    Saves the analysis results to a YAML file.

    Args:
    - analysis_results (Dict[str, Any]): Results from the attribute analysis.
    - attribute_report_path (str): Path to save the report.

    Returns:
    - bool: True if successful, False otherwise.
    """
    try:
        report_data = {
            "analysis_summary": {
                "total_objects_checked": analysis_results["total_objects_checked"],
                "objects_with_issues": len(analysis_results["objects_with_issues"]),
                "unique_unexpected_attributes": len(
                    analysis_results["unexpected_attributes"]
                ),
            },
            "unexpected_attribute_names": list(
                analysis_results["unexpected_attributes"].keys()
            ),
            "detailed_findings": analysis_results["unexpected_attributes"],
            "affected_objects": analysis_results["objects_with_issues"],
        }

        with open(attribute_report_path, "w") as file:
            yaml.dump(report_data, file, default_flow_style=False)
        logger.info(f"Analysis report saved to '{attribute_report_path}'")
        return True

    except Exception as e:
        logger.error(f"Error saving analysis report: {e}")
        return False


def detect_unexpected_attribute_names(
    source_objects: List[Dict[str, Any]],
    valid_attribute_names: List[str],
    attribute_report_path: str,
) -> Dict[str, Any]:
    """
    Main function to detect unexpected attribute names with comprehensive reporting.

    Args:
    - source_objects (List[Dict[str, Any]]): A list of JSON objects to check.
    - valid_attribute_names (List[str]): A list of valid attribute names.
    - attribute_report_path (str): Path to save the detailed report.

    Returns:
    - Dict[str, Any]: Summary of findings.
    """
    # Capture unexpected attributes
    analysis_results = capture_unexpected_attributes(
        source_objects, valid_attribute_names
    )

    # If unexpected attributes found, enhance with fuzzy matching
    if analysis_results["unexpected_attributes"]:
        add_fuzzy_matching(
            analysis_results["unexpected_attributes"], valid_attribute_names
        )

        # Log findings
        logger.warning(
            f"Found {len(analysis_results['unexpected_attributes'])} unexpected attribute names "
            f"across {len(analysis_results['objects_with_issues'])} objects"
        )

        for attr_name, details in analysis_results["unexpected_attributes"].items():
            logger.warning(
                f"Unexpected attribute: '{attr_name}' (found {details['count']} times)"
            )
            if details.get("similar_valid_names"):
                logger.info(
                    f"  Possible matches: {', '.join(details['similar_valid_names'])}"
                )

        # Save detailed report
        save_analysis_report(analysis_results, attribute_report_path)
    else:
        logger.info("No unexpected attribute names detected.")

    return {
        "total_objects_checked": analysis_results["total_objects_checked"],
        "total_attributes_checked": analysis_results["total_attributes_checked"],
        "objects_with_issues": len(analysis_results["objects_with_issues"]),
        "unique_unexpected_attributes": len(analysis_results["unexpected_attributes"]),
    }