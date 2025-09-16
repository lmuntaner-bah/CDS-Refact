from loguru import logger
from typing import Dict, Any, Optional


def is_classif_too_high(ism: Dict[str, Any], config: Dict[str, Any]) -> bool:
    """
    Determines if the classification level in the ISM (Information Security Marking) is too high or contains forbidden values.

    Args:
        ism (Dict[str, Any]): The ISM dictionary containing classification information.
        config (Dict[str, Any]): Configuration dictionary with forbidden values:
            - "forbidden_sci": List of forbidden SCI controls.
            - "forbidden_controls": List of forbidden dissemination controls.
            - "forbidden_terms": List of forbidden terms to check in the banner.

    Returns:
        bool: True if the ISM classification is too high or contains forbidden values, False otherwise.
    """
    if not ism:
        logger.warning("ISM is empty, cannot determine classification level.")
        return False

    if (
        ism.get("classification") == "TS"
        or set(ism.get("sciControls", [])) & set(config["forbidden_sci"])
        or set(ism.get("disseminationControls", [])) & set(config["forbidden_controls"])
        or any(
            term in ism.get("banner", "").upper() for term in config["forbidden_terms"]
        )
    ):
        # logger.warning(f"ISM too high or contains forbidden values: {ism}")
        return True

    return False


def is_more_restrictive(ism1: Dict[str, Any], ism2: Dict[str, Any], config: Dict[str, Any]) -> bool:
    """
    Determines if the first ISM (Information Security Marking) is more restrictive than the second.
    
    The function evaluates restrictiveness based on a hierarchy of security controls:
    1. FGI (Foreign Government Information) controls - presence makes marking more restrictive
    2. NOFORN dissemination controls - presence makes marking more restrictive  
    3. REL (Releasable To) controls - fewer releasable groups/entities makes marking more restrictive
    4. Classification level - higher classification level makes marking more restrictive
    
    Args:
        ism1 (Dict[str, Any]): First ISM dictionary containing security marking information.
            Expected keys: "sciControls", "disseminationControls", "releasableTo", "classification"
        ism2 (Dict[str, Any]): Second ISM dictionary containing security marking information.
            Expected keys: "sciControls", "disseminationControls", "releasableTo", "classification"
        config (Dict[str, Any]): Configuration dictionary containing:
            - "special_groups": List of special group identifiers
            - "classifications": Dict mapping classification levels to numeric values
    
    Returns:
        bool: True if ism1 is more restrictive than ism2, False otherwise.
              Returns False if either ism1 or ism2 is empty/None.
    """
    if not ism1 or not ism2:
        return False

    # FGI controls
    ism1_fgi = any(c.startswith("FGI") for c in ism1.get("sciControls", []))
    ism2_fgi = any(c.startswith("FGI") for c in ism2.get("sciControls", []))
    
    if ism1_fgi != ism2_fgi:
        return ism1_fgi

    # NOFORN
    ism1_noforn = "NOFORN" in ism1.get("disseminationControls", [])
    ism2_noforn = "NOFORN" in ism2.get("disseminationControls", [])
    
    if ism1_noforn != ism2_noforn:
        return ism1_noforn

    # REL controls
    rel1 = "REL" in ism1.get("disseminationControls", [])
    rel2 = "REL" in ism2.get("disseminationControls", [])
    
    if rel1 and rel2:
        ism1_release = set(ism1.get("releasableTo", []))
        ism2_release = set(ism2.get("releasableTo", []))
        ism1_groups = ism1_release & set(config["special_groups"])
        ism2_groups = ism2_release & set(config["special_groups"])
        
        # More restrictive if ism1 has fewer groups or fewer releasable entities
        if ism1_groups != ism2_groups:
            return len(ism1_groups) < len(ism2_groups)
        return len(ism1_release) < len(ism2_release)

    # Classification hierarchy
    classif1 = ism1.get("classification", "U")
    classif2 = ism2.get("classification", "U")
    
    return config["classifications"].get(classif1, 0) > config["classifications"].get(
        classif2, 0
    )


def find_most_restrictive_valid_ism(
    obj: Dict[str, Any], config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Traverse a nested object to find the most restrictive valid ISM (Information Security Marking).

    The function searches through all dictionaries and lists within the provided object,
    identifies ISMs that are not too highly classified (using is_classif_too_high),
    and returns the most restrictive valid ISM according to the is_more_restrictive function.

    Args:
        obj (Dict[str, Any]): The object to search for ISMs.
        config (Dict[str, Any]): The classification configuration dictionary.

    Returns:
        Optional[Dict[str, Any]]: The most restrictive valid ISM found, or None if no valid ISM exists.
    """
    most_restrictive = None
    stack = [obj]  # Use a stack to traverse the object hierarchy

    while stack:
        item = stack.pop()

        if isinstance(item, dict):
            # Check if the current item has an ISM and if it's valid
            if "ism" in item:
                ism = item.get("ism")
                if ism and not is_classif_too_high(ism, config):
                    # Early exit if 'TS' found
                    if ism.get("classification") == "TS":
                        return ism.copy()
                    if most_restrictive is None or is_more_restrictive(
                        ism, most_restrictive, config
                    ):
                        most_restrictive = ism.copy()

            # Add all dictionary values to the stack
            stack.extend(item.values())
        elif isinstance(item, list):
            # Add all list items to the stack
            stack.extend(item)
    
    return most_restrictive


def apply_restrictions(
    standard_object: Dict[str, Any], config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Recursively process a standard object to remove or redact data that is too highly classified,
    according to the provided classification configuration.

    The function finds the most restrictive valid ISM (Information Security Marking) within the object,
    then traverses all nested dictionaries and lists, replacing any data with a classification that is
    considered too high with a placeholder. The processed object will include an 'overallClassification'
    field set to the most restrictive valid ISM found.

    Args:
        standard_object (Dict[str, Any]): The object to process and apply restrictions to.
        config (Dict[str, Any]): The classification configuration dictionary.

    Returns:
        Optional[Dict[str, Any]]: The processed object with restricted data redacted, or None if no valid ISM is found.
    """
    # Find the most restrictive valid ISM in the object
    most_restrictive_ism = find_most_restrictive_valid_ism(standard_object, config)

    if not most_restrictive_ism:
        # If no valid ISM is found, return None
        logger.warning("No valid ISM found for object")
        return None

    def process_item(item: Any) -> Any:
        if isinstance(item, dict):
            # If the item has an ISM, check if it is too high
            if "ism" in item and is_classif_too_high(item["ism"], config):
                logger.debug(f"Removing item due to high classification: {item}")
                return None

            # Process all key-value pairs in the dictionary
            return {k: process_item(v) for k, v in item.items()}
        if isinstance(item, list):
            # Process all items in the list
            return [process_item(x) for x in item]

        return item  # Return the item as is if it's neither a dict nor a list

    # Process the object and add the overall classification
    processed_object = process_item(standard_object)
    if isinstance(processed_object, dict):
        processed_object["overallClassification"] = most_restrictive_ism

    return processed_object