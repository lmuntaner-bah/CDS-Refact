from typing import Dict, Any, List
from src.utils.logger import setup_module_logger, send_alert, log_performance


# Initialize logger for this module
logger = setup_module_logger(__file__)


def extract_ism(acm: dict) -> dict:
    """Extract the reduced 'ism' structure from any ACM dict."""
    return {
        "banner": acm.get("banner"),
        "classification": acm.get("classif"),
        "ownerProducer": acm.get("owner_prod"),
        "releaseableTo": acm.get("rel_to"),
        'disseminationControls': acm.get("dissem_ctrls"),
    }


@log_performance
def build_standard_object(target_structure: Dict[str, Any], attr_index: Dict[str, Dict], attribute_map: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    """
    Build a standard object by mapping attributes from the source data to target fields.
    
    This function takes a pre-initialized target_structure dictionary and populates it with
    transformed attribute values based on the provided attribute mapping. Each
    attribute value is wrapped with ISM classification metadata.
    
    Args:
        target_structure (Dict[str, Any]): Pre-initialized dictionary containing basic object metadata
            and empty containers (ontology, maritimeMetadata, equipment, facility)
        
        attr_index (Dict[str, Dict]): Index of attribute data items keyed by attribute name,
            where each item contains 'attributeValue' and 'acm' fields
        
        attribute_map (Dict[str, Dict[str, str]]): Mapping configuration where keys are attribute names and values are dicts with
            'field' and 'container' specifications
    
    Returns:
        Dict[str, Any]: The populated target_structure dictionary with mapped attributes organized
            into their designated containers, or empty dict if an error occurs
    
    Note:
        - Attributes mapped to "root" container are placed directly in the target_structure dict
        - Other containers are nested under their respective keys
        - Each mapped value includes the original value and ISM classification metadata
        - Missing attributes in attr_index are silently skipped
    """
    try:
        for attr_name, mapping in attribute_map.items():
            item = attr_index.get(attr_name)
            
            if not item:
                continue
            
            target_field = mapping["field"]
            container = mapping["container"]
            
            transformed_value = {
                "value": item.get("attributeValue"),
                "ism": extract_ism(item.get("acm", {}))
            }
            
            if container == "root":
                target_structure[target_field] = transformed_value
            else:
                # Ensure nested container exists
                if container not in target_structure:
                    target_structure[container] = {}
                target_structure[container][target_field] = transformed_value
        
        return target_structure
    except Exception as e:
        error_msg = "Unexpected error building standard object"
        logger.critical(f"{error_msg} - {str(e)}")
        
        send_alert("data_parsing", "CRITICAL", error_msg, {
            "exception_type": type(e).__name__,
            "exception": str(e)
        })
        return {}


@log_performance
def transform_source_object(source: Dict[str, Any], attribute_map: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    """
    Transform a source object into a structured format based on the provided attribute mapping.
    
    Args:
        source: The source dictionary containing object data with attributes, ACM, and metadata
        attribute_map: Dictionary mapping attribute names to their target field and container locations
    
    Returns:
        Dict containing the transformed object with structured fields including:
        - Basic metadata (version, id, name, etc.)
        - Overall classification from ACM
        - Mapped attributes organized into appropriate containers (root, ontology, maritimeMetadata, facility)
    """
    try:
        # Check if source is a valid dictionary
        if not isinstance(source, dict):
            error_msg = "Source object should be a dictionary."
            logger.error(error_msg)
            
            send_alert("data_parsing", "ERROR", error_msg, {
                "data_type": type(source).__name__
            })
            return {}
        
        # Check that attribute_map is provided
        if not attribute_map or not isinstance(attribute_map, dict):
            logger.error("Attribute map is not provided or is not a valid dictionary.")
            return {}
        
        target_structure = {
            "version": source.get("version"),
            "overallClassification": extract_ism(source.get("acm", {})),
            "id": source.get("id"),
            "name": source.get("name"),
            "lastUpdatedDate": source.get("lastVerified", {}).get("timestamp"),
            "excerciseIndicator": source.get("gide_id"),
        }
        
        # Pre-initialize containers
        target_structure["maritimeMetadata"] = {}
        target_structure["ontology"] = {}
        target_structure["equipment"] = {}
        target_structure["facility"] = {}
        
        # Build quick index for attributes.data
        data_items = source.get("attributes", {}).get("data", [])
        attr_index = {item.get("attributeName"): item for item in data_items}
        
        standard_object = build_standard_object(target_structure, attr_index, attribute_map)
        
        return standard_object
    except Exception as e:
        error_msg = "Unexpected error transforming object"
        logger.critical(f"{error_msg} - {str(e)}")
        
        send_alert("data_parsing", "CRITICAL", error_msg, {
            "exception_type": type(e).__name__,
            "exception": str(e)
        })
        return {}


def remove_empty_containers(obj: Dict[str, Any], container_keys: List[str]) -> Dict[str, Any]:
    """
    Remove empty containers from a standard object.
    
    This function removes any container dictionaries that are empty, helping to 
    clean up the object structure and reduce noise in the final output.
    
    Args:
        obj (Dict[str, Any]): The standard object with potentially empty containers
        container_keys (List[str]): List of keys representing containers to check for emptiness
    
    Returns:
        Dict[str, Any]: The cleaned object with empty containers removed
    """
    try:
        # Create a copy of the object to avoid modifying the original
        cleaned_obj = obj.copy()
        
        # Remove empty containers
        for container_key in container_keys:
            if container_key in cleaned_obj:
                container = cleaned_obj[container_key]
                # Remove if container is empty dict or None
                if not container or (isinstance(container, dict) and len(container) == 0):
                    del cleaned_obj[container_key]
        
        return cleaned_obj
    except Exception as e:
        error_msg = "Unexpected error removing empty containers"
        logger.critical(f"{error_msg} - {str(e)}")
        
        send_alert("data_parsing", "CRITICAL", error_msg, {
            "exception_type": type(e).__name__,
            "exception": str(e)
        })
        return {}