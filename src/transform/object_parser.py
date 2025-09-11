
from loguru import logger
from datetime import datetime
from typing import Dict, Any, List, Optional
from src.transform.preprocessor import preprocess_raw_data
from src.transform.classif_restrictor import apply_restrictions


def is_empty_container(container: Any) -> bool:
    """
    Check if a container is empty.

    Args:
        container (Any): The container to check

    Returns:
        bool: True if the container is empty or None, False otherwise
    """
    try:
        if container is None:
            return True
        if isinstance(container, dict):
            return all(is_empty_container(value) for value in container.values())
        if isinstance(container, list):
            return all(is_empty_container(item) for item in container)
        return False  # For other types, consider them non-empty if they are not None
    except Exception as e:
        logger.error(f"Error checking if container is empty: {e}")
        raise ValueError("Invalid container structure")


def fix_container_types(objects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fix container types to match schema requirements.

    Converts dict containers to arrays where required by the schema.
    Currently handles: provenance (dict -> array)

    Args:
        objects: List of standard objects to fix

    Returns:
        List of objects with corrected container types
    """
    # Define containers that should be arrays in the schema
    array_containers = {"location", "equipment", "provenance"}  # Add more as needed

    for obj in objects:
        for container_name in array_containers:
            if container_name in obj and isinstance(obj[container_name], dict):
                # Convert dict to array format
                if obj[container_name]:  # If not empty dict
                    obj[container_name] = [obj[container_name]]
                else:  # If empty dict
                    obj[container_name] = []

    return objects


def clean_object(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove empty containers from the object.

    Args:
        obj: The object to clean

    Returns:
        The cleaned object with empty containers removed
    """
    try:
        if isinstance(obj, dict):
            # Use dictionary comprehension to clean nested dictionaries
            return {
                key: clean_object(value)
                for key, value in obj.items()
                if not is_empty_container(value)
            }
        elif isinstance(obj, list):
            # Use list comprehension to clean nested lists
            return [clean_object(item) for item in obj if not is_empty_container(item)]
        else:
            # return non-container value as is.
            return obj
    except Exception as e:
        logger.error(f"Error cleaning object: {e}")
        raise ValueError("An error occurred while cleaning the object")


def extract_ism(acm: dict) -> dict:
    """Extract the reduced 'ism' structure from any ACM dict."""
    return {
        "banner": acm.get("banner"),
        "classification": acm.get("classif"),
        "ownerProducer": acm.get("owner_prod"),
        "releaseableTo": acm.get("rel_to"),
        "disseminationControls": acm.get("dissem_ctrls"),
    }


def extract_created_date(source_object: Dict[str, Any]) -> Optional[int]:
    """
    Extracts the 'createdDate' (Unix timestamp) from the source object's attributes.

    Args:
        source_object (Dict[str, Any]): The source dictionary containing object data.

    Returns:
        Optional[int]: The Unix timestamp of 'Date Of Introduction' if found, otherwise None.
    """
    try:
        for attr in source_object.get("attributes", {}).get("data", []):
            name = attr.get("attributeName", "").strip().lower()
            
            if name == "date of introduction":
                value = attr.get("attributeValue")
                if isinstance(value, int):
                    return value
    except Exception as e:
        logger.error(f"There was an error extracting createdDate: {e}")
    return None


def extract_elevation(source_object: Dict[str, Any]) -> Optional[Any]:
    """
    Retrieves the elevation value from the source object, handling variations
    in the attribute name (e.g., "Elevation", "Elevation(m)", "Elevation (m)").

    Args:
        source_object (Dict[str, Any]): The source JSON-like object.

    Returns:
        Optional[Any]: The elevation value if found, otherwise None.
    """
    elevation_value = None

    # Define possible variations of the "Elevation" attribute name
    elevation_variations = ["elevation", "elevation(m)", "elevation (m)"]

    try:
        # Ensure the source object is a dictionary and contains the expected structure
        if not isinstance(source_object, dict):
            raise ValueError("source object must be a dictionary.")

        if (
            "attributes" not in source_object
            or "data" not in source_object["attributes"]
        ):
            raise KeyError(
                "source object does not contain the expected 'attributes.data' structure."
            )

        # Iterate through the attributes to find the elevation value
        for attr in source_object["attributes"]["data"]:
            attribute_name = attr.get("attributeName", "").lower()

            if (
                attribute_name in elevation_variations
                and attr.get("attributeValue") is not None
            ):
                elevation_value = attr.get("attributeValue")
                break  # Exit the loop once the elevation value is found

        if isinstance(elevation_value, str):
            try:
                elevation_value = float(elevation_value)
            except Exception as e:
                logger.error(f"Error transforming elevation into float: {e}")
    except Exception as e:
        # Log the exception for debugging purposes
        logger.error(f"Error occurred while retrieving elevation: {e}")

    return elevation_value


def prepare_attribute_index(source: Dict[str, Any]) -> Dict[str, Dict]:
    """
    Prepare a complete attribute index including both standard attributes and top-level fields.

    Args:
        source: Source dictionary containing object data

    Returns:
        Dict[str, Dict]: Attribute index with both regular attributes and transformed top-level fields
    """
    # Get standard attributes from data items
    data_items = source.get("attributes", {}).get("data", [])
    attr_index = {item.get("attributeName"): item for item in data_items}

    # Define top-level fields to be included in attribute mapping
    top_level_fields = {
        "domain": "Domain",
        "allegience": "Allegience",
        "allegienceAor": "Allegience Aor",
        "eoid": "Enterprise Object ID",
    }

    # Add top-level fields to attribute index with proper structure
    for source_field, attr_name in top_level_fields.items():
        if source_field in source:
            attr_index[attr_name] = {
                "attributeValue": source[source_field],
                "acm": source.get("acm", {}),
            }

    return attr_index


def parse_location(source_object: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Processes location information from the input object's geographic data.

    Args:
        source_object (Dict[str, Any]): The input object containing location data.

    Returns:
        Dict[str, Any]: Processed location data, or None if data is invalid.
    """
    try:
        location_data = source_object.get("latestKnownLocation")
        if not location_data:
            logger.warning(
                f"No location data found for object {source_object.get('id')}"
            )
            return None

        geometry_data = location_data.get("geometry")
        if not geometry_data:
            logger.warning(
                f"No geometry data found for object {source_object.get('id')}"
            )
            return None

        # Enhance coordinate validation - accept arrays with 2 or more values
        coords = geometry_data.get("coordinates", [])
        if not coords or len(coords) < 2:
            logger.warning(f"Invalid coordinates for object {source_object.get('id')}")
            return None

        if len(coords) > 2:
            logger.warning(
                f"Using first two values from {len(coords)}-element coordinate array for object {source_object.get('id')}."
            )

        elevation_value = extract_elevation(source_object)

        return {
            "ism": extract_ism(location_data.get("acm", {})),
            "id": location_data.get("id"),
            "timestamp": location_data.get("lastVerified", {}).get("timestamp"),
            "latitude": coords[1],
            "longitude": coords[0],
            "altitude": {
                "value": None,
                "quality": None,
                "error": None,
                "units": {"value": None},
            },
            "elevation": {
                "value": elevation_value,
                "quality": None,
                "error": None,
                "units": {"value": None},
            },
            "derivation": geometry_data.get("type"),
            "quality": None,
            "locationName": None,
        }
    except Exception as e:
        logger.error(
            f"Unexpected error writing location values for object {source_object.get('id')}: {str(e)}"
        )
        return None


def parse_ship_class_name(
    source_object: Dict[str, Any], standard_object: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Updates the standard_object's maritimeMetadata with shipClass and shipName if the object is a ship.

    Args:
        source_object (Dict[str, Any]): Input dictionary containing vessel information.
        standard_object (Dict[str, Any]): Dictionary to be updated with vessel metadata.

    Returns:
        Dict[str, Any]: The updated standard_object.
    """
    try:
        attributes = source_object.get("attributes", {}).get("data", [])
        class_name = source_object.get("className")
        acm = source_object.get("acm", {})

        # Using any() to more efficiently determine shipName-shipClass without a for loop
        is_ship = any(
            attr.get("attributeName") == "Echelon"
            and attr.get("attributeValue") == "SHIP"
            for attr in attributes
        )
        # Find shipName and its ACM from the attribute
        ship_name_attr = next(
            (attr for attr in attributes if attr.get("attributeName") == "Name"),
            None,
        )

        ship_name = ship_name_attr.get("attributeValue") if ship_name_attr else None
        ship_name_acm = ship_name_attr.get("acm", {}) if ship_name_attr else {}

        if is_ship:
            if "maritimeMetadata" not in standard_object or not isinstance(
                standard_object["maritimeMetadata"], dict
            ):
                standard_object["maritimeMetadata"] = {}

            standard_object["maritimeMetadata"]["shipClass"] = {
                "value": class_name,
                "ism": extract_ism(acm),
            }
            if ship_name:
                standard_object["maritimeMetadata"]["shipName"] = {
                    "value": ship_name,
                    "ism": extract_ism(ship_name_acm),
                }

            logger.info(
                f"Set shipClass and shipName for object {standard_object.get('id')}"
            )
        return standard_object
    except Exception as e:
        logger.error(
            f"Error parsing ship class/name for object {standard_object.get('id', 'unknown')}: {e}"
        )
        return standard_object


def parse_facility_name_id(
    source_object: Dict[str, Any], standard_object: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Updates the standard_object with facilityName and facilityId if the object represents a facility.

    Args:
        source_object (Dict[str, Any]): Dictionary containing facility information.
        standard_object (Dict[str, Any]): Dictionary to be updated with facility metadata.

    Returns:
        Dict[str, Any]: The updated standard_object.
    """
    try:
        attributes = source_object.get("attributes", {}).get("data", [])
        class_name = source_object.get("className")
        acm = source_object.get("acm", {})

        is_facility = class_name == "Facility"
        facility_name = source_object.get("name")

        # Find OSuffix attribute and its ACM
        facility_id_attr = next(
            (
                attr
                for attr in attributes
                if attr.get("attributeName") == "OSuffix"
                and attr.get("attributeValue") is not None
            ),
            None,
        )
        facility_id = (
            facility_id_attr.get("attributeValue") if facility_id_attr else None
        )
        facility_id_acm = facility_id_attr.get("acm", {}) if facility_id_attr else {}

        if is_facility:
            if "facility" not in standard_object or not isinstance(
                standard_object["facility"], dict
            ):
                standard_object["facility"] = {}

            standard_object["facility"]["facilityName"] = {
                "value": facility_name,
                "ism": extract_ism(acm),
            }
            if facility_id:
                standard_object["facility"]["facilityId"] = {
                    "value": facility_id,
                    "ism": extract_ism(facility_id_acm),
                }

            logger.info(
                f"Set facilityName and facilityId for object {standard_object.get('id')}"
            )
        return standard_object
    except Exception as e:
        logger.error(
            f"Error parsing facility name/id for object {standard_object.get('id', 'unknown')}: {e}"
        )
        return standard_object


def build_standard_object(
    target_structure: Dict[str, Any],
    attr_index: Dict[str, Dict],
    attribute_map: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
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
                "ism": extract_ism(item.get("acm", {})),
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
        logger.error(f"Error building standard object: {e}")
        return {}


def transform_source_object(
    source: Dict[str, Any], attribute_map: Dict[str, Dict[str, str]]
) -> Dict[str, Any]:
    """
    Transform a source object into a structured format based on the provided attribute mapping.

    Args:
        source: The source dictionary containing object data with attributes, ACM, and metadata
        attribute_map: Dictionary mapping attribute names to their target field and container locations

    Returns:
        Dict containing the transformed object with structured fields
    """
    try:
        if not isinstance(source, dict) or not isinstance(attribute_map, dict):
            logger.error("Invalid source object or attribute map.")
            return {}

        # Special handling for createdDate.
        created_date = extract_created_date(source)

        # Initialize target structure with basic metadata
        target_structure = {
            "version": source.get("version"),
            "overallClassification": extract_ism(source.get("acm", {})),
            "id": source.get("id"),
            "name": source.get("name"),
            "createdDate": created_date,
            "lastUpdatedDate": source.get("lastVerified", {}).get("timestamp"),
            "excerciseIndicator": source.get("gideId"),
            "location": parse_location(source),
            "maritimeMetadata": {},
            "landMetadata": {},
            "equipment": {},
            "unit": {},
            "ontology": {},
            "facility": {},
            "provenance": {},
        }

        # Get complete attribute index including top-level fields
        attr_index = prepare_attribute_index(source)

        # Build and return the standard object
        standard_object = build_standard_object(
            target_structure, attr_index, attribute_map
        )

        # Apply the bespoke functions that parse maritime and facility attributes
        parse_ship_class_name(source, standard_object)
        parse_facility_name_id(source, standard_object)

        if standard_object is not None:
            logger.info(
                f"Finished transforming object with ID: {standard_object.get('id', 'unknown')}"
            )
            return standard_object
        else:
            logger.warning(
                f"Transformation resulted in None for object with ID: {source.get('id', 'unknown')}"
            )
            return {}
    except Exception as e:
        logger.error(
            f"Error transforming object with ID {source.get('id', 'unknown')}: {e}"
        )
        return {}


def process_objects(
    source_objects: List[Dict[str, Any]],
    attribute_mapping: Dict[str, Dict[str, str]],
    restrictions_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Processes a list of input objects by parsing, validating, and applying ISM policies.

    Args:
        source_objects (List[Dict[str, Any]]): List of objects to process.
        attribute_mapping (Dict[str, Dict[str, str]]): Mapping configuration for attributes.
        restrictions_config (Dict[str, Any]): Configuration for classification restrictions.

    Returns:
        List[Dict[str, Any]]:
            - A list of processed objects.
    """
    logger.info(f"Processing total objects: {len(source_objects)}")

    # Preprocess the raw data to ensure we're working with a clean set
    preprocessed_objects = preprocess_raw_data(source_objects)
    logger.info(f"Successfully pre-processed {len(preprocessed_objects)} object(s)")

    processed_objects = []
    for obj in preprocessed_objects:  # Iterate over preprocessed raw data
        try:
            obj_id = obj.get("id")  # Ensure obj_id is extracted
            logger.info(f"Processing object with ID: {obj_id}")

            standard_obj = transform_source_object(obj, attribute_mapping)
            processed_obj = apply_restrictions(standard_obj, restrictions_config)

            if processed_obj is not None:
                processed_objects.append(processed_obj)
        except Exception as e:
            logger.error(
                f"Unexpected error processing object {obj.get('id')}: {str(e)}"
            )
    
    # Fix container types to match schema requirements
    cleaned_processed_objects = fix_container_types(processed_objects)
    logger.info("Fixed container types to match schema requirements")

    return cleaned_processed_objects