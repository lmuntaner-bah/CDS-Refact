from src.extract.json_extractor import get_source_objects
from src.utils.logger import setup_module_logger, log_performance

# Initialize logger for this module
logger = setup_module_logger(__file__)


@log_performance
def oms_pipeline():
    """
    Main function to run the OMS data pipeline.
    
    This function orchestrates the entire pipeline, including:
    - Extracting source objects
    - Transforming objects to standard format
    - Cleaning and validating objects
    - Saving the final output
    """
    # Step 1: Extract source objects
    source_objects = get_source_objects(data_path)
    
    
    # Step 2: Validate source objects against the schema
    
    # Step 3: Transform source objects to standard format
    attribute_mapping = load_attribute_mapping()
    standard_objects = [transform_object(obj, attribute_mapping) for obj in source_objects]
    
    # Step 4: Clean and validate the transformed objects
    cleaned_standard_objects = clean_and_validate_objects(standard_objects)
    
    # Step 5: Save the cleaned objects to the database or file system
    save_cleaned_objects(cleaned_standard_objects)