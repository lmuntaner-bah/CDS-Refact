import json
from src.utils.logger import setup_module_logger, send_alert, log_performance

# Initialize logger for this module
logger = setup_module_logger(__file__)


@log_performance
def get_source_objects(data_path: str):
    """
    Get the source objects from the data folder and store them as a list of dictionaries.
    """
    try:
        with open(data_path, "r") as f:
            source_objects = json.load(f)
        
        if not isinstance(source_objects, list):
            error_msg = "Source objects should be a list of dictionaries."
            logger.error(error_msg)
            
            send_alert("json_extractor", "ERROR", error_msg, {
                "file_path": data_path, 
                "data_type": type(source_objects).__name__
            })
            return []
        
        logger.info(f"Source objects loaded successfully. Count: {len(source_objects)}")
        return source_objects
    
    except FileNotFoundError as e:
        error_msg = f"Source objects file not found: {data_path}"
        logger.error(error_msg)
        
        send_alert("json_extractor", "ERROR", error_msg, {
            "file_path": data_path, 
            "exception": str(e)
        })
        return []
    
    except json.JSONDecodeError as e:
        error_msg = f"Error decoding JSON from source objects file: {data_path}"
        logger.error(f"{error_msg} - {str(e)}")
        
        send_alert("json_extractor", "CRITICAL", error_msg, {
            "file_path": data_path, 
            "json_error": str(e),
            "line_number": getattr(e, 'lineno', 'unknown'),
            "column": getattr(e, 'colno', 'unknown')
        })
        return []
    
    except Exception as e:
        error_msg = f"Unexpected error loading source objects from: {data_path}"
        logger.critical(f"{error_msg} - {str(e)}")
        
        send_alert("json_extractor", "CRITICAL", error_msg, {
            "file_path": data_path, 
            "exception_type": type(e).__name__,
            "exception": str(e)
        })
        return []