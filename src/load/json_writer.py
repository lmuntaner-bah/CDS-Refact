import json
from datetime import datetime
from typing import Dict, Any, List
from src.utils.config_loader import get_output_path
from src.utils.logger import setup_module_logger, send_alert


# Initialize logger for this module
logger = setup_module_logger(__file__)


def save_standard_objects(cleaned_objects: List[Dict[str, Any]]) -> None:
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
    
    output_path = None
    
    try:
        output_path = get_output_path("processed_dir")
    except OSError as e:
        error_msg = f"Failed to access output directory {output_path}: {e}"
        logger.error(error_msg)
        
        send_alert("json_writer", "ERROR", error_msg, {
            "file_path": output_path,
            "exception_type": type(e).__name__,
            "exception": str(e)
        })
        raise
    
    # Generate timestamp for this batch
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for i, obj in enumerate(cleaned_objects):
        try:
            # Get object ID, fallback to index if ID is missing
            obj_id = obj.get("id", f"object_{i}")
            
            # Create filename with object ID and timestamp
            filename = f"{obj_id}_{timestamp}.json"
            file_path = output_path / filename
            
            # Ensure the object is JSON serializable
            if not isinstance(obj, dict):
                error_msg = "Object is not a valid dictionary"
                logger.error(error_msg)
                
                send_alert("json_writer", "ERROR", error_msg, {
                    "data_type": type(obj).__name__,
                    "exception": "ValueError"
                })
                raise ValueError(f"Object {i} is not a valid dictionary")
            
            # Write JSON file with proper formatting
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(obj, f, indent=2, ensure_ascii=False, sort_keys=True)
            
            logger.info(f"Successfully saved object {obj_id} to {filename}")
        
        except (ValueError, TypeError) as e:
            error_msg = f"Object {i} serialization error"
            logger.error(error_msg)
            
            send_alert("json_writer", "ERROR", error_msg, {
                "data_type": type(obj).__name__,
                "exception": type(e).__name__,
                "exception_message": str(e)
            })
            raise
        
        except OSError as e:
            error_msg = f"File write error for object {i}"
            logger.error(error_msg)
            
            send_alert("json_writer", "ERROR", error_msg, {
                "file_path": str(output_path),
                "exception_type": type(e).__name__,
                "exception": str(e)
            })
            raise
        
        except Exception as e:
            error_msg = f"Unexpected error saving object {i}"
            logger.critical(error_msg)
            
            send_alert("json_writer", "CRITICAL", error_msg, {
                "data_type": type(obj).__name__,
                "exception": type(e).__name__,
                "exception_message": str(e)
            })
            raise