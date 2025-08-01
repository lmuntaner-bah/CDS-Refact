import sys
from pathlib import Path
from loguru import logger
from datetime import datetime
import functools
from src.utils.config_loader import get_output_path

def setup_module_logger(file_path: str, log_level: str = "INFO"):
    """
    Set up a logger for a specific module based on its file path.
    
    Args:
        file_path (str): The __file__ variable from the calling module
        log_level (str): Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    
    Returns:
        logger: Configured logger instance
    """
    # Extract module name from file path
    module_name = Path(file_path).stem
    
    # Get the logs directory from the output path configuration
    logs_dir = get_output_path("log_dir")
    
    # Remove default logger to avoid duplicates
    logger.remove()
    
    # Console logging with colors
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{extra[module]}</cyan> | <level>{message}</level>",
        level=log_level,
        colorize=True
    )
    
    # Module-specific log file
    log_file = logs_dir / f"{module_name}.log"
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[module]} | {message}",
        level=log_level,
        rotation="10 MB",
        retention="30 days",
        compression="zip",
        backtrace=True,
        diagnose=True
    )
    
    # Error-specific log file
    error_log_file = logs_dir / f"{module_name}_errors.log"
    logger.add(
        error_log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[module]} | {message}",
        level="ERROR",
        rotation="5 MB",
        retention="60 days",
        compression="zip",
        backtrace=True,
        diagnose=True
    )
    
    # Bind the module name to the logger
    return logger.bind(module=module_name)


def send_alert(module_name: str, level: str, message: str, details: dict | None = None):
    """
    Send an alert for critical issues.
    
    Args:
        module_name (str): Name of the module sending the alert
        level (str): Alert level ('WARNING', 'ERROR', 'CRITICAL')
        message (str): Alert message
        details (dict): Additional details about the alert
    """
    alert_data = {
        "timestamp": datetime.now().isoformat(),
        "module": module_name,
        "level": level,
        "message": message,
        "details": details or {}
    }
    
    # Create alert log file
    logs_dir = get_output_path("log_dir")
    alert_file = logs_dir / "alerts.log"
    
    logger.add(
        alert_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | ALERT | {level: <8} | {message}",
        level="WARNING",
        rotation="5 MB",
        retention="90 days"
    )
    
    alert_message = f"ALERT in {module_name}: {message} | Details: {alert_data}"
    
    if level == "CRITICAL":
        logger.critical(alert_message)
    elif level == "ERROR":
        logger.error(alert_message)
    else:
        logger.warning(alert_message)
    
    return alert_data


def log_performance(func):
    """
    Decorator to log function performance.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        import time
        
        module_name = Path(func.__module__.replace('.', '/')).stem if hasattr(func, '__module__') else 'unknown'
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"{func.__name__} executed in {execution_time:.4f} seconds")
            
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.4f} seconds: {str(e)}")
            
            raise
    
    return wrapper