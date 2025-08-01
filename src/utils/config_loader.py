import yaml
from pathlib import Path
from functools import lru_cache
from src.utils.logger import setup_module_logger

# Initialize logger for this module
logger = setup_module_logger(__file__)


CONFIG_FILE = Path("../config/config.yaml")

@lru_cache(maxsize=1)
def load_config():
    with open(CONFIG_FILE, "r") as f:
        return yaml.safe_load(f)

def get_base_path():
    cfg = load_config()
    return Path(cfg["project_dir"])

def get_input_path(key: str) -> Path:
    cfg = load_config()
    return get_base_path() / cfg["input_files"][key]

def get_output_path(key: str) -> Path:
    cfg = load_config()
    return get_base_path() / cfg["output_files"][key]