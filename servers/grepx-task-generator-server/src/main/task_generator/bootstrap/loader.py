# bootstrap/loader.py
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger("task-generator")


def load_config(seed_dir: Path) -> List[Dict]:
    """Load the task-generator.yaml configuration file."""
    config_file = seed_dir / "task-generator.yaml"
    
    if not config_file.exists():
        logger.warning(f"Configuration file not found: {config_file}")
        return []
    
    with open(config_file, "r") as f:
        config = yaml.safe_load(f) or {}
    
    tables = config.get("tables", [])
    logger.info(f"Loaded configuration for {len(tables)} tables")
    
    return tables


def load_table_data(seed_dir: Path, table_name: str, file_list: List[str]) -> List[Dict]:
    """Load specified YAML files from a table's folder."""
    table_dir = seed_dir / table_name
    
    if not table_dir.exists() or not table_dir.is_dir():
        logger.debug(f"Table directory not found: {table_dir}")
        return []
    
    if not file_list:
        logger.debug(f"No files specified for table '{table_name}'")
        return []
    
    logger.info(f"Loading {len(file_list)} file(s) for table '{table_name}'")
    
    all_rows = []
    for filename in file_list:
        file_path = table_dir / filename
        
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            continue
        
        logger.debug(f"Loading {filename} for table '{table_name}'")
        
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
        
        if isinstance(data, list):
            all_rows.extend(data)
            logger.debug(f"Loaded {len(data)} rows from {filename}")
        else:
            logger.warning(f"Skipping {filename}: expected list, got {type(data).__name__}")
    
    return all_rows


def load_seed_files(seed_dir: Path) -> List[Tuple[str, List[Dict]]]:
    """
    Load seed data for all configured tables.
    
    Returns:
        List of tuples: (table_name, rows)
    """
    tables_config = load_config(seed_dir)
    
    if not tables_config:
        logger.error("No tables configured in task-generator.yaml")
        return []
    
    for table_config in tables_config:
        table_name = table_config.get("name")
        file_list = table_config.get("files", [])
        
        if not table_name:
            logger.warning(f"Table config missing 'name': {table_config}")
            continue
        
        rows = load_table_data(seed_dir, table_name, file_list)
        
        if rows:
            logger.info(f"Loaded {len(rows)} total rows for table '{table_name}'")
            yield table_name, rows
        else:
            logger.debug(f"No data found for table '{table_name}'")
