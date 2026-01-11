"""Config loader."""
import os
from omegaconf import OmegaConf
from pathlib import Path
import re

def _load_config():
    """Load config."""
    config_file = Path(__file__).parent.parent / "resources" / "config.yaml"
    
    if not config_file.exists():
        raise FileNotFoundError(f"Config not found: {config_file}")
    
    # Read and substitute environment variables
    with open(config_file, 'r') as f:
        content = f.read()
    
    # Substitute ${VAR} format (no default needed as env.common should be loaded)
    def replace_var(match):
        var_name = match.group(1)
        value = os.getenv(var_name)
        if value is None:
            raise ValueError(f"Environment variable {var_name} not set. Ensure env.common is loaded.")
        return value
    
    content = re.sub(r'\$\{([^}]+)\}', replace_var, content)
    
    cfg = OmegaConf.create(content)
    
    # Override with env vars if explicitly set
    if "CELERY_BROKER_URL" in os.environ:
        cfg.celery.broker_url = os.environ["CELERY_BROKER_URL"]
    if "CELERY_RESULT_BACKEND" in os.environ:
        cfg.celery.result_backend = os.environ["CELERY_RESULT_BACKEND"]
    if "GREPX_MASTER_DB_URL" in os.environ:
        cfg.tasks.database.uri = os.environ["GREPX_MASTER_DB_URL"]
    
    return cfg
