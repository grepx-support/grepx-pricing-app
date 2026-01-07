"""Config loader."""
import os
from omegaconf import OmegaConf
from pathlib import Path

def _load_config():
    """Load config."""
    config_file = Path(__file__).parent.parent / "resources" / "config.yaml"
    
    if not config_file.exists():
        raise FileNotFoundError(f"Config not found: {config_file}")
    
    cfg = OmegaConf.load(config_file)
    
    # Override with env vars
    if "CELERY_BROKER_URL" in os.environ:
        cfg.celery.broker_url = os.environ["CELERY_BROKER_URL"]
    if "CELERY_RESULT_BACKEND" in os.environ:
        cfg.celery.result_backend = os.environ["CELERY_RESULT_BACKEND"]
    
    return cfg
