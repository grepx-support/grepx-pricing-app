"""
Configuration Loader - Loads tasks and assets from YAML/JSON config files
"""
import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigLoader:
    """Loads configuration from YAML or JSON files"""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize config loader
        
        Args:
            config_dir: Directory containing config files. Defaults to resources/config/
        """
        if config_dir is None:
            # Default to resources/config relative to this file
            config_dir = Path(__file__).parent.parent / "resources" / "config"
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)
    
    def load_config(self, filename: str = "tasks_config.yaml") -> Dict[str, Any]:
        """
        Load configuration from YAML or JSON file
        
        Args:
            filename: Config file name (supports .yaml, .yml, .json)
        
        Returns:
            Configuration dictionary
        """
        config_path = self.config_dir / filename
        
        if not config_path.exists():
            # Create default config if it doesn't exist
            self._create_default_config(config_path)
            print(f"Created default config file: {config_path}")
        
        if filename.endswith('.json'):
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or {}
    
    def _create_default_config(self, config_path: Path):
        """Create a default configuration file"""
        default_config = {
            'celery_tasks': [],
            'assets': [],
            'resources': [],
            'schedules': [],
            'sensors': []
        }
        
        if config_path.suffix == '.json':
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=2)
        else:
            with open(config_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False, sort_keys=False)
    
    def save_config(self, config: Dict[str, Any], filename: str = "tasks_config.yaml"):
        """
        Save configuration to file
        
        Args:
            config: Configuration dictionary
            filename: Config file name
        """
        config_path = self.config_dir / filename
        
        if filename.endswith('.json'):
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
        else:
            with open(config_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)

