"""
Configuration Loader for Dagster Framework
"""
import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigLoader:
    """Loads configuration from YAML file"""
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize config loader
        
        Args:
            config_path: Path to config.yaml file. Defaults to resources/config.yaml
        """
        if config_path is None:
            config_path = Path(__file__).parent / "resources" / "config.yaml"
        self.config_path = Path(config_path)
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file with environment variable substitution
        
        Returns:
            Configuration dictionary
        """
        if not self.config_path.exists():
            return self._get_default_config()
        
        with open(self.config_path, 'r') as f:
            content = f.read()
        
        # Simple environment variable substitution: ${VAR:default}
        content = self._substitute_env_vars(content)
        
        config = yaml.safe_load(content) or {}
        return config
    
    def _substitute_env_vars(self, content: str) -> str:
        """Substitute environment variables in format ${VAR} or ${VAR:default}"""
        import re
        
        def replace_var(match):
            var_expr = match.group(1)
            if ':' in var_expr:
                var_name, default = var_expr.split(':', 1)
                return os.getenv(var_name, default)
            else:
                value = os.getenv(var_expr)
                if value is None:
                    raise ValueError(f"Environment variable {var_expr} not set. Ensure env.common is loaded.")
                return value
        
        pattern = r'\$\{([^}]+)\}'
        return re.sub(pattern, replace_var, content)
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration if config file doesn't exist"""
        return {
            'app': {
                'name': 'grepx-dagster-server',
                'version': '1.0.0'
            },
            'dagster': {
                'home': './.dagster_home'
            },
            'database': {
                'db_url': 'sqlite:///./dagster_config_orm.db'
            },
            'celery': {
                'broker_url': 'redis://localhost:6379/0',
                'result_backend': 'redis://localhost:6379/0'
            }
        }
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation (e.g., 'dagster.home')
        
        Args:
            key_path: Dot-separated path to config value
            default: Default value if key not found
        
        Returns:
            Configuration value or default
        """
        config = self.load_config()
        keys = key_path.split('.')
        value = config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value

