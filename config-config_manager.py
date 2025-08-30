from abc import ABC, abstractmethod
import json
import yaml
from typing import Dict, Any

class ConfigManager(ABC):
    """Abstract base class for configuration management"""
    
    @abstractmethod
    def load_config(self, config_path: str) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def save_config(self, config: Dict[str, Any], config_path: str) -> None:
        pass

class JSONConfigManager(ConfigManager):
    """Concrete implementation for JSON config management"""
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def save_config(self, config: Dict[str, Any], config_path: str) -> None:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)

class YAMLConfigManager(ConfigManager):
    """Concrete implementation for YAML config management"""
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def save_config(self, config: Dict[str, Any], config_path: str) -> None:
        with open(config_path, 'w') as f:
            yaml.dump(config, f)