import json
import os
from typing import Dict, Any, List
from pathlib import Path

class ConfigManager:
    def __init__(self):
        self.config_dir = Path.home() / ".db_populator"
        self.config_file = self.config_dir / "config.json"
        self.connections_file = self.config_dir / "connections.json"
        self._ensure_config_dir()
    
    def _ensure_config_dir(self):
        """Asegura que el directorio de configuración exista"""
        self.config_dir.mkdir(exist_ok=True)
        
        # Crear archivos de configuración si no existen
        if not self.config_file.exists():
            self._create_default_config()
        
        if not self.connections_file.exists():
            self._create_default_connections()
    
    def _create_default_config(self):
        """Crea configuración por defecto"""
        default_config = {
            "defaults": {
                "record_count": 100,
                "max_retries": 3,
                "batch_size": 1000,
                "show_progress": True
            },
            "data_generation": {
                "use_realistic_data": True,
                "prefer_unique_values": True,
                "max_unique_attempts": 100
            }
        }
        self.save_config(default_config)
    
    def _create_default_connections(self):
        """Crea archivo de conexiones vacío"""
        self.save_connections({})
    
    def get_config(self) -> Dict[str, Any]:
        """Obtiene la configuración actual"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            self._create_default_config()
            return self.get_config()
    
    def save_config(self, config: Dict[str, Any]):
        """Guarda la configuración"""
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
    
    def get_connections(self) -> Dict[str, str]:
        """Obtiene las conexiones guardadas"""
        try:
            with open(self.connections_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            self._create_default_connections()
            return {}
    
    def save_connection(self, name: str, connection_string: str):
        """Guarda una conexión"""
        connections = self.get_connections()
        connections[name] = connection_string
        self.save_connections(connections)
    
    def save_connections(self, connections: Dict[str, str]):
        """Guarda todas las conexiones"""
        with open(self.connections_file, 'w', encoding='utf-8') as f:
            json.dump(connections, f, indent=2, ensure_ascii=False)
    
    def delete_connection(self, name: str) -> bool:
        """Elimina una conexión"""
        connections = self.get_connections()
        if name in connections:
            del connections[name]
            self.save_connections(connections)
            return True
        return False
    
    def export_config(self, file_path: str):
        """Exporta configuración y conexiones"""
        export_data = {
            "config": self.get_config(),
            "connections": self.get_connections(),
            "export_date": str(os.path.getctime(self.config_file))
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
    
    def import_config(self, file_path: str) -> bool:
        """Importa configuración y conexiones"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                import_data = json.load(f)
            
            if "config" in import_data:
                self.save_config(import_data["config"])
            
            if "connections" in import_data:
                self.save_connections(import_data["connections"])
            
            return True
        except Exception as e:
            print(f"❌ Error importando configuración: {e}")
            return False