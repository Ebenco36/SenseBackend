# src/Services/RegistryService.py

from typing import List, Optional, Dict, Any
from src.Services.BaseService import BaseService
from sqlalchemy import inspect

from src.models_.ModelRegistry import ModelRegistry


class RegistryService(BaseService):
    """
    Service for managing model registry
    
    Responsibilities:
    - Safe initialization
    - Table detection
    - Model management
    - Error recovery
    """
    
    def __init__(self, db):
        """Initialize registry service"""
        super().__init__('RegistryService')
        self.db = db
        self.registry = None
    
    def validate_input(self, data: Any) -> tuple[bool, Optional[str]]:
        """Validate input"""
        if not data:
            return False, "Input cannot be empty"
        return True, None
    
    def initialize_safely(self, tables: List[str] = None) -> Dict:
        """
        Initialize registry safely without crashing
        
        Don't reflect tables - they're already reflected!
        """
        
        try:
            self.log_info("Starting registry initialization")
            
            # Get registry (already created in SafeRegistryInit)
            # Don't call reflect() - SafeRegistryInit already did this!
            
            registry = self.registry
            
            if not registry:
                return self.create_response(False, errors=['Registry not initialized'])
            
            # Just verify tables exist
            verified_count = 0
            missing = []
            
            if tables:
                for table_name in tables:
                    if registry.table_exists(table_name):
                        verified_count += 1
                        self.log_info(f"Verified: {table_name}")
                    else:
                        missing.append(table_name)
                        self.log_warning(f"Table not found: {table_name}")
            else:
                # No specific tables requested, just check what's in registry
                all_models = registry.get_all()
                verified_count = len(all_models)
                self.log_info(f"Registry contains {verified_count} models")
            
            return self.create_response(
                True,
                data={
                    'reflected_tables': verified_count,
                    'total_tables': len(tables) if tables else verified_count,
                    'failed': missing,
                    'models': list(registry.models.keys())
                },
                message=f"Registry initialized with {verified_count} tables"
            )
        
        except Exception as e:
            self.log_error("Registry initialization failed", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def _get_available_tables(self) -> List[str]:
        """Get all available tables in database"""
        try:
            inspector = inspect(self.db.engine)
            tables = inspector.get_table_names()
            self.log_info(f"Found {len(tables)} tables in database")
            return tables
        except Exception as e:
            self.log_error("Failed to get table list", exception=e)
            return []
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists in database"""
        try:
            inspector = inspect(self.db.engine)
            return table_name in inspector.get_table_names()
        except Exception as e:
            self.log_error(f"Error checking table '{table_name}'", exception=e)
            return False
    
    def get_registry(self) -> Optional[ModelRegistry]:
        """Get initialized registry"""
        if not self.registry:
            self.log_warning("Registry not initialized")
        return self.registry
    
    def has_model(self, table_name: str) -> bool:
        """Check if model exists in registry"""
        if not self.registry:
            return False
        return table_name in self.registry.models
    
    def get_available_tables(self) -> Dict:
        """Get list of available tables"""
        if not self.registry:
            return self.create_response(False, errors=['Registry not initialized'])
        
        tables = list(self.registry.models.keys())
        self.log_info(f"Retrieved {len(tables)} available tables")
        
        return self.create_response(
            True,
            data={'tables': tables, 'total': len(tables)}
        )
    
    def get_table_columns(self, table_name: str) -> Dict:
        """Get columns for a table"""
        try:
            if not self.has_model(table_name):
                return self.create_response(
                    False,
                    errors=[f"Table '{table_name}' not found in registry"]
                )
            
            model_class = self.registry.models[table_name]
            mapper = inspect(model_class)
            
            columns = []
            for col in mapper.columns:
                columns.append({
                    'name': col.name,
                    'type': str(col.type),
                    'nullable': col.nullable,
                    'primary_key': col.primary_key
                })
            
            self.log_info(f"Retrieved {len(columns)} columns for '{table_name}'")
            return self.create_response(True, data={'columns': columns, 'total': len(columns)})
        
        except Exception as e:
            self.log_error(f"Failed to get columns for '{table_name}'", exception=e)
            return self.create_response(False, errors=[str(e)])