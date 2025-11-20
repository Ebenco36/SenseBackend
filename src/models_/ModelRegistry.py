# src/Models/ModelRegistry.py
"""
Model Registry - Central registry for all models (reflected or defined)
Provides unified access to models throughout the application
"""

from typing import Any, Dict, Type, Optional, List
from database.db import db
from src.Services.DBservices.TableReflector import TableReflector
import logging

logger = logging.getLogger(__name__)


class ModelRegistry:
    """
    Central registry for managing both defined and reflected models.
    
    This pattern allows:
    - Single source of truth for all models
    - Dynamic table reflection
    - Model caching
    - Easy switching between defined and reflected models
    - Unified CRUD operations through repositories
    
    Example:
        # Initialize registry
        registry = ModelRegistry(db)
        
        # Register manually defined models
        registry.register(User)
        registry.register(Profile)
        
        # Auto-reflect tables from database
        registry.reflect_many(['publications', 'studies'])
        
        # Access models
        User = registry.get('User')
        User = registry.get_by_table('users')
        
        # Create repository for any model
        user_repo = registry.get_repository('users')
    """
    
    def __init__(self, db_instance):
        """
        Initialize model registry.
        
        Args:
            db_instance: Flask-SQLAlchemy instance
        """
        self.db = db_instance
        self.reflector = TableReflector(db_instance)
        
        #  PUBLIC attributes (not private)
        self.models: Dict[str, Type] = {}  # Map class_name -> model
        self._table_map: Dict[str, str] = {}  # Map table_name -> class_name
        self._repositories: Dict[str, Any] = {}  # Cache repositories
        
        logger.info("ModelRegistry initialized")
    
    def register(self, model_class: Type, table_name: Optional[str] = None) -> None:
        """
        Register a manually defined model.
        
        Args:
            model_class: Model class
            table_name: Optional table name (inferred from __tablename__ if not provided)
        """
        try:
            class_name = model_class.__name__
            table = table_name or getattr(model_class, '__tablename__', class_name.lower())
            
            self.models[class_name] = model_class
            self._table_map[table] = class_name
            
            logger.info(f" Registered model: {class_name} -> {table}")
        
        except Exception as e:
            logger.error(f"Failed to register model: {str(e)}", exc_info=True)
            raise
    
    def reflect(self, table_name: str) -> Type:
        """
        Reflect and register a table from database.
        
        Args:
            table_name: Table name to reflect
            
        Returns:
            Reflected model class
        """
        try:
            logger.info(f"Reflecting table: {table_name}")
            
            model_class = self.reflector.reflect_table(table_name)
            class_name = model_class.__name__
            
            self.models[class_name] = model_class
            self._table_map[table_name] = class_name
            
            logger.info(f" Reflected model: {class_name} -> {table_name}")
            return model_class
        
        except Exception as e:
            logger.error(f"Failed to reflect '{table_name}': {str(e)}", exc_info=True)
            raise
    
    def reflect_many(self, table_names: List[str]) -> Dict[str, Type]:
        """
        Reflect and register multiple tables.
        
        Args:
            table_names: List of table names
            
        Returns:
            Dictionary of reflected models
        """
        models = {}
        failed = []
        
        for table_name in table_names:
            try:
                models[table_name] = self.reflect(table_name)
            except Exception as e:
                logger.warning(f"Failed to reflect '{table_name}': {str(e)}")
                failed.append(table_name)
        
        if failed:
            logger.warning(f"Failed to reflect {len(failed)} tables: {failed}")
        
        return models
    
    def reflect_all(self) -> Dict[str, Type]:
        """
        Reflect and register all tables from database.
        
        Returns:
            Dictionary of all reflected models
        """
        try:
            tables = self.reflector.get_tables()
            logger.info(f"Found {len(tables)} tables to reflect")
            return self.reflect_many(tables)
        
        except Exception as e:
            logger.error(f"Failed to reflect all tables: {str(e)}", exc_info=True)
            return {}
    
    def get(self, class_name: str) -> Optional[Type]:
        """
        Get model by class name.
        
        Args:
            class_name: Model class name (e.g., 'User')
            
        Returns:
            Model class or None if not found
        """
        return self.models.get(class_name)
    
    def get_or_fail(self, class_name: str) -> Type:
        """
        Get model by class name or raise exception.
        
        Args:
            class_name: Model class name
            
        Returns:
            Model class
            
        Raises:
            ValueError: If model not found
        """
        model = self.get(class_name)
        if not model:
            available = ', '.join(self.models.keys()) if self.models else 'None'
            raise ValueError(
                f"Model '{class_name}' not found in registry. "
                f"Available: {available}"
            )
        return model
    
    def get_by_table(self, table_name: str) -> Optional[Type]:
        """
        Get model by table name.
        
        Args:
            table_name: Table name (e.g., 'users')
            
        Returns:
            Model class or None if not found
        """
        class_name = self._table_map.get(table_name)
        return self.get(class_name) if class_name else None
    
    def get_by_table_or_fail(self, table_name: str) -> Type:
        """
        Get model by table name or raise exception.
        
        Args:
            table_name: Table name
            
        Returns:
            Model class
            
        Raises:
            ValueError: If table/model not found
        """
        model = self.get_by_table(table_name)
        if not model:
            available = ', '.join(self._table_map.keys()) if self._table_map else 'None'
            raise ValueError(
                f"Table '{table_name}' not found in registry. "
                f"Available tables: {available}"
            )
        return model
    
    def exists(self, class_name: str) -> bool:
        """Check if model exists in registry"""
        return class_name in self.models
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table model exists in registry"""
        return table_name in self._table_map
    
    def get_all(self) -> Dict[str, Type]:
        """Get all registered models"""
        return self.models.copy()
    
    def get_all_tables(self) -> Dict[str, str]:
        """Get all table mappings"""
        return self._table_map.copy()
    
    def get_repository(self, model_or_table: str):
        """
        Get repository for a model (cached).
        
        Args:
            model_or_table: Class name or table name
            
        Returns:
            BaseRepository instance
            
        Example:
            repo = registry.get_repository('users')  # By table name
            repo = registry.get_repository('User')    # By class name
        """
        try:
            from src.Services.DBservices.BaseRepository import BaseRepository
            
            # Determine which model we're looking for
            if model_or_table in self.models:
                model_class = self.get(model_or_table)
                cache_key = model_or_table
                logger.debug(f"Getting repository by class name: {model_or_table}")
            
            elif model_or_table in self._table_map:
                model_class = self.get_by_table(model_or_table)
                cache_key = model_or_table
                logger.debug(f"Getting repository by table name: {model_or_table}")
            
            else:
                available_classes = ', '.join(self.models.keys())
                available_tables = ', '.join(self._table_map.keys())
                raise ValueError(
                    f"Model or table '{model_or_table}' not found in registry.\n"
                    f"Available classes: {available_classes}\n"
                    f"Available tables: {available_tables}"
                )
            
            # Return cached repository or create new one
            if cache_key not in self._repositories:
                self._repositories[cache_key] = BaseRepository(model_class)
                logger.debug(f"Created new repository for: {cache_key}")
            else:
                logger.debug(f"Using cached repository for: {cache_key}")
            
            return self._repositories[cache_key]
        
        except Exception as e:
            logger.error(f"Failed to get repository: {str(e)}", exc_info=True)
            raise
    
    def print_registry(self) -> None:
        """Print all registered models and tables"""
        try:
            print(f"\n{'='*80}")
            print(f"Model Registry ({len(self.models)} models)")
            print(f"{'='*80}\n")
            
            if not self.models:
                print("No models registered\n")
                return
            
            for i, class_name in enumerate(sorted(self.models.keys()), 1):
                # Find corresponding table
                table_name = next(
                    (t for t, c in self._table_map.items() if c == class_name),
                    'N/A'
                )
                print(f"{i:3d}. {class_name:<30} -> {table_name:<30}")
            
            print(f"\n{'='*80}\n")
        
        except Exception as e:
            logger.error(f"Error printing registry: {str(e)}")
    
    def print_model_details(self, class_name: str) -> None:
        """Print details about a specific model"""
        try:
            model_class = self.get_or_fail(class_name)
            table_name = next(
                (t for t, c in self._table_map.items() if c == class_name),
                None
            )
            
            if table_name:
                self.reflector.print_table_info(table_name)
            else:
                logger.warning(f"No table name found for model: {class_name}")
        
        except Exception as e:
            logger.error(f"Error printing model details: {str(e)}", exc_info=True)
    
    def clear(self) -> None:
        """Clear all registered models"""
        self.models.clear()
        self._table_map.clear()
        self._repositories.clear()
        logger.info("ModelRegistry cleared")
    
    def __repr__(self) -> str:
        """String representation"""
        return f"<ModelRegistry with {len(self.models)} models>"