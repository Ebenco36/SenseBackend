"""
Table Reflector - Dynamically create models from existing database tables
PRODUCTION VERSION: Proper caching, logging, error handling
✅ UPDATED: Preserves exact table names, simpler approach
"""

from typing import Dict, Any, List, Optional, Type
from sqlalchemy import inspect, MetaData, Table
import logging

logger = logging.getLogger(__name__)


class TableReflector:
    """
    Reflect existing database tables and generate SQLAlchemy models dynamically.
    
    Features:
    - ✅ Exact table names preserved (all_db → all_db, NOT AllDb)
    - ✅ Proper caching to prevent duplicate model creation
    - ✅ Comprehensive logging
    - ✅ Full error handling
    - ✅ Model introspection
    """
    
    def __init__(self, db_instance):
        """Initialize reflector"""
        try:
            self.db = db_instance
            
            if not self.db:
                raise RuntimeError("Database instance is None")
            
            if not hasattr(self.db, 'engine'):
                raise RuntimeError("Database not properly initialized")
            
            _ = self.db.engine
            
            self.metadata = MetaData()
            self._reflected_models: Dict[str, Type] = {}
            
            logger.info("✅ TableReflector initialized successfully")
        
        except Exception as e:
            logger.error(f"❌ TableReflector initialization failed: {str(e)}", exc_info=True)
            raise RuntimeError(f"Database not properly initialized: {str(e)}")
    
    def get_engine(self):
        """Get database engine"""
        try:
            if not hasattr(self.db, 'engine') or self.db.engine is None:
                raise RuntimeError("Database engine not available")
            return self.db.engine
        except Exception as e:
            logger.error(f"Failed to get database engine: {str(e)}")
            raise RuntimeError(f"Database engine not available: {str(e)}")
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in database"""
        try:
            engine = self.get_engine()
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            logger.info(f"Found {len(tables)} tables in database: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Failed to get tables: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to get tables from database: {str(e)}")
    
    def reflect_table(self, table_name: str) -> Type:
        """
        Reflect a table from database and create a SQLAlchemy model.
        
        ✅ USES EXACT TABLE NAME (no CamelCase conversion)
        ✅ CRITICAL: Uses cache to prevent duplicate model creation
        
        Args:
            table_name: Name of the table to reflect
            
        Returns:
            SQLAlchemy model class with exact table name
        """
        try:
            # ✅ CHECK CACHE FIRST
            if table_name in self._reflected_models:
                logger.debug(f"Using cached model for table: {table_name}")
                return self._reflected_models[table_name]
            
            logger.info(f"Reflecting table: {table_name}")
            
            # Verify table exists
            engine = self.get_engine()
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            if table_name not in tables:
                raise ValueError(f"Table '{table_name}' does not exist in database")
            
            # ✅ Create model using simple approach
            model_class = self._create_model(table_name)
            
            # ✅ Store in cache immediately with exact table name as key
            self._reflected_models[table_name] = model_class
            
            logger.info(f"✅ Reflected and cached model: {model_class.__name__} -> {table_name}")
            return model_class
        
        except ValueError as e:
            logger.error(f"❌ Reflection failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error reflecting '{table_name}': {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to reflect table '{table_name}': {str(e)}")
    
    def _create_model(self, table_name: str) -> Type:
        """
        Create a SQLAlchemy model by reflecting a single table.
        
        ✅ Uses exact table name as class name (all_db → all_db)
        
        Args:
            table_name: Table name to reflect
            
        Returns:
            Model class with exact table name
        """
        try:
            engine = self.get_engine()
            
            # Create metadata for this table only
            metadata = MetaData()
            
            # Reflect the table
            table = Table(
                table_name,
                metadata,
                autoload_with=engine,
                extend_existing=True
            )
            
            # ✅ Create a dynamic model class with exact table name
            class_dict = {
                '__tablename__': table_name,
                '__table__': table,
                '__module__': 'dynamic_models'
            }
            
            # Use exact table name as class name
            model_class = type(table_name, (self.db.Model,), class_dict)
            
            logger.debug(f"Created model class: {model_class.__name__} for table: {table_name}")
            return model_class
        
        except Exception as e:
            logger.error(f"Failed to create model for '{table_name}': {str(e)}", exc_info=True)
            raise
    
    def reflect_many(self, table_names: List[str]) -> Dict[str, Type]:
        """Reflect multiple tables"""
        results = {}
        failed = []
        
        for table_name in table_names:
            try:
                results[table_name] = self.reflect_table(table_name)
            except Exception as e:
                logger.warning(f"Failed to reflect '{table_name}': {str(e)}")
                failed.append((table_name, str(e)))
        
        if failed:
            logger.warning(f"Failed to reflect {len(failed)} tables: {failed}")
        
        return results
    
    def reflect_all_tables(self) -> Dict[str, Type]:
        """Reflect all tables in database"""
        try:
            tables = self.get_tables()
            logger.info(f"Reflecting all {len(tables)} tables")
            return self.reflect_many(tables)
        except Exception as e:
            logger.error(f"Failed to reflect all tables: {str(e)}", exc_info=True)
            return {}
    
    def get_table_info(self, table_name: str) -> dict:
        """Get information about a table"""
        try:
            inspector = inspect(self.db.engine)
            
            columns = inspector.get_columns(table_name)
            column_names = [col['name'] for col in columns]
            column_types = {col['name']: str(col['type']) for col in columns}
            
            pk = inspector.get_pk_constraint(table_name)
            primary_key = pk['constrained_columns'] if pk['constrained_columns'] else None
            
            info = {
                'name': table_name,
                'columns': column_names,
                'column_types': column_types,
                'primary_key': primary_key,
                'total_columns': len(column_names)
            }
            
            logger.debug(f"Table info for {table_name}: {len(column_names)} columns")
            return info
        
        except Exception as e:
            logger.error(f"Failed to get table info: {e}", exc_info=True)
            raise
    
    def print_table_info(self, table_name: str) -> None:
        """Print detailed information about a table"""
        try:
            info = self.get_table_info(table_name)
            
            if not info:
                print(f"Could not retrieve info for table '{table_name}'")
                return
            
            print(f"\n{'='*80}")
            print(f"Table: {info['name']}")
            print(f"Total Columns: {info['total_columns']}")
            print(f"Primary Key: {info['primary_key']}")
            print(f"{'='*80}\n")
            
            print(f"{'Column':<40} {'Type':<20}")
            print("-" * 60)
            
            for col_name, col_type in info['column_types'].items():
                print(f"{col_name:<40} {col_type:<20}")
            
            print(f"\n{'='*80}\n")
        
        except Exception as e:
            logger.error(f"Error printing table info: {str(e)}")
    
    def print_all_tables(self) -> None:
        """Print information about all tables in database"""
        try:
            tables = self.get_tables()
            
            print(f"\n{'='*80}")
            print(f"Database Tables ({len(tables)} total)")
            print(f"{'='*80}\n")
            
            for i, table_name in enumerate(tables, 1):
                try:
                    info = self.get_table_info(table_name)
                    col_count = info.get('total_columns', 0)
                    print(f"{i:3d}. {table_name:<40} ({col_count} columns)")
                except Exception as e:
                    logger.warning(f"Failed to print info for '{table_name}': {str(e)}")
                    print(f"{i:3d}. {table_name:<40} (ERROR)")
            
            print(f"\n{'='*80}\n")
        
        except Exception as e:
            logger.error(f"Error printing all tables: {str(e)}")
    
    def get_cached_model(self, table_name: str) -> Optional[Type]:
        """Get cached model if exists"""
        return self._reflected_models.get(table_name)
    
    def clear_cache(self) -> None:
        """Clear all cached models"""
        self._reflected_models.clear()
        logger.info("TableReflector cache cleared")
    
    def get_cache_status(self) -> Dict[str, Any]:
        """Get cache status"""
        return {
            'cached_models': len(self._reflected_models),
            'tables': list(self._reflected_models.keys())
        }
    
    def __repr__(self) -> str:
        """String representation"""
        cache_info = self.get_cache_status()
        return f"<TableReflector with {cache_info['cached_models']} cached models: {cache_info['tables']}>"