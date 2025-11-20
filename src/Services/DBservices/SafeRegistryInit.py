# src/Services/DBservices/SafeRegistryInit.py

"""
Safe Registry Initialization - FIXED VERSION
- Handles TableReflector validation
- Won't crash on missing tables
- Comprehensive error handling
"""

from typing import List, Optional, Dict, Any
from sqlalchemy import inspect
import logging

from src.models_.ModelRegistry import ModelRegistry

logger = logging.getLogger(__name__)


class MockRegistry:
    """
    Mock registry when real registry can't be created
    Allows app to start without crashing
    """
    def __init__(self):
        self.models = {}
        self.reflector = None
    
    def reflect(self, table_name):
        """No-op reflect"""
        pass


def initialize_registry_safely(app, table_names: List[str] = None) -> Dict[str, Any]:
    """
    Initialize ModelRegistry safely without crashing
    
    Handles cases where db.engine might not be ready
    """
    
    try:
        from database.db import db
        
        logger.info("Starting safe registry initialization")
        
        # Verify db is initialized
        if not db or not hasattr(db, 'engine'):
            logger.error("Database not properly initialized before registry init")
            logger.info("Returning mock registry to allow app to start")
            
            return {
                'success': False,
                'registry': MockRegistry(),
                'reflected_count': 0,
                'failed': ['Database engine not available'],
                'message': 'Database engine not ready, using mock registry'
            }
        
        # Check if engine is accessible
        try:
            _ = db.engine
        except RuntimeError as e:
            logger.error(f"Database engine access failed: {str(e)}")
            logger.info("Returning mock registry to allow app to start")
            
            return {
                'success': False,
                'registry': MockRegistry(),
                'reflected_count': 0,
                'failed': ['Database engine not accessible'],
                'message': 'Database engine not accessible, using mock registry'
            }
        
        
        
        try:
            registry = ModelRegistry(db)
        except Exception as e:
            logger.error(f"Failed to create ModelRegistry: {str(e)}", exc_info=True)
            logger.info("Returning mock registry to allow app to start")
            
            return {
                'success': False,
                'registry': MockRegistry(),
                'reflected_count': 0,
                'failed': [str(e)],
                'message': f'Failed to create registry: {str(e)}'
            }
        
        # Get list of tables to reflect
        if table_names is None:
            table_names = _get_available_tables(db)
            logger.info(f"Auto-detected {len(table_names)} tables in database")
        else:
            logger.info(f"Reflecting specified tables: {table_names}")
        
        reflected_count = 0
        failed_tables = []
        
        # Reflect each table safely
        for table_name in table_names:
            try:
                if not _table_exists(db, table_name):
                    message = f"Table '{table_name}' does not exist"
                    failed_tables.append(message)
                    logger.warning(message)
                    continue
                
                registry.reflect(table_name)
                reflected_count += 1
                logger.info(f"Reflected table: {table_name}")
            
            except Exception as e:
                message = f"Failed to reflect '{table_name}': {str(e)}"
                failed_tables.append(message)
                logger.error(message, exc_info=True)
                continue
        
        logger.info("=" * 80)
        logger.info(f"Registry initialization summary:")
        logger.info(f"  - Tables requested: {len(table_names)}")
        logger.info(f"  - Tables reflected: {reflected_count}")
        logger.info(f"  - Tables failed: {len(failed_tables)}")
        if reflected_count > 0:
            logger.info(f"  - Available models: {', '.join(registry.models.keys())}")
        logger.info("=" * 80)
        
        return {
            'success': reflected_count > 0,
            'registry': registry,
            'reflected_count': reflected_count,
            'failed': failed_tables,
            'message': f"Registry initialized with {reflected_count}/{len(table_names)} tables"
        }
    
    except Exception as e:
        logger.error(f"Fatal error in registry initialization: {str(e)}", exc_info=True)
        logger.info("Returning mock registry to allow app to start")
        
        return {
            'success': False,
            'registry': MockRegistry(),
            'reflected_count': 0,
            'failed': [str(e)],
            'message': f'Registry initialization failed: {str(e)}'
        }


def _get_available_tables(db) -> List[str]:
    """Get all available tables from database"""
    try:
        inspector = inspect(db.engine)
        tables = inspector.get_table_names()
        logger.info(f"Found {len(tables)} tables in database")
        return tables
    
    except Exception as e:
        logger.error(f"Failed to get table list: {str(e)}", exc_info=True)
        return []


def _table_exists(db, table_name: str) -> bool:
    """Check if table exists in database"""
    try:
        inspector = inspect(db.engine)
        return table_name in inspector.get_table_names()
    
    except Exception as e:
        logger.error(f"Error checking table '{table_name}': {str(e)}")
        return False


def validate_registry(registry) -> Dict[str, Any]:
    """Validate registry is properly initialized"""
    try:
        if not registry:
            return {'valid': False, 'errors': ['Registry is None']}
        
        models = getattr(registry, 'models', {})
        
        if not isinstance(models, dict):
            return {'valid': False, 'errors': ['Registry models is not a dictionary']}
        
        return {
            'valid': True,
            'errors': [],
            'models': list(models.keys())
        }
    
    except Exception as e:
        logger.error(f"Registry validation failed: {str(e)}")
        return {'valid': False, 'errors': [str(e)]}