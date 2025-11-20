# src/Services/ApplicationService.py

from typing import Dict, Any, Optional, List
from src.Services.BaseService import BaseService
from src.Services.FilterService import FilterService
import logging

logger = logging.getLogger(__name__)


class ApplicationService(BaseService):
    """Main application service"""
    
    def __init__(self, db, filter_structure: Dict = None, registry=None):
        """
        Initialize application service
        
        Args:
            db: Flask-SQLAlchemy instance
            filter_structure: Filter structure dict
            registry: ModelRegistry instance ✅
        """
        super().__init__('ApplicationService')
        self.db = db
        self.registry = registry  # ✅ Store registry
        self.filter_service = FilterService(registry=registry)  # ✅ Pass registry
        self.log_info("Application service initialized")
    
    def validate_input(self, data):
        """Validate input"""
        return True, None
    
    def initialize(self, tables: list = None) -> Dict:
        """Initialize application"""
        try:
            self.log_info("Starting application initialization")
            
            if not self.registry:
                self.log_error("Registry not available")
                return self.create_response(False, errors=['Registry not initialized'])
            
            # Verify tables are loaded
            if tables:
                missing = []
                for table_name in tables:
                    if not self.registry.table_exists(table_name):
                        missing.append(table_name)
                
                if missing:
                    self.log_warning(f"Missing tables: {missing}")
                    return self.create_response(False, errors=[f"Missing tables: {missing}"])
                
                self.log_info(f"✅ Verified {len(tables)} tables loaded")
            
            # Get models
            models = self.registry.get_all()
            self.log_info(f"✅ Registry has {len(models)} models")
            
            self.log_info("✅ Application fully initialized")
            return self.create_response(
                True,
                data={
                    'status': 'ready',
                    'models_count': len(models),
                    'models': list(models.keys())
                },
                message='Application initialized successfully'
            )
        
        except Exception as e:
            self.log_error("Application initialization failed", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def get_filters(self) -> Dict:
        """Get all filters ✅ Uses FilterService with registry"""
        try:
            return self.filter_service.get_all_filters()
        except Exception as e:
            self.log_error("Failed to get filters", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def get_filter_tree(self) -> Dict:
        """Get filter tree ✅ Uses FilterService with registry"""
        try:
            filters = self.filter_service.get_all_filters()
            return self.create_response(True, data=filters)
        except Exception as e:
            self.log_error("Failed to get filter tree", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def get_available_tables(self) -> Dict:
        """Get available tables"""
        try:
            if not self.registry:
                return self.create_response(False, errors=['Registry not available'])
            
            tables = list(self.registry.models.keys())
            return self.create_response(
                True,
                data={'tables': tables, 'total': len(tables)}
            )
        except Exception as e:
            self.log_error("Failed to get available tables", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def get_health_status(self) -> Dict:
        """Get application health status"""
        try:
            if not self.registry:
                return self.create_response(
                    True,
                    data={
                        'status': 'degraded',
                        'database': 'unknown',
                        'registry': False,
                        'models': []
                    }
                )
            
            return self.create_response(
                True,
                data={
                    'status': 'healthy',
                    'database': 'connected',
                    'registry': True,
                    'models': list(self.registry.models.keys())
                },
                message='Application is healthy'
            )
        
        except Exception as e:
            self.log_error("Health check failed", exception=e)
            return self.create_response(False, errors=[str(e)])