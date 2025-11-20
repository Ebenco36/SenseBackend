# # src/Services/ApplicationService.py

# from typing import Dict, Any, Optional, List
# from src.Services.BaseService import BaseService
# from src.Services.DBservices.RecordProcessor import RecordProcessor
# from src.Services.FilterService import FilterService
# import logging
# # from src.record_processor import RecordProcessor

# logger = logging.getLogger(__name__)


# class ApplicationService(BaseService):
#     """Main application service"""
    
#     def __init__(self, db, filter_structure: Dict = None, registry=None):
#         """
#         Initialize application service
        
#         Args:
#             db: Flask-SQLAlchemy instance
#             filter_structure: Filter structure dict
#             registry: ModelRegistry instance 
#         """
#         super().__init__('ApplicationService')
#         self.db = db
#         self.registry = registry  # Store registry
#         self.filter_service = FilterService(registry=registry)  # Pass registry
#         self.log_info("Application service initialized")
#         self.record_processor = RecordProcessor()
    
#     def validate_input(self, data):
#         """Validate input"""
#         return True, None
    
#     def initialize(self, tables: list = None) -> Dict:
#         """Initialize application"""
#         try:
#             self.log_info("Starting application initialization")
            
#             if not self.registry:
#                 self.log_error("Registry not available")
#                 return self.create_response(False, errors=['Registry not initialized'])
            
#             # Verify tables are loaded
#             if tables:
#                 missing = []
#                 for table_name in tables:
#                     if not self.registry.table_exists(table_name):
#                         missing.append(table_name)
                
#                 if missing:
#                     self.log_warning(f"Missing tables: {missing}")
#                     return self.create_response(False, errors=[f"Missing tables: {missing}"])
                
#                 self.log_info(f"Verified {len(tables)} tables loaded")
            
#             # Get models
#             models = self.registry.get_all()
#             self.log_info(f"Registry has {len(models)} models")
            
#             self.log_info("Application fully initialized")
#             return self.create_response(
#                 True,
#                 data={
#                     'status': 'ready',
#                     'models_count': len(models),
#                     'models': list(models.keys())
#                 },
#                 message='Application initialized successfully'
#             )
        
#         except Exception as e:
#             self.log_error("Application initialization failed", exception=e)
#             return self.create_response(False, errors=[str(e)])
    
#     def get_filters(self) -> Dict:
#         """Get all filters Uses FilterService with registry"""
#         try:
#             return self.filter_service.get_all_filters()
#         except Exception as e:
#             self.log_error("Failed to get filters", exception=e)
#             return self.create_response(False, errors=[str(e)])
    
#     def get_filter_tree(self) -> Dict:
#         """Get filter tree Uses FilterService with registry"""
#         try:
#             filters = self.filter_service.get_all_filters()
#             return self.create_response(True, data=filters)
#         except Exception as e:
#             self.log_error("Failed to get filter tree", exception=e)
#             return self.create_response(False, errors=[str(e)])
    
#     def get_available_tables(self) -> Dict:
#         """Get available tables"""
#         try:
#             if not self.registry:
#                 return self.create_response(False, errors=['Registry not available'])
            
#             tables = list(self.registry.models.keys())
#             return self.create_response(
#                 True,
#                 data={'tables': tables, 'total': len(tables)}
#             )
#         except Exception as e:
#             self.log_error("Failed to get available tables", exception=e)
#             return self.create_response(False, errors=[str(e)])
    
#     def get_health_status(self) -> Dict:
#         """Get application health status"""
#         try:
#             if not self.registry:
#                 return self.create_response(
#                     True,
#                     data={
#                         'status': 'degraded',
#                         'database': 'unknown',
#                         'registry': False,
#                         'models': []
#                     }
#                 )
            
#             return self.create_response(
#                 True,
#                 data={
#                     'status': 'healthy',
#                     'database': 'connected',
#                     'registry': True,
#                     'models': list(self.registry.models.keys())
#                 },
#                 message='Application is healthy'
#             )
        
#         except Exception as e:
#             self.log_error("Health check failed", exception=e)
#             return self.create_response(False, errors=[str(e)])





# src/Services/ApplicationService.py

from typing import Dict, Any, Optional, List
from src.Services.BaseService import BaseService
from src.Services.DBservices.RecordProcessor import RecordProcessor
from src.Services.FilterService import FilterService
import logging

logger = logging.getLogger(__name__)


class ApplicationService(BaseService):
    """
    Main application service with automatic artificial column injection.
    All record retrieval operations automatically include research_notes, topic_notes, notes.
    """
    
    def __init__(self, db, filter_structure: Dict = None, registry=None):
        """
        Initialize application service
        
        Args:
            db: Flask-SQLAlchemy instance
            filter_structure: Filter structure dict
            registry: ModelRegistry instance
        """
        super().__init__('ApplicationService')
        self.db = db
        self.registry = registry
        self.filter_service = FilterService(registry=registry)
        self.record_processor = RecordProcessor(include_empty=False)  # Centralized processor
        self.log_info("Application service initialized with RecordProcessor")
    
    def validate_input(self, data):
        """Validate input"""
        return True, None
    
    # ==================== INITIALIZATION ====================
    
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
                
                self.log_info(f"Verified {len(tables)} tables loaded")
            
            models = self.registry.get_all()
            self.log_info(f"Registry has {len(models)} models")
            self.log_info("Application fully initialized")
            
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
    
    # ==================== FILTER OPERATIONS ====================
    
    def get_filters(self) -> Dict:
        """Get all filters"""
        try:
            return self.filter_service.get_all_filters()
        except Exception as e:
            self.log_error("Failed to get filters", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def get_filter_tree(self) -> Dict:
        """Get filter tree"""
        try:
            filters = self.filter_service.get_all_filters()
            return self.create_response(True, data=filters)
        except Exception as e:
            self.log_error("Failed to get filter tree", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    # ==================== RECORD OPERATIONS WITH ARTIFICIAL COLUMNS ====================
    
    def get_all_records(self, table_name: str, **kwargs) -> Dict:
        """
        Get all records with artificial columns.
        
        Automatically adds research_notes, topic_notes, notes to all records.
        """
        try:
            if not self.registry or not self.registry.table_exists(table_name):
                return self.create_response(
                    False, 
                    errors=[f'Table {table_name} not found']
                )
            
            model = self.registry.get_model(table_name)
            records = self.db.session.query(model).all()
            
            # Serialize records
            serialized = [self._serialize_record(r) for r in records]
            
            # Add artificial columns
            serialized = self.record_processor.add_artificial_columns(serialized)
            
            self.log_info(f"Retrieved {len(serialized)} records from {table_name}")
            
            return self.create_response(
                True,
                data=serialized,
                message=f'Retrieved {len(serialized)} records'
            )
        
        except Exception as e:
            self.log_error(f"Error getting records from {table_name}", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def get_record_by_id(self, table_name: str, record_id: int) -> Dict:
        """
        Get single record by ID with artificial columns.
        
        Automatically adds research_notes, topic_notes, notes.
        """
        try:
            if not self.registry or not self.registry.table_exists(table_name):
                return self.create_response(
                    False,
                    errors=[f'Table {table_name} not found']
                )
            
            model = self.registry.get_model(table_name)
            record = self.db.session.query(model).filter_by(primary_id=record_id).first()
            
            if not record:
                return self.create_response(
                    False,
                    errors=[f'Record {record_id} not found']
                )
            
            # Serialize record
            serialized = self._serialize_record(record)
            
            # Add artificial columns (as single-item list)
            serialized = self.record_processor.add_artificial_columns([serialized])[0]
            
            self.log_info(f"Retrieved record {record_id} from {table_name}")
            
            return self.create_response(
                True,
                data=serialized,
                message='Record retrieved successfully'
            )
        
        except Exception as e:
            self.log_error(f"Error getting record {record_id}", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def search_records(self, table_name: str, search_params: Dict) -> Dict:
        """
        Search records with artificial columns.
        
        Automatically adds research_notes, topic_notes, notes to results.
        """
        try:
            if not self.registry or not self.registry.table_exists(table_name):
                return self.create_response(
                    False,
                    errors=[f'Table {table_name} not found']
                )
            
            model = self.registry.get_model(table_name)
            query = self.db.session.query(model)
            
            # Apply search filters (simplified - extend as needed)
            if 'filters' in search_params:
                for field, value in search_params['filters'].items():
                    if hasattr(model, field):
                        query = query.filter(getattr(model, field).ilike(f'%{value}%'))
            
            records = query.all()
            
            # Serialize records
            serialized = [self._serialize_record(r) for r in records]
            
            # Add artificial columns
            serialized = self.record_processor.add_artificial_columns(serialized)
            
            self.log_info(f"Search returned {len(serialized)} records from {table_name}")
            
            return self.create_response(
                True,
                data=serialized,
                message=f'Found {len(serialized)} records'
            )
        
        except Exception as e:
            self.log_error(f"Error searching {table_name}", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def create_record(self, table_name: str, data: Dict) -> Dict:
        """
        Create new record.
        
        Note: Artificial columns are computed on retrieval, not stored.
        """
        try:
            if not self.registry or not self.registry.table_exists(table_name):
                return self.create_response(
                    False,
                    errors=[f'Table {table_name} not found']
                )
            
            model = self.registry.get_model(table_name)
            new_record = model(**data)
            
            self.db.session.add(new_record)
            self.db.session.commit()
            
            # Get created record with artificial columns
            serialized = self._serialize_record(new_record)
            serialized = self.record_processor.add_artificial_columns([serialized])[0]
            
            self.log_info(f"Created record in {table_name}")
            
            return self.create_response(
                True,
                data=serialized,
                message='Record created successfully'
            )
        
        except Exception as e:
            self.db.session.rollback()
            self.log_error(f"Error creating record in {table_name}", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def update_record(self, table_name: str, record_id: int, data: Dict) -> Dict:
        """
        Update existing record.
        
        Returns updated record with artificial columns.
        """
        try:
            if not self.registry or not self.registry.table_exists(table_name):
                return self.create_response(
                    False,
                    errors=[f'Table {table_name} not found']
                )
            
            model = self.registry.get_model(table_name)
            record = self.db.session.query(model).filter_by(primary_id=record_id).first()
            
            if not record:
                return self.create_response(
                    False,
                    errors=[f'Record {record_id} not found']
                )
            
            # Update fields
            for key, value in data.items():
                if hasattr(record, key):
                    setattr(record, key, value)
            
            self.db.session.commit()
            
            # Get updated record with artificial columns
            serialized = self._serialize_record(record)
            serialized = self.record_processor.add_artificial_columns([serialized])[0]
            
            self.log_info(f"Updated record {record_id} in {table_name}")
            
            return self.create_response(
                True,
                data=serialized,
                message='Record updated successfully'
            )
        
        except Exception as e:
            self.db.session.rollback()
            self.log_error(f"Error updating record {record_id}", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def delete_record(self, table_name: str, record_id: int) -> Dict:
        """Delete record by ID"""
        try:
            if not self.registry or not self.registry.table_exists(table_name):
                return self.create_response(
                    False,
                    errors=[f'Table {table_name} not found']
                )
            
            model = self.registry.get_model(table_name)
            record = self.db.session.query(model).filter_by(primary_id=record_id).first()
            
            if not record:
                return self.create_response(
                    False,
                    errors=[f'Record {record_id} not found']
                )
            
            self.db.session.delete(record)
            self.db.session.commit()
            
            self.log_info(f"Deleted record {record_id} from {table_name}")
            
            return self.create_response(
                True,
                message='Record deleted successfully'
            )
        
        except Exception as e:
            self.db.session.rollback()
            self.log_error(f"Error deleting record {record_id}", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    # ==================== UTILITY METHODS ====================
    
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
                        'models': [],
                        'record_processor': False
                    }
                )
            
            return self.create_response(
                True,
                data={
                    'status': 'healthy',
                    'database': 'connected',
                    'registry': True,
                    'models': list(self.registry.models.keys()),
                    'record_processor': True
                },
                message='Application is healthy'
            )
        
        except Exception as e:
            self.log_error("Health check failed", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def _serialize_record(self, record) -> Dict:
        """
        Serialize SQLAlchemy model to dict.
        
        Helper method for converting database records to JSON-serializable format.
        """
        from datetime import datetime
        
        result = {}
        for column in record.__table__.columns:
            value = getattr(record, column.name)
            if isinstance(value, datetime):
                result[column.name] = value.isoformat()
            else:
                result[column.name] = value
        return result
