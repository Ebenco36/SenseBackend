# src/Services/ResourceService.py
"""
Base Resource Service - Provides common functionality for all resource services
Handles CRUD, search, filtering, and pagination
"""


from typing import Dict, Any, List, Optional, Type
from database import db
from src.Services.DBservices.BaseRepository import BaseRepository
from src.Services.DBservices.ModelSerializer import ModelSerializer


class ResourceService:
    """
    Base service for handling resource operations
    ⭐ CRITICAL FIX: Now actually serializes items!
    """
    
    def __init__(self, repository: BaseRepository, model_class: Type):
        """Initialize resource service"""
        self.repository = repository
        self.model_class = model_class
        self.model_name = model_class.__name__
    
    def _serialize(self, obj: Any, include_relationships: bool = False) -> Dict:
        """Serialize single object"""
        if obj is None:
            return None
        return ModelSerializer.to_dict(obj, include_relationships=include_relationships)
    
    def _serialize_list(self, items: List[Any], include_relationships: bool = False) -> List[Dict]:
        """Serialize list of objects - MUST BE CALLED!"""
        if not items:
            return []
        return [ModelSerializer.to_dict(item, include_relationships=include_relationships) for item in items]
    
    # ========================================================================
    # GET OPERATIONS
    # ========================================================================
    
    def get_all(self, page: int = 1, per_page: int = 20, order_by: Optional[str] = None, 
                include_relationships: bool = False) -> Dict[str, Any]:
        """Get all records with pagination"""
        try:
            query = self.repository.new_query()
            
            if order_by:
                query.order_by(order_by)
            
            results = query.paginate(page=page, per_page=per_page)
            
            # ⭐ CRITICAL: MUST serialize items!
            serialized_items = self._serialize_list(results.items, include_relationships=include_relationships)
            
            return {
                'success': True,
                'data': {
                    'items': serialized_items,  # ← Must be list of dicts, not objects!
                    'pagination': {
                        'total': results.total,
                        'page': results.page,
                        'per_page': results.per_page,
                        'total_pages': results.pages,
                        'has_next': results.has_next,
                        'has_prev': results.has_prev
                    }
                },
                'message': f'{self.model_name} records retrieved successfully'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Failed to retrieve {self.model_name} records'
            }
    
    def get_by_id(self, record_id: Any, include_relationships: bool = False) -> Dict[str, Any]:
        """Get single record by ID"""
        try:
            record = self.repository.find(record_id)
            
            if not record:
                return {
                    'success': False,
                    'error': f'{self.model_name} not found',
                    'message': f'{self.model_name} with ID {record_id} does not exist'
                }
            
            return {
                'success': True,
                'data': self._serialize(record, include_relationships=include_relationships),
                'message': f'{self.model_name} retrieved successfully'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Failed to retrieve {self.model_name}'
            }
    
    def get_many(self, ids: List[Any], include_relationships: bool = False) -> Dict[str, Any]:
        """Get multiple records by IDs"""
        try:
            records = self.repository.find_many(ids)
            
            return {
                'success': True,
                'data': self._serialize_list(records, include_relationships=include_relationships),
                'message': f'{len(records)} {self.model_name} records retrieved'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Failed to retrieve {self.model_name} records'
            }
    
    # ========================================================================
    # SEARCH & FILTER
    # ========================================================================
    
    def search(self, search_term: str, searchable_fields: List[str], 
               page: int = 1, per_page: int = 20, include_relationships: bool = False) -> Dict[str, Any]:
        """Search records with text search"""
        try:
            results = self.repository.search(
                search_term=search_term,
                searchable_fields=searchable_fields,
                page=page,
                per_page=per_page
            )
            
            # ⭐ CRITICAL: MUST serialize items!
            serialized_items = self._serialize_list(results.items, include_relationships=include_relationships)
            
            return {
                'success': True,
                'data': {
                    'items': serialized_items,  # ← Must be list of dicts, not objects!
                    'pagination': {
                        'total': results.total,
                        'page': results.page,
                        'per_page': results.per_page,
                        'total_pages': results.pages,
                        'has_next': results.has_next,
                        'has_prev': results.has_prev
                    }
                },
                'message': f'Found {results.total} {self.model_name} records'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Search failed for {self.model_name}'
            }
    
    def advanced_search(self, filters: Dict[str, Any], search_term: Optional[str] = None,
                       searchable_fields: Optional[List[str]] = None,
                       sort_by: Optional[str] = None, sort_direction: str = 'asc',
                       page: int = 1, per_page: int = 20, 
                       include_relationships: bool = False) -> Dict[str, Any]:
        """Advanced search with filters, text search, and sorting"""
        try:
            results = self.repository.advanced_search(
                filters=filters,
                search_term=search_term,
                searchable_fields=searchable_fields,
                sort_by=sort_by,
                sort_direction=sort_direction,
                page=page,
                per_page=per_page
            )
            
            # ⭐ CRITICAL: MUST serialize items!
            serialized_items = self._serialize_list(results.items, include_relationships=include_relationships)
            
            return {
                'success': True,
                'data': {
                    'items': serialized_items,  # ← Must be list of dicts, not objects!
                    'pagination': {
                        'total': results.total,
                        'page': results.page,
                        'per_page': results.per_page,
                        'total_pages': results.pages,
                        'has_next': results.has_next,
                        'has_prev': results.has_prev
                    }
                },
                'message': f'Found {results.total} {self.model_name} records'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Advanced search failed'
            }
    
    def get_filters(self, filter_fields: Dict[str, str]) -> Dict[str, Any]:
        """Get unique values for filter dropdowns"""
        try:
            filters = {}
            
            for field_name, display_name in filter_fields.items():
                try:
                    unique_values = self.repository.get_unique_values(field_name, limit=100)
                    filters[field_name] = {
                        'display_name': display_name,
                        'values': [ModelSerializer.serialize_value(v, include_relationships=False) for v in unique_values]
                    }
                except Exception as e:
                    filters[field_name] = {
                        'display_name': display_name,
                        'values': [],
                        'error': str(e)
                    }
            
            return {
                'success': True,
                'data': filters,
                'message': 'Filters retrieved successfully'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': 'Failed to retrieve filters'
            }
    
    # ========================================================================
    # CREATE & UPDATE & DELETE
    # ========================================================================
    
    def create(self, include_relationships: bool = False, **data) -> Dict[str, Any]:
        """Create new record"""
        try:
            record = self.repository.create(**data)
            return {
                'success': True,
                'data': self._serialize(record, include_relationships=include_relationships),
                'message': f'{self.model_name} created successfully'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Failed to create {self.model_name}'
            }
    
    def create_many(self, items: List[Dict[str, Any]], include_relationships: bool = False) -> Dict[str, Any]:
        """Create multiple records"""
        try:
            records = self.repository.create_many(items)
            return {
                'success': True,
                'data': self._serialize_list(records, include_relationships=include_relationships),
                'message': f'{len(records)} {self.model_name} records created'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Failed to create {self.model_name} records'
            }
    
    def update(self, record_id: Any, include_relationships: bool = False, **data) -> Dict[str, Any]:
        """Update record"""
        try:
            record = self.repository.update(record_id, **data)
            return {
                'success': True,
                'data': self._serialize(record, include_relationships=include_relationships),
                'message': f'{self.model_name} updated successfully'
            }
        except ValueError:
            return {
                'success': False,
                'error': f'{self.model_name} not found',
                'message': f'{self.model_name} with ID {record_id} does not exist'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Failed to update {self.model_name}'
            }
    
    def delete(self, record_id: Any) -> Dict[str, Any]:
        """Delete record"""
        try:
            self.repository.delete(record_id)
            return {
                'success': True,
                'message': f'{self.model_name} deleted successfully'
            }
        except ValueError:
            return {
                'success': False,
                'error': f'{self.model_name} not found',
                'message': f'{self.model_name} with ID {record_id} does not exist'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Failed to delete {self.model_name}'
            }
    
    def delete_many(self, ids: List[Any]) -> Dict[str, Any]:
        """Delete multiple records"""
        try:
            count = self.repository.delete_many(ids)
            return {
                'success': True,
                'message': f'{count} {self.model_name} records deleted'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Failed to delete {self.model_name} records'
            }
    
    # ========================================================================
    # STATISTICS & AGGREGATIONS
    # ========================================================================
    
    def count(self, **filters) -> Dict[str, Any]:
        """Count records with optional filters"""
        try:
            count = self.repository.count(**filters)
            return {
                'success': True,
                'data': {'count': count},
                'message': f'Total {self.model_name} records: {count}'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Failed to count {self.model_name} records'
            }
    
    def get_stats(self, numeric_fields: List[str]) -> Dict[str, Any]:
        """Get statistics on numeric fields"""
        try:
            stats = {}
            
            for field in numeric_fields:
                try:
                    stats[field] = {
                        'sum': self.repository.sum(field),
                        'avg': self.repository.avg(field),
                        'min': self.repository.min(field),
                        'max': self.repository.max(field)
                    }
                except:
                    stats[field] = {'error': 'Failed to calculate'}
            
            return {
                'success': True,
                'data': stats,
                'message': 'Statistics retrieved successfully'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': 'Failed to retrieve statistics'
            }