# src/Services/SearchService.py

from typing import Any, Dict, List, Optional
from src.Services.BaseService import BaseService


class SearchService(BaseService):
    """
    Service for handling advanced search operations
    
    Responsibilities:
    - Validate search queries
    - Build database queries
    - Execute searches
    - Format results
    """
    
    def __init__(self, repository):
        """Initialize search service"""
        super().__init__('SearchService')
        self.repository = repository
    
    def validate_input(self, data: Any) -> tuple[bool, Optional[str]]:
        """Validate search query"""
        if not data:
            return False, "Search query is empty"
        
        if not isinstance(data, dict):
            return False, "Search query must be a dictionary"
        
        if 'conditions' not in data:
            return False, "Search query must have 'conditions'"
        
        return True, None
    
    def build_query(self, search_params: Dict) -> Dict:
        """
        Build database query from search parameters
        
        Input:
        {
            'conditions': [
                {'field': 'title', 'operator': 'contains', 'value': 'vaccine'},
                {'field': 'year', 'operator': 'in', 'values': [2020, 2021]}
            ],
            'logic': 'AND'
        }
        
        Returns: Validated query
        """
        try:
            is_valid, error = self.validate_input(search_params)
            if not is_valid:
                return self.create_response(False, errors=[error])
            
            conditions = search_params.get('conditions', [])
            logic = search_params.get('logic', 'AND').upper()
            
            # Validate logic
            if logic not in ['AND', 'OR']:
                return self.create_response(False, errors=['Logic must be AND or OR'])
            
            # Validate conditions
            errors = self._validate_conditions(conditions)
            if errors:
                return self.create_response(False, errors=errors)
            
            self.log_info(f"Built query with {len(conditions)} conditions")
            return self.create_response(
                True,
                data={'conditions': conditions, 'logic': logic},
                message=f"Query built with {len(conditions)} conditions"
            )
        
        except Exception as e:
            self.log_error("Failed to build query", exception=e)
            return self.create_response(False, errors=[str(e)])
    
    def _validate_conditions(self, conditions: List[Dict]) -> List[str]:
        """Validate search conditions"""
        errors = []
        
        if not isinstance(conditions, list):
            errors.append("Conditions must be a list")
            return errors
        
        if len(conditions) == 0:
            errors.append("At least one condition is required")
            return errors
        
        required_fields = ['field', 'operator']
        valid_operators = ['=', '!=', '>', '<', '>=', '<=', 'contains', 'in', 'between']
        
        for i, condition in enumerate(conditions):
            # Check required fields
            for field in required_fields:
                if field not in condition:
                    errors.append(f"Condition {i}: Missing '{field}'")
            
            # Check operator validity
            if 'operator' in condition and condition['operator'] not in valid_operators:
                errors.append(f"Condition {i}: Invalid operator '{condition['operator']}'")
        
        return errors
    
    def execute_search(self, query: Dict, pagination: Dict = None) -> Dict:
        """
        Execute search query
        
        Args:
            query: Search query dict
            pagination: {'page': 1, 'per_page': 20}
        
        Returns: Search results
        """
        try:
            if not self.repository:
                return self.create_response(False, errors=['Repository not initialized'])
            
            is_valid, error = self.validate_input(query)
            if not is_valid:
                return self.create_response(False, errors=[error])
            
            # Build database query from conditions
            db_query = self.repository.new_query()
            
            conditions = query.get('conditions', [])
            logic = query.get('logic', 'AND')
            
            # Apply conditions
            for i, condition in enumerate(conditions):
                field = condition.get('field')
                operator = condition.get('operator')
                value = condition.get('value')
                values = condition.get('values')
                
                if operator == '=':
                    db_query.where(field, '=', value)
                elif operator == '!=':
                    db_query.where(field, '!=', value)
                elif operator == '>':
                    db_query.where(field, '>', value)
                elif operator == '<':
                    db_query.where(field, '<', value)
                elif operator == 'contains':
                    db_query.where(field, 'ilike', f'%{value}%')
                elif operator == 'in':
                    db_query.where_in(field, values or [value])
                elif operator == 'between':
                    if isinstance(value, list) and len(value) == 2:
                        db_query.where_between(field, value[0], value[1])
            
            # Apply pagination
            page = pagination.get('page', 1) if pagination else 1
            per_page = pagination.get('per_page', 20) if pagination else 20
            
            results = db_query.paginate(page=page, per_page=per_page)
            
            self.log_info(f"Search executed: {results.total} results found")
            
            return self.create_response(
                True,
                data={
                    'items': results.items,
                    'pagination': {
                        'total': results.total,
                        'page': results.page,
                        'per_page': results.per_page,
                        'total_pages': results.pages
                    }
                },
                message=f"Found {results.total} results"
            )
        
        except Exception as e:
            self.log_error("Search execution failed", exception=e)
            return self.create_response(False, errors=[str(e)])