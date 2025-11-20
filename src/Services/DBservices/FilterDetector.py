# src/Services/DBservices/FilterDetector.py

from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import inspect, String, Integer, Float, DateTime, Boolean, Text
from sqlalchemy.types import VARCHAR, CHAR, NUMERIC, DECIMAL, SMALLINT, BIGINT
import logging

logger = logging.getLogger(__name__)


class FilterDetector:
    """
    Auto-detect filterable columns from SQLAlchemy model
    
    Responsibilities:
    - Analyze database schema
    - Classify column types
    - Generate filter options
    - Handle special columns
    """
    
    # Column types that are searchable (text-based)
    SEARCHABLE_TYPES = (
        String, VARCHAR, CHAR, Text
    )
    
    # Column types that are numeric
    NUMERIC_TYPES = (
        Integer, SMALLINT, BIGINT, Float, NUMERIC, DECIMAL
    )
    
    # Column types that are date/time
    DATETIME_TYPES = (
        DateTime,
    )
    
    # Column types that are boolean
    BOOLEAN_TYPES = (
        Boolean,
    )
    
    # Columns to exclude from filtering
    EXCLUDED_COLUMNS = {
        'id', 'primary_id', 'created_at', 'updated_at', 
        'deleted_at', 'created_on', 'updated_on',
        'password', 'token', 'secret', 'hash'
    }
    
    # Special column handlers
    SPECIAL_COLUMNS = {
        'year': {'type': 'numeric_range', 'min': 1900, 'max': 2100},
        'date': {'type': 'date_range'},
        'status': {'type': 'select', 'values': ['active', 'inactive', 'pending']},
        'country': {'type': 'select'},
        'region': {'type': 'select'},
        'language': {'type': 'select'},
    }
    
    def __init__(self, model_class):
        """
        Initialize filter detector
        
        Args:
            model_class: SQLAlchemy model class
        """
        self.model_class = model_class
        self.mapper = inspect(model_class)
        self.logger = logger
    
    def get_all_filters(self) -> Dict[str, Any]:
        """
        Get all filters for the model
        
        Returns:
        {
            'string_filters': {...},
            'numeric_filters': {...},
            'datetime_filters': {...},
            'boolean_filters': {...},
            'special_filters': {...}
        }
        """
        try:
            self.logger.info(f"Detecting filters for {self.model_class.__name__}")
            
            filters = {
                'string_filters': {},
                'numeric_filters': {},
                'datetime_filters': {},
                'boolean_filters': {},
                'special_filters': {}
            }
            
            # Analyze each column
            for column in self.mapper.columns:
                # Skip excluded columns
                if column.name in self.EXCLUDED_COLUMNS:
                    self.logger.debug(f"Skipping excluded column: {column.name}")
                    continue
                
                # Skip primary keys
                if column.primary_key:
                    self.logger.debug(f"Skipping primary key: {column.name}")
                    continue
                
                # Get column filter info
                filter_info = self._get_column_filter(column)
                
                if filter_info:
                    category = filter_info.pop('category')
                    filters[category][column.name] = filter_info
            
            self.logger.info(f"Detected {sum(len(v) for v in filters.values())} filterable columns")
            return filters
        
        except Exception as e:
            self.logger.error(f"Error detecting filters: {str(e)}", exc_info=True)
            return {
                'string_filters': {},
                'numeric_filters': {},
                'datetime_filters': {},
                'boolean_filters': {},
                'special_filters': {}
            }
    
    def _get_column_filter(self, column) -> Optional[Dict]:
        """
        Get filter info for a column
        
        Returns: Filter configuration dict or None
        """
        try:
            column_name = column.name
            column_type = type(column.type)
            
            # Check for special column handling
            if column_name.lower() in self.SPECIAL_COLUMNS:
                return self._handle_special_column(column_name)
            
            # Classify by type
            if self._is_searchable(column_type):
                return {
                    'category': 'string_filters',
                    'type': 'text',
                    'display_name': column_name.replace('_', ' ').title(),
                    'searchable': True,
                    'nullable': column.nullable
                }
            
            elif self._is_numeric(column_type):
                return {
                    'category': 'numeric_filters',
                    'type': 'numeric',
                    'display_name': column_name.replace('_', ' ').title(),
                    'searchable': False,
                    'nullable': column.nullable
                }
            
            elif self._is_datetime(column_type):
                return {
                    'category': 'datetime_filters',
                    'type': 'datetime',
                    'display_name': column_name.replace('_', ' ').title(),
                    'searchable': False,
                    'nullable': column.nullable
                }
            
            elif self._is_boolean(column_type):
                return {
                    'category': 'boolean_filters',
                    'type': 'boolean',
                    'display_name': column_name.replace('_', ' ').title(),
                    'values': [True, False],
                    'nullable': column.nullable
                }
            
            return None
        
        except Exception as e:
            self.logger.error(f"Error getting filter for {column.name}: {str(e)}")
            return None
    
    def _handle_special_column(self, column_name: str) -> Dict:
        """Handle special column types"""
        special = self.SPECIAL_COLUMNS.get(column_name.lower(), {})
        
        return {
            'category': 'special_filters',
            'type': special.get('type', 'select'),
            'display_name': column_name.replace('_', ' ').title(),
            'values': special.get('values', []),
            'min': special.get('min'),
            'max': special.get('max')
        }
    
    def _is_searchable(self, column_type) -> bool:
        """Check if column type is searchable (text)"""
        return isinstance(column_type, self.SEARCHABLE_TYPES) or \
               column_type in self.SEARCHABLE_TYPES
    
    def _is_numeric(self, column_type) -> bool:
        """Check if column type is numeric"""
        return isinstance(column_type, self.NUMERIC_TYPES) or \
               column_type in self.NUMERIC_TYPES
    
    def _is_datetime(self, column_type) -> bool:
        """Check if column type is datetime"""
        return isinstance(column_type, self.DATETIME_TYPES) or \
               column_type in self.DATETIME_TYPES
    
    def _is_boolean(self, column_type) -> bool:
        """Check if column type is boolean"""
        return isinstance(column_type, self.BOOLEAN_TYPES) or \
               column_type in self.BOOLEAN_TYPES
    
    def get_column_values(self, column_name: str, limit: int = 100) -> List[Any]:
        """
        Get distinct values for a column (for select filters)
        
        Args:
            column_name: Name of column
            limit: Maximum values to return
        
        Returns: List of distinct values
        """
        try:
            from database import db
            
            # Build query to get distinct values
            column = getattr(self.model_class, column_name)
            values = db.session.query(column).distinct().limit(limit).all()
            
            # Flatten results (they come as tuples)
            return [v[0] for v in values if v[0] is not None]
        
        except Exception as e:
            self.logger.error(f"Error getting values for {column_name}: {str(e)}")
            return []
    
    def get_column_range(self, column_name: str) -> Optional[Tuple[Any, Any]]:
        """
        Get min/max values for numeric columns
        
        Returns: (min_value, max_value) or None
        """
        try:
            from database import db
            from sqlalchemy import func
            
            column = getattr(self.model_class, column_name)
            result = db.session.query(
                func.min(column),
                func.max(column)
            ).first()
            
            if result and result[0] is not None:
                return (result[0], result[1])
            return None
        
        except Exception as e:
            self.logger.error(f"Error getting range for {column_name}: {str(e)}")
            return None
    
    def get_column_stats(self, column_name: str) -> Dict[str, Any]:
        """
        Get statistics for a column
        
        Returns:
        {
            'type': 'numeric|text|datetime|boolean',
            'display_name': 'Column Name',
            'unique_count': 42,
            'null_count': 3,
            'min': 0,
            'max': 100,
            'searchable': True
        }
        """
        try:
            from database import db
            from sqlalchemy import func
            
            column = getattr(self.model_class, column_name)
            column_type = self.mapper.columns[column_name].type
            
            # Get basic stats
            total_count = db.session.query(func.count(column)).scalar() or 0
            unique_count = db.session.query(func.count(func.distinct(column))).scalar() or 0
            null_count = db.session.query(func.count()).filter(column == None).scalar() or 0
            
            stats = {
                'type': self._classify_type(type(column_type)),
                'display_name': column_name.replace('_', ' ').title(),
                'total_count': total_count,
                'unique_count': unique_count,
                'null_count': null_count,
                'searchable': self._is_searchable(type(column_type))
            }
            
            # Add numeric stats if applicable
            if self._is_numeric(type(column_type)):
                range_vals = self.get_column_range(column_name)
                if range_vals:
                    stats['min'] = range_vals[0]
                    stats['max'] = range_vals[1]
            
            return stats
        
        except Exception as e:
            self.logger.error(f"Error getting stats for {column_name}: {str(e)}")
            return {}
    
    def _classify_type(self, column_type) -> str:
        """Classify column type as string"""
        if self._is_searchable(column_type):
            return 'text'
        elif self._is_numeric(column_type):
            return 'numeric'
        elif self._is_datetime(column_type):
            return 'datetime'
        elif self._is_boolean(column_type):
            return 'boolean'
        return 'unknown'
    
    def generate_filter_options(self) -> Dict[str, Any]:
        """
        Generate complete filter options for UI
        
        Returns fully structured filter options ready for frontend
        """
        try:
            filters = self.get_all_filters()
            options = {
                'categories': {},
                'total_filters': 0
            }
            
            # Process each filter category
            for category, columns in filters.items():
                options['categories'][category] = {}
                
                for col_name, col_info in columns.items():
                    # Get additional data for select filters
                    if col_info.get('type') == 'text' and col_info.get('searchable'):
                        col_info['values'] = self.get_column_values(col_name, limit=50)
                    
                    elif col_info.get('type') == 'numeric':
                        range_vals = self.get_column_range(col_name)
                        if range_vals:
                            col_info['min'] = range_vals[0]
                            col_info['max'] = range_vals[1]
                    
                    options['categories'][category][col_name] = col_info
                    options['total_filters'] += 1
            
            self.logger.info(f"Generated {options['total_filters']} filter options")
            return options
        
        except Exception as e:
            self.logger.error(f"Error generating filter options: {str(e)}")
            return {'categories': {}, 'total_filters': 0}
    
    def validate_filter_value(self, column_name: str, value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate a filter value for a column
        
        Returns: (is_valid, error_message)
        """
        try:
            # Check column exists
            if column_name not in [c.name for c in self.mapper.columns]:
                return False, f"Column '{column_name}' does not exist"
            
            column = self.mapper.columns[column_name]
            
            # Check nullable
            if value is None and not column.nullable:
                return False, f"Column '{column_name}' does not allow NULL"
            
            # Check type
            column_type = type(column.type)
            
            if self._is_numeric(column_type):
                try:
                    float(value)
                except (ValueError, TypeError):
                    return False, f"Value must be numeric for {column_name}"
            
            elif self._is_boolean(column_type):
                if not isinstance(value, bool):
                    return False, f"Value must be boolean for {column_name}"
            
            return True, None
        
        except Exception as e:
            self.logger.error(f"Error validating filter: {str(e)}")
            return False, str(e)