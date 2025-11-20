# src/Services/DBservices/ModelSerializer.py (FIXED)
"""
Model Serializer - Convert ANY model instance to dict for JSON serialization

⭐ FIXED: Now properly uses include_relationships parameter
"""

from datetime import datetime, date, time
from decimal import Decimal
from typing import Any, Dict, List
from sqlalchemy import inspect


class ModelSerializer:
    """Serialize SQLAlchemy models to dictionaries"""
    
    @staticmethod
    def serialize_value(value: Any, include_relationships: bool = False) -> Any:
        """
        Convert value to JSON-serializable format
        
        Args:
            value: Value to serialize
            include_relationships: Whether to serialize related objects
            
        Returns:
            JSON-serializable value
        """
        if value is None:
            return None
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (date, time)):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        elif isinstance(value, (list, tuple)):
            return [ModelSerializer.serialize_value(v, include_relationships) for v in value]
        elif isinstance(value, dict):
            return {k: ModelSerializer.serialize_value(v, include_relationships) for k, v in value.items()}
        elif hasattr(value, 'to_dict') and callable(value.to_dict):
            # ⭐ Use to_dict if available
            return value.to_dict(include_relationships=include_relationships)
        else:
            try:
                return str(value)
            except:
                return None
    
    @staticmethod
    def to_dict(obj: Any, exclude_fields: List[str] = None, include_relationships: bool = False) -> Dict:
        """
        Convert SQLAlchemy model to dictionary
        
        ⭐ FIXED: Now properly uses include_relationships!
        
        Args:
            obj: Model instance
            exclude_fields: List of field names to exclude
            include_relationships: Whether to include related objects
            
        Returns:
            Dictionary representation
        """
        if exclude_fields is None:
            exclude_fields = []
        
        result = {}
        
        try:
            mapper = inspect(obj.__class__)
            
            # ========================================================================
            # SERIALIZE COLUMNS (always included)
            # ========================================================================
            for column in mapper.columns:
                if column.name in exclude_fields:
                    continue
                
                try:
                    value = getattr(obj, column.name, None)
                    result[column.name] = ModelSerializer.serialize_value(value, include_relationships)
                except:
                    result[column.name] = None
            
            # ========================================================================
            # SERIALIZE RELATIONSHIPS (only if include_relationships=True)
            # ========================================================================
            if include_relationships:
                for relationship in mapper.relationships:
                    if relationship.key in exclude_fields:
                        continue
                    
                    try:
                        related_obj = getattr(obj, relationship.key, None)
                        
                        if related_obj is None:
                            result[relationship.key] = None
                        elif isinstance(related_obj, list):
                            # One-to-many relationship
                            result[relationship.key] = [
                                ModelSerializer.to_dict(item, include_relationships=False)
                                for item in related_obj
                            ]
                        else:
                            # One-to-one or many-to-one relationship
                            result[relationship.key] = ModelSerializer.to_dict(
                                related_obj,
                                include_relationships=False
                            )
                    except:
                        # Skip if relationship can't be loaded
                        pass
            
            return result
        
        except Exception as e:
            return {'error': f'Failed to serialize: {str(e)}'}
    
    @staticmethod
    def serialize_list(items: List[Any], include_relationships: bool = False) -> List[Dict]:
        """
        Convert list of models to list of dicts
        
        ⭐ FIXED: Now properly uses include_relationships!
        
        Args:
            items: List of model instances
            include_relationships: Whether to include related objects
            
        Returns:
            List of dictionaries
        """
        return [ModelSerializer.to_dict(item, include_relationships=include_relationships) for item in items]