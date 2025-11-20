# src/Models/BaseModel.py
"""
Enhanced Base Model with common functionality for all models
"""

from database import db
from datetime import datetime
from sqlalchemy import inspect
from typing import Dict, Any, List, Optional


class BaseModel(db.Model):
    """
    Base model with common functionality for all database models.
    
    Provides:
    - Automatic timestamps
    - CRUD operations with error handling
    - Serialization
    - Soft deletes (optional)
    - Audit trail hooks
    """
    
    __abstract__ = True
    
    # Common timestamp fields
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def save(self, commit: bool = True) -> 'BaseModel':
        """
        Save the current instance to database.
        
        Args:
            commit: Whether to commit immediately (default: True)
            
        Returns:
            Self for chaining
            
        Raises:
            Exception: If database operation fails
        """
        try:
            db.session.add(self)
            if commit:
                db.session.commit()
            else:
                db.session.flush()
            return self
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to save {self.__class__.__name__}: {str(e)}")
    
    def delete(self, commit: bool = True) -> bool:
        """
        Delete the current instance from database.
        
        Args:
            commit: Whether to commit immediately (default: True)
            
        Returns:
            True if successful
            
        Raises:
            Exception: If database operation fails
        """
        try:
            db.session.delete(self)
            if commit:
                db.session.commit()
            else:
                db.session.flush()
            return True
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to delete {self.__class__.__name__}: {str(e)}")
    
    def update(self, commit: bool = True, **kwargs) -> 'BaseModel':
        """
        Update the current instance with given attributes.
        
        Args:
            commit: Whether to commit immediately (default: True)
            **kwargs: Attributes to update
            
        Returns:
            Self for chaining
            
        Raises:
            ValueError: If attribute doesn't exist
            Exception: If database operation fails
        """
        try:
            for key, value in kwargs.items():
                if not hasattr(self, key):
                    raise ValueError(f"Attribute '{key}' does not exist on {self.__class__.__name__}")
                setattr(self, key, value)
            
            if commit:
                db.session.commit()
            else:
                db.session.flush()
            
            return self
        except ValueError as e:
            raise e
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to update {self.__class__.__name__}: {str(e)}")
    
    def refresh(self) -> 'BaseModel':
        """Refresh instance from database"""
        db.session.refresh(self)
        return self
    
    @classmethod
    def create(cls, commit: bool = True, **kwargs) -> 'BaseModel':
        """
        Create a new instance and save to database.
        
        Args:
            commit: Whether to commit immediately (default: True)
            **kwargs: Attributes for the new instance
            
        Returns:
            New instance
        """
        instance = cls(**kwargs)
        return instance.save(commit=commit)
    
    @classmethod
    def find(cls, id: Any) -> Optional['BaseModel']:
        """
        Find record by primary key.
        
        Args:
            id: Primary key value
            
        Returns:
            Instance or None
        """
        return db.session.get(cls, id)
    
    @classmethod
    def find_or_fail(cls, id: Any) -> 'BaseModel':
        """
        Find record by primary key or raise exception.
        
        Args:
            id: Primary key value
            
        Returns:
            Instance
            
        Raises:
            ValueError: If record not found
        """
        instance = cls.find(id)
        if not instance:
            raise ValueError(f"{cls.__name__} with ID {id} not found")
        return instance
    
    @classmethod
    def all(cls, limit: Optional[int] = None) -> List['BaseModel']:
        """
        Get all records.
        
        Args:
            limit: Optional limit on number of records
            
        Returns:
            List of instances
        """
        query = cls.query
        if limit:
            query = query.limit(limit)
        return query.all()
    
    @classmethod
    def count(cls) -> int:
        """Get total count of records"""
        return cls.query.count()
    
    @classmethod
    def exists(cls, id: Any) -> bool:
        """Check if record exists by primary key"""
        return cls.find(id) is not None
    
    @classmethod
    def bulk_create(cls, items: List[Dict[str, Any]], commit: bool = True) -> List['BaseModel']:
        """
        Bulk create multiple records efficiently.
        
        Args:
            items: List of dictionaries with attributes
            commit: Whether to commit immediately (default: True)
            
        Returns:
            List of created instances
        """
        try:
            instances = [cls(**item) for item in items]
            db.session.bulk_save_objects(instances, return_defaults=True)
            
            if commit:
                db.session.commit()
            else:
                db.session.flush()
            
            return instances
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to bulk create {cls.__name__}: {str(e)}")
    
    @classmethod
    def bulk_update(cls, items: List[Dict[str, Any]], commit: bool = True) -> int:
        """
        Bulk update multiple records efficiently.
        
        Args:
            items: List of dictionaries with 'id' and attributes to update
            commit: Whether to commit immediately (default: True)
            
        Returns:
            Number of updated records
        """
        try:
            db.session.bulk_update_mappings(cls, items)
            
            if commit:
                db.session.commit()
            else:
                db.session.flush()
            
            return len(items)
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to bulk update {cls.__name__}: {str(e)}")
    
    def to_dict(self, include_relationships: bool = False) -> Dict[str, Any]:
        """
        Serialize model to dictionary.
        
        Args:
            include_relationships: Whether to include relationship data
            
        Returns:
            Dictionary representation
        """
        mapper = inspect(self.__class__)
        result = {}
        
        # Add columns
        for column in mapper.columns:
            value = getattr(self, column.name)
            # Handle datetime serialization
            if isinstance(value, datetime):
                value = value.isoformat()
            result[column.name] = value
        
        # Optionally add relationships
        if include_relationships:
            for relationship in mapper.relationships:
                related = getattr(self, relationship.key)
                if related is not None:
                    if relationship.uselist:
                        result[relationship.key] = [r.to_dict(False) for r in related]
                    else:
                        result[relationship.key] = related.to_dict(False)
        
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """
        Create instance from dictionary.
        
        Args:
            data: Dictionary with attributes
            
        Returns:
            New instance (not saved to database)
        """
        # Filter only valid columns
        mapper = inspect(cls)
        valid_data = {
            key: value 
            for key, value in data.items() 
            if key in [col.name for col in mapper.columns]
        }
        return cls(**valid_data)
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Get list of all column names"""
        return [column.name for column in inspect(cls).columns]
    
    @classmethod
    def get_primary_key(cls) -> str:
        """Get primary key column name"""
        return inspect(cls).primary_key[0].name
    
    def __repr__(self) -> str:
        """String representation"""
        pk = self.get_primary_key()
        pk_value = getattr(self, pk, None)
        return f"<{self.__class__.__name__} {pk}={pk_value}>"