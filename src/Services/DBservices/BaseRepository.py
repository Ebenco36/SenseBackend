# src/Services/DBservices/BaseRepository.py (FIXED)
"""
Enhanced Base Repository with robust error handling and advanced features
FIXED VERSION: Works with both BaseModel and reflected models
"""

from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from database.db import db
from src.Services.DBservices.QueryBuilder import QueryBuilder
from sqlalchemy import inspect


@dataclass
class PaginationResult:
    """Enhanced pagination result with comprehensive metadata"""
    items: List[Any]
    total: int
    page: int
    per_page: int
    pages: int
    has_next: bool
    has_prev: bool
    next_page: Optional[int] = None
    prev_page: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON responses"""
        return {
            'items': [
                item.to_dict() if hasattr(item, 'to_dict') else item 
                for item in self.items
            ],
            'total': self.total,
            'page': self.page,
            'per_page': self.per_page,
            'pages': self.pages,
            'has_next': self.has_next,
            'has_prev': self.has_prev,
            'next_page': self.next_page,
            'prev_page': self.prev_page
        }


class BaseRepository:
    """
    Enhanced Base Repository with comprehensive database operations.
    
    ⭐ FIXED: Now works with both:
       - Models that extend BaseModel (have .count(), .find(), etc.)
       - Reflected models (created by TableReflector)
    
    Features:
    - Full CRUD operations with error handling
    - Advanced query building through QueryBuilder
    - Pagination with rich metadata
    - Bulk operations
    - Search functionality
    - Transaction support
    - Query scopes
    - Caching hooks
    """
    
    def __init__(self, model_class):
        """
        Initialize repository for a model.
        
        Args:
            model_class: SQLAlchemy model class (BaseModel or reflected)
        """
        self.model_class = model_class
        self.query_builder = QueryBuilder(model_class)
    
    def _is_base_model(self) -> bool:
        """Check if model extends BaseModel"""
        return hasattr(self.model_class, 'save') and callable(getattr(self.model_class, 'save'))
    
    def new_query(self) -> QueryBuilder:
        """
        Create a new query builder instance.
        
        Returns:
            Fresh QueryBuilder instance
        """
        return QueryBuilder(self.model_class)
    
    # ========================================================================
    # CRUD OPERATIONS
    # ========================================================================
    
    def all(self, limit: Optional[int] = None, order_by: Optional[str] = None) -> List[Any]:
        """
        Get all records.
        
        Args:
            limit: Optional limit
            order_by: Optional ordering column
            
        Returns:
            List of model instances
        """
        query = self.new_query()
        
        if order_by:
            query.order_by(order_by)
        
        if limit:
            query.limit(limit)
        
        return query.get()
    
    def find(self, id: Any) -> Optional[Any]:
        """
        Find record by primary key.
        
        Args:
            id: Primary key value
            
        Returns:
            Model instance or None
        """
        # ⭐ FIX: Use db.session.get instead of model_class.find()
        return db.session.get(self.model_class, id)
    
    def find_or_fail(self, id: Any) -> Any:
        """
        Find by ID or raise exception.
        
        Args:
            id: Primary key value
            
        Returns:
            Model instance
            
        Raises:
            ValueError: If record not found
        """
        instance = self.find(id)
        if not instance:
            raise ValueError(f"{self.model_class.__name__} with ID {id} not found")
        return instance
    
    def find_by(self, column: str, value: Any) -> Optional[Any]:
        """
        Find first record by column value.
        
        Args:
            column: Column name
            value: Value to search for
            
        Returns:
            Model instance or None
        """
        return self.new_query().where(column, value).first()
    
    def find_by_or_fail(self, column: str, value: Any) -> Any:
        """
        Find by column or raise exception.
        
        Args:
            column: Column name
            value: Value to search for
            
        Returns:
            Model instance
            
        Raises:
            ValueError: If record not found
        """
        return self.new_query().where(column, value).first_or_fail()
    
    def find_many(self, ids: List[Any]) -> List[Any]:
        """
        Find multiple records by primary keys.
        
        Args:
            ids: List of primary key values
            
        Returns:
            List of model instances
        """
        # ⭐ FIX: Get primary key dynamically
        mapper = inspect(self.model_class)
        pk_column = list(mapper.primary_key)[0].name
        return self.new_query().where_in(pk_column, ids).get()
    
    def create(self, commit: bool = True, **data) -> Any:
        """
        Create new record.
        
        Args:
            commit: Whether to commit immediately
            **data: Attributes for new record
            
        Returns:
            Created instance
            
        Raises:
            Exception: If creation fails
        """
        try:
            instance = self.model_class(**data)
            db.session.add(instance)
            
            if commit:
                db.session.commit()
            else:
                db.session.flush()
            
            return instance
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to create {self.model_class.__name__}: {str(e)}")
    
    def create_many(self, items: List[Dict[str, Any]], commit: bool = True) -> List[Any]:
        """
        Create multiple records efficiently.
        
        Args:
            items: List of attribute dictionaries
            commit: Whether to commit immediately
            
        Returns:
            List of created instances
        """
        try:
            instances = [self.model_class(**item) for item in items]
            db.session.bulk_save_objects(instances, return_defaults=True)
            
            if commit:
                db.session.commit()
            else:
                db.session.flush()
            
            return instances
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to bulk create {self.model_class.__name__}: {str(e)}")
    
    def update(self, id: Any, commit: bool = True, **data) -> Any:
        """
        Update record by ID.
        
        Args:
            id: Primary key value
            commit: Whether to commit immediately
            **data: Attributes to update
            
        Returns:
            Updated instance
            
        Raises:
            ValueError: If record not found
        """
        instance = self.find_or_fail(id)
        
        try:
            for key, value in data.items():
                if hasattr(instance, key):
                    setattr(instance, key, value)
            
            if commit:
                db.session.commit()
            else:
                db.session.flush()
            
            return instance
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to update {self.model_class.__name__}: {str(e)}")
    
    def update_or_create(self, filters: Dict[str, Any], values: Dict[str, Any]) -> tuple[Any, bool]:
        """
        Update existing record or create new one.
        
        Args:
            filters: Conditions to find existing record
            values: Values to update/create
            
        Returns:
            Tuple of (instance, created) where created is True if new record
        """
        query = self.new_query()
        for column, value in filters.items():
            query.where(column, value)
        
        instance = query.first()
        
        if instance:
            instance = self.update(self._get_id(instance), **values)
            return instance, False
        else:
            combined_data = {**filters, **values}
            return self.create(**combined_data), True
    
    def delete(self, id: Any) -> bool:
        """
        Delete record by ID.
        
        Args:
            id: Primary key value
            
        Returns:
            True if successful
            
        Raises:
            ValueError: If record not found
        """
        instance = self.find_or_fail(id)
        
        try:
            db.session.delete(instance)
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to delete {self.model_class.__name__}: {str(e)}")
    
    def delete_many(self, ids: List[Any]) -> int:
        """
        Delete multiple records by IDs.
        
        Args:
            ids: List of primary key values
            
        Returns:
            Number of deleted records
        """
        mapper = inspect(self.model_class)
        pk_column = list(mapper.primary_key)[0].name
        return self.new_query().where_in(pk_column, ids).delete()
    
    def delete_where(self, **conditions) -> int:
        """
        Delete records matching conditions.
        
        Args:
            **conditions: Column-value pairs
            
        Returns:
            Number of deleted records
        """
        query = self.new_query()
        for column, value in conditions.items():
            query.where(column, value)
        return query.delete()
    
    # ========================================================================
    # QUERY BUILDING
    # ========================================================================
    
    def where(self, column: str, operator: Any = None, value: Any = None) -> QueryBuilder:
        """
        Start a where query.
        
        Args:
            column: Column name
            operator: Comparison operator
            value: Value to compare
            
        Returns:
            QueryBuilder for chaining
        """
        return self.new_query().where(column, operator, value)
    
    def where_in(self, column: str, values: List[Any]) -> QueryBuilder:
        """Start a WHERE IN query"""
        return self.new_query().where_in(column, values)
    
    def where_between(self, column: str, low: Any, high: Any) -> QueryBuilder:
        """Start a WHERE BETWEEN query"""
        return self.new_query().where_between(column, low, high)
    
    def where_null(self, column: str) -> QueryBuilder:
        """Start a WHERE IS NULL query"""
        return self.new_query().where_null(column)
    
    def where_not_null(self, column: str) -> QueryBuilder:
        """Start a WHERE IS NOT NULL query"""
        return self.new_query().where_not_null(column)
    
    def order_by(self, column: str, direction: str = 'asc') -> QueryBuilder:
        """
        Start an ordered query.
        
        Args:
            column: Column to order by
            direction: 'asc' or 'desc'
            
        Returns:
            QueryBuilder for chaining
        """
        return self.new_query().order_by(column, direction)
    
    def latest(self, column: str = 'created_at') -> QueryBuilder:
        """Get latest records (newest first)"""
        return self.new_query().latest(column)
    
    def oldest(self, column: str = 'created_at') -> QueryBuilder:
        """Get oldest records (oldest first)"""
        return self.new_query().oldest(column)
    
    def with_relations(self, *relations: str) -> QueryBuilder:
        """
        Start query with eager-loaded relationships.
        
        Args:
            *relations: Relationship names
            
        Returns:
            QueryBuilder for chaining
        """
        return self.new_query().with_relations(*relations)
    
    # ========================================================================
    # PAGINATION
    # ========================================================================
    
    def paginate(self, page: int = 1, per_page: int = 20, 
                 query: Optional[QueryBuilder] = None) -> PaginationResult:
        """
        Paginate results with comprehensive metadata.
        
        Args:
            page: Page number (1-indexed)
            per_page: Items per page
            query: Optional existing QueryBuilder
            
        Returns:
            PaginationResult with items and metadata
        """
        if query is None:
            query = self.new_query()
        
        pagination = query.paginate(page=page, per_page=per_page)
        
        return PaginationResult(
            items=pagination.items,
            total=pagination.total,
            page=pagination.page,
            per_page=pagination.per_page,
            pages=pagination.pages,
            has_next=pagination.has_next,
            has_prev=pagination.has_prev,
            next_page=pagination.next_num if pagination.has_next else None,
            prev_page=pagination.prev_num if pagination.has_prev else None
        )
    
    # ========================================================================
    # SEARCH
    # ========================================================================
    
    def search(self, search_term: str, searchable_fields: List[str], 
               page: int = 1, per_page: int = 20) -> PaginationResult:
        """
        Full-text search across multiple fields with pagination.
        
        Args:
            search_term: Search term
            searchable_fields: List of column names to search in
            page: Page number
            per_page: Items per page
            
        Returns:
            PaginationResult with matching items
        """
        if not search_term or not searchable_fields:
            return self.paginate(page=page, per_page=per_page)
        
        query = self.new_query()
        
        # Build OR conditions for each searchable field
        from sqlalchemy import or_
        conditions = []
        for field in searchable_fields:
            col = getattr(self.model_class, field)
            conditions.append(col.ilike(f'%{search_term}%'))
        
        query._filters.append(or_(*conditions))
        
        return self.paginate(page=page, per_page=per_page, query=query)
    
    def advanced_search(self, filters: Dict[str, Any], 
                       search_term: Optional[str] = None,
                       searchable_fields: Optional[List[str]] = None,
                       sort_by: Optional[str] = None,
                       sort_direction: str = 'asc',
                       page: int = 1,
                       per_page: int = 20) -> PaginationResult:
        """
        Advanced search with filters, text search, sorting, and pagination.
        
        Args:
            filters: Dictionary of column-value pairs for exact matches
            search_term: Optional text search term
            searchable_fields: Fields to search in (required if search_term provided)
            sort_by: Column to sort by
            sort_direction: 'asc' or 'desc'
            page: Page number
            per_page: Items per page
            
        Returns:
            PaginationResult with matching items
        """
        query = self.new_query()
        
        # Apply filters
        for column, value in filters.items():
            if isinstance(value, list):
                query.where_in(column, value)
            elif value is None:
                query.where_null(column)
            else:
                query.where(column, value)
        
        # Apply search
        if search_term and searchable_fields:
            from sqlalchemy import or_
            conditions = []
            for field in searchable_fields:
                col = getattr(self.model_class, field)
                conditions.append(col.ilike(f'%{search_term}%'))
            query._filters.append(or_(*conditions))
        
        # Apply sorting
        if sort_by:
            query.order_by(sort_by, sort_direction)
        
        return self.paginate(page=page, per_page=per_page, query=query)
    
    # ========================================================================
    # AGGREGATIONS
    # ========================================================================
    
    def count(self, **conditions) -> int:
        """
        Count records, optionally with conditions.
        
        Args:
            **conditions: Optional column-value pairs
            
        Returns:
            Count of matching records
        """
        # ⭐ FIX: Always use QueryBuilder, not model_class.count()
        if not conditions:
            return self.new_query().count()
        
        query = self.new_query()
        for column, value in conditions.items():
            query.where(column, value)
        return query.count()
    
    def exists(self, **conditions) -> bool:
        """
        Check if records exist with given conditions.
        
        Args:
            **conditions: Column-value pairs
            
        Returns:
            True if at least one record exists
        """
        return self.count(**conditions) > 0
    
    def sum(self, column: str, **conditions) -> float:
        """Calculate sum of column with optional conditions"""
        query = self.new_query()
        for col, value in conditions.items():
            query.where(col, value)
        return query.sum(column)
    
    def avg(self, column: str, **conditions) -> float:
        """Calculate average of column with optional conditions"""
        query = self.new_query()
        for col, value in conditions.items():
            query.where(col, value)
        return query.avg(column)
    
    def min(self, column: str, **conditions) -> Any:
        """Get minimum value of column with optional conditions"""
        query = self.new_query()
        for col, value in conditions.items():
            query.where(col, value)
        return query.min(column)
    
    def max(self, column: str, **conditions) -> Any:
        """Get maximum value of column with optional conditions"""
        query = self.new_query()
        for col, value in conditions.items():
            query.where(col, value)
        return query.max(column)
    
    def pluck(self, column: str, **conditions) -> List[Any]:
        """
        Get list of values from a single column.
        
        Args:
            column: Column name
            **conditions: Optional filters
            
        Returns:
            List of column values
        """
        query = self.new_query()
        for col, value in conditions.items():
            query.where(col, value)
        return query.pluck(column)
    
    # ========================================================================
    # BULK OPERATIONS
    # ========================================================================
    
    def update_where(self, conditions: Dict[str, Any], updates: Dict[str, Any]) -> int:
        """
        Update all records matching conditions.
        
        Args:
            conditions: Column-value pairs for filtering
            updates: Column-value pairs to update
            
        Returns:
            Number of updated records
        """
        query = self.new_query()
        for column, value in conditions.items():
            query.where(column, value)
        return query.update(**updates)
    
    def increment(self, id: Any, column: str, amount: int = 1) -> Any:
        """
        Increment a column value.
        
        Args:
            id: Primary key value
            column: Column to increment
            amount: Amount to increment by
            
        Returns:
            Updated instance
        """
        mapper = inspect(self.model_class)
        pk_column = list(mapper.primary_key)[0].name
        self.new_query().where(pk_column, id).increment(column, amount)
        return self.find(id)
    
    def decrement(self, id: Any, column: str, amount: int = 1) -> Any:
        """Decrement a column value"""
        mapper = inspect(self.model_class)
        pk_column = list(mapper.primary_key)[0].name
        self.new_query().where(pk_column, id).decrement(column, amount)
        return self.find(id)
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def get_unique_values(self, column: str, limit: int = 100) -> List[Any]:
        """
        Get unique values for a column (useful for filters/dropdowns).
        
        Args:
            column: Column name
            limit: Maximum number of unique values
            
        Returns:
            List of unique values
        """
        return self.new_query().distinct(column).limit(limit).pluck(column)
    
    def chunk(self, chunk_size: int, callback: Callable[[List[Any]], None]) -> None:
        """
        Process all records in chunks (memory efficient).
        
        Args:
            chunk_size: Number of items per chunk
            callback: Function to process each chunk
        """
        self.new_query().chunk(chunk_size, callback)
    
    def first_or_create(self, filters: Dict[str, Any], values: Dict[str, Any] = None) -> tuple[Any, bool]:
        """
        Get first matching record or create new one.
        
        Args:
            filters: Conditions to find existing record
            values: Additional values for creation (optional)
            
        Returns:
            Tuple of (instance, created) where created is True if new record
        """
        query = self.new_query()
        for column, value in filters.items():
            query.where(column, value)
        
        instance = query.first()
        
        if instance:
            return instance, False
        else:
            create_data = {**filters}
            if values:
                create_data.update(values)
            return self.create(**create_data), True
    
    def fresh(self, instance: Any) -> Any:
        """
        Get a fresh copy of an instance from database.
        
        Args:
            instance: Model instance
            
        Returns:
            Fresh instance from database
        """
        id_value = self._get_id(instance)
        return self.find(id_value)
    
    def _get_id(self, instance: Any) -> Any:
        """Get primary key value from instance"""
        mapper = inspect(self.model_class)
        pk_column = list(mapper.primary_key)[0]
        return getattr(instance, pk_column.name)
    
    # ========================================================================
    # TRANSACTION SUPPORT
    # ========================================================================
    
    @staticmethod
    def transaction(callback: Callable[[], Any]) -> Any:
        """
        Execute callback in a database transaction.
        
        Args:
            callback: Function to execute in transaction
            
        Returns:
            Result of callback
            
        Raises:
            Exception: If transaction fails (automatically rolls back)
        """
        try:
            result = callback()
            db.session.commit()
            return result
        except Exception as e:
            db.session.rollback()
            raise e
    
    # ========================================================================
    # DEBUGGING
    # ========================================================================
    
    def to_sql(self) -> str:
        """Get SQL query string for debugging"""
        return self.new_query().to_sql()
    
    def dump(self):
        """Print SQL query and return self"""
        return self.new_query().dump()