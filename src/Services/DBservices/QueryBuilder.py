# src/Services/DBservices/QueryBuilder.py
"""
Enhanced Laravel-style Query Builder with advanced features
"""

from typing import Any, List, Union, Optional, Callable, Dict
from sqlalchemy import and_, or_, not_, func, cast, String
from sqlalchemy.orm import Query, joinedload
from database import db


class QueryBuilder:
    """
    Advanced Query Builder with chainable methods and robust operators.
    
    Features:
    - All comparison operators (=, !=, >, <, >=, <=, like, in, between, etc.)
    - Complex AND/OR logic with nested conditions
    - Eager loading with relationships
    - Aggregations and grouping
    - Raw SQL execution
    - Query scopes
    - Soft delete support
    """
    
    def __init__(self, model_class):
        """
        Initialize query builder for a model.
        
        Args:
            model_class: SQLAlchemy model class
        """
        self.model_class = model_class
        self._query = model_class.query
        self._filters = []
        self._or_filters = []
        self._relations = []
        self._select_fields = None
        self._group_by_fields = []
        self._having_conditions = []
        self._order_by_list = []
        self._limit_value = None
        self._offset_value = None
        self._distinct_value = False
        self._with_deleted = False
    
    # ========================================================================
    # WHERE CONDITIONS
    # ========================================================================
    
    def where(self, column: str, operator: Any = None, value: Any = None) -> 'QueryBuilder':
        """
        Add WHERE condition (AND logic).
        
        Args:
            column: Column name or callable for complex conditions
            operator: Comparison operator or value if operator is '='
            value: Value to compare (optional if operator is value)
            
        Returns:
            Self for chaining
            
        Examples:
            .where('name', 'John')              # name = 'John'
            .where('age', '>', 18)              # age > 18
            .where('status', 'in', ['active'])  # status IN ['active']
        """
        # Handle callable (for complex conditions)
        if callable(column):
            condition = column(self.model_class)
            self._filters.append(condition)
            return self
        
        # Handle two-argument form: where('name', 'John')
        if value is None:
            value = operator
            operator = '='
        
        condition = self._build_condition(column, operator, value)
        self._filters.append(condition)
        return self
    
    def or_where(self, column: str, operator: Any = None, value: Any = None) -> 'QueryBuilder':
        """
        Add WHERE condition with OR logic.
        
        Args:
            column: Column name
            operator: Comparison operator or value
            value: Value to compare
            
        Returns:
            Self for chaining
        """
        if value is None:
            value = operator
            operator = '='
        
        condition = self._build_condition(column, operator, value)
        self._or_filters.append(condition)
        return self
    
    def where_in(self, column: str, values: List[Any]) -> 'QueryBuilder':
        """Add WHERE IN condition"""
        return self.where(column, 'in', values)
    
    def where_not_in(self, column: str, values: List[Any]) -> 'QueryBuilder':
        """Add WHERE NOT IN condition"""
        return self.where(column, 'not in', values)
    
    def where_null(self, column: str) -> 'QueryBuilder':
        """Add WHERE IS NULL condition"""
        return self.where(column, 'is null')
    
    def where_not_null(self, column: str) -> 'QueryBuilder':
        """Add WHERE IS NOT NULL condition"""
        return self.where(column, 'is not null')
    
    def where_between(self, column: str, low: Any, high: Any) -> 'QueryBuilder':
        """Add WHERE BETWEEN condition"""
        self._filters.append(
            getattr(self.model_class, column).between(low, high)
        )
        return self
    
    def where_not_between(self, column: str, low: Any, high: Any) -> 'QueryBuilder':
        """Add WHERE NOT BETWEEN condition"""
        self._filters.append(
            not_(getattr(self.model_class, column).between(low, high))
        )
        return self
    
    def where_like(self, column: str, pattern: str, case_sensitive: bool = True) -> 'QueryBuilder':
        """
        Add WHERE LIKE condition.
        
        Args:
            column: Column name
            pattern: Pattern to match (use % for wildcards)
            case_sensitive: Whether match should be case-sensitive
            
        Returns:
            Self for chaining
        """
        col = getattr(self.model_class, column)
        if case_sensitive:
            self._filters.append(col.like(pattern))
        else:
            self._filters.append(col.ilike(pattern))
        return self
    
    def where_starts_with(self, column: str, prefix: str, case_sensitive: bool = True) -> 'QueryBuilder':
        """Add WHERE column STARTS WITH condition"""
        return self.where_like(column, f"{prefix}%", case_sensitive)
    
    def where_ends_with(self, column: str, suffix: str, case_sensitive: bool = True) -> 'QueryBuilder':
        """Add WHERE column ENDS WITH condition"""
        return self.where_like(column, f"%{suffix}", case_sensitive)
    
    def where_contains(self, column: str, substring: str, case_sensitive: bool = True) -> 'QueryBuilder':
        """Add WHERE column CONTAINS condition"""
        return self.where_like(column, f"%{substring}%", case_sensitive)
    
    def where_date(self, column: str, operator: str, date_value: Any) -> 'QueryBuilder':
        """Add WHERE condition on date part only"""
        col = getattr(self.model_class, column)
        date_col = func.date(col)
        return self.where(lambda m: self._build_raw_condition(date_col, operator, date_value))
    
    def where_year(self, column: str, operator: str, year: int) -> 'QueryBuilder':
        """Add WHERE condition on year part"""
        col = getattr(self.model_class, column)
        year_col = func.extract('year', col)
        return self.where(lambda m: self._build_raw_condition(year_col, operator, year))
    
    def where_month(self, column: str, operator: str, month: int) -> 'QueryBuilder':
        """Add WHERE condition on month part"""
        col = getattr(self.model_class, column)
        month_col = func.extract('month', col)
        return self.where(lambda m: self._build_raw_condition(month_col, operator, month))
    
    # ========================================================================
    # NESTED CONDITIONS (Advanced)
    # ========================================================================
    
    def where_nested(self, callback: Callable[['QueryBuilder'], None], logic: str = 'and') -> 'QueryBuilder':
        """
        Add nested WHERE conditions with parentheses.
        
        Args:
            callback: Function that receives a QueryBuilder and adds conditions
            logic: 'and' or 'or' to combine with existing conditions
            
        Returns:
            Self for chaining
            
        Example:
            .where('status', 'active')
            .where_nested(lambda q: q.where('age', '>', 18).or_where('verified', True))
            # SQL: WHERE status = 'active' AND (age > 18 OR verified = True)
        """
        nested_builder = QueryBuilder(self.model_class)
        callback(nested_builder)
        
        nested_conditions = nested_builder._compile_filters()
        if nested_conditions is not None:
            if logic == 'or':
                self._or_filters.append(nested_conditions)
            else:
                self._filters.append(nested_conditions)
        
        return self
    
    def or_where_nested(self, callback: Callable[['QueryBuilder'], None]) -> 'QueryBuilder':
        """Add nested WHERE conditions with OR logic"""
        return self.where_nested(callback, logic='or')
    
    # ========================================================================
    # ORDERING
    # ========================================================================
    
    def order_by(self, column: str, direction: str = 'asc') -> 'QueryBuilder':
        """
        Add ORDER BY clause.
        
        Args:
            column: Column name
            direction: 'asc' or 'desc'
            
        Returns:
            Self for chaining
        """
        column_obj = getattr(self.model_class, column)
        if direction.lower() == 'asc':
            self._order_by_list.append(column_obj.asc())
        else:
            self._order_by_list.append(column_obj.desc())
        return self
    
    def order_by_desc(self, column: str) -> 'QueryBuilder':
        """Add ORDER BY DESC"""
        return self.order_by(column, 'desc')
    
    def order_by_raw(self, sql: str) -> 'QueryBuilder':
        """Add raw ORDER BY clause"""
        self._order_by_list.append(db.text(sql))
        return self
    
    def latest(self, column: str = 'created_at') -> 'QueryBuilder':
        """Order by column descending (newest first)"""
        return self.order_by(column, 'desc')
    
    def oldest(self, column: str = 'created_at') -> 'QueryBuilder':
        """Order by column ascending (oldest first)"""
        return self.order_by(column, 'asc')
    
    # ========================================================================
    # LIMIT & OFFSET
    # ========================================================================
    
    def limit(self, limit: int) -> 'QueryBuilder':
        """Limit number of results"""
        self._limit_value = limit
        return self
    
    def take(self, count: int) -> 'QueryBuilder':
        """Alias for limit"""
        return self.limit(count)
    
    def offset(self, offset: int) -> 'QueryBuilder':
        """Offset results"""
        self._offset_value = offset
        return self
    
    def skip(self, count: int) -> 'QueryBuilder':
        """Alias for offset"""
        return self.offset(count)
    
    # ========================================================================
    # RELATIONSHIPS
    # ========================================================================
    
    def with_relations(self, *relations: str) -> 'QueryBuilder':
        """
        Eager load relationships to avoid N+1 queries.
        
        Args:
            *relations: Relationship names
            
        Returns:
            Self for chaining
            
        Example:
            .with_relations('author', 'comments')
        """
        self._relations.extend(relations)
        return self
    
    def with_(self, *relations: str) -> 'QueryBuilder':
        """Alias for with_relations"""
        return self.with_relations(*relations)
    
    # ========================================================================
    # SELECT
    # ========================================================================
    
    def select(self, *fields: str) -> 'QueryBuilder':
        """
        Select specific fields only.
        
        Args:
            *fields: Field names
            
        Returns:
            Self for chaining
        """
        self._select_fields = [getattr(self.model_class, f) for f in fields]
        return self
    
    def distinct(self, *columns: str) -> 'QueryBuilder':
        """
        Select distinct values.
        
        Args:
            *columns: Optional columns for DISTINCT ON (PostgreSQL)
            
        Returns:
            Self for chaining
        """
        self._distinct_value = True
        if columns:
            self._select_fields = [getattr(self.model_class, c) for c in columns]
        return self
    
    # ========================================================================
    # AGGREGATIONS
    # ========================================================================
    
    def group_by(self, *columns: str) -> 'QueryBuilder':
        """
        Add GROUP BY clause.
        
        Args:
            *columns: Column names to group by
            
        Returns:
            Self for chaining
        """
        self._group_by_fields.extend([
            getattr(self.model_class, col) for col in columns
        ])
        return self
    
    def having(self, condition) -> 'QueryBuilder':
        """
        Add HAVING clause (use with group_by).
        
        Args:
            condition: SQLAlchemy condition
            
        Returns:
            Self for chaining
        """
        self._having_conditions.append(condition)
        return self
    
    def sum(self, column: str) -> float:
        """Calculate sum of column"""
        self._apply_filters()
        return self._query.with_entities(
            func.sum(getattr(self.model_class, column))
        ).scalar() or 0
    
    def avg(self, column: str) -> float:
        """Calculate average of column"""
        self._apply_filters()
        return self._query.with_entities(
            func.avg(getattr(self.model_class, column))
        ).scalar() or 0
    
    def min(self, column: str) -> Any:
        """Get minimum value of column"""
        self._apply_filters()
        return self._query.with_entities(
            func.min(getattr(self.model_class, column))
        ).scalar()
    
    def max(self, column: str) -> Any:
        """Get maximum value of column"""
        self._apply_filters()
        return self._query.with_entities(
            func.max(getattr(self.model_class, column))
        ).scalar()
    
    # ========================================================================
    # EXECUTION
    # ========================================================================
    
    def get(self) -> List[Any]:
        """Execute query and get all results"""
        self._apply_filters()
        return self._query.all()
    
    def all(self) -> List[Any]:
        """Alias for get()"""
        return self.get()
    
    def first(self) -> Optional[Any]:
        """Get first result or None"""
        self._apply_filters()
        return self._query.first()
    
    def first_or_fail(self) -> Any:
        """
        Get first result or raise exception.
        
        Returns:
            First result
            
        Raises:
            ValueError: If no results found
        """
        result = self.first()
        if result is None:
            raise ValueError(f"No {self.model_class.__name__} found matching query")
        return result
    
    def find(self, id: Any) -> Optional[Any]:
        """Find by primary key"""
        return db.session.get(self.model_class, id)
    
    def find_or_fail(self, id: Any) -> Any:
        """Find by primary key or raise exception"""
        result = self.find(id)
        if result is None:
            raise ValueError(f"{self.model_class.__name__} with ID {id} not found")
        return result
    
    def count(self) -> int:
        """Get count of results"""
        self._apply_filters()
        return self._query.count()
    
    def exists(self) -> bool:
        """Check if any records exist"""
        return self.count() > 0
    
    def doesnt_exist(self) -> bool:
        """Check if no records exist"""
        return not self.exists()
    
    def paginate(self, page: int = 1, per_page: int = 20, error_out: bool = False):
        """
        Paginate results.
        
        Args:
            page: Page number (1-indexed)
            per_page: Items per page
            error_out: Whether to raise error on invalid page
            
        Returns:
            Pagination object with items and metadata
        """
        self._apply_filters()
        return self._query.paginate(page=page, per_page=per_page, error_out=error_out)
    
    def chunk(self, chunk_size: int, callback: Callable[[List[Any]], None]) -> None:
        """
        Process results in chunks (memory efficient for large datasets).
        
        Args:
            chunk_size: Number of items per chunk
            callback: Function to process each chunk
        """
        offset = 0
        while True:
            builder = self._clone()
            builder.limit(chunk_size).offset(offset)
            chunk = builder.get()
            
            if not chunk:
                break
            
            callback(chunk)
            offset += chunk_size
    
    def pluck(self, column: str) -> List[Any]:
        """
        Get list of values from a single column.
        
        Args:
            column: Column name
            
        Returns:
            List of column values
        """
        self._apply_filters()
        col = getattr(self.model_class, column)
        return [row[0] for row in self._query.with_entities(col).all()]
    
    def value(self, column: str) -> Any:
        """Get single value from first row"""
        self._apply_filters()
        col = getattr(self.model_class, column)
        result = self._query.with_entities(col).first()
        return result[0] if result else None
    
    # ========================================================================
    # MUTATIONS
    # ========================================================================
    
    def update(self, **attributes) -> int:
        """
        Update matching records.
        
        Args:
            **attributes: Attributes to update
            
        Returns:
            Number of updated records
        """
        try:
            self._apply_filters()
            count = self._query.update(attributes, synchronize_session=False)
            db.session.commit()
            return count
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to update: {str(e)}")
    
    def delete(self) -> int:
        """
        Delete matching records.
        
        Returns:
            Number of deleted records
        """
        try:
            self._apply_filters()
            count = self._query.delete(synchronize_session=False)
            db.session.commit()
            return count
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Failed to delete: {str(e)}")
    
    def increment(self, column: str, amount: int = 1) -> int:
        """Increment column value"""
        col = getattr(self.model_class, column)
        return self.update(**{column: col + amount})
    
    def decrement(self, column: str, amount: int = 1) -> int:
        """Decrement column value"""
        col = getattr(self.model_class, column)
        return self.update(**{column: col - amount})
    
    # ========================================================================
    # INTERNAL HELPERS
    # ========================================================================
    
    def _build_condition(self, column: str, operator: str, value: Any):
        """Build individual condition based on operator"""
        column_obj = getattr(self.model_class, column)
        return self._build_raw_condition(column_obj, operator, value)
    
    def _build_raw_condition(self, column_obj, operator: str, value: Any):
        """Build condition from column object"""
        op = operator.lower() if isinstance(operator, str) else operator
        
        if op == '=' or op == 'eq':
            return column_obj == value
        elif op == '!=' or op == 'ne':
            return column_obj != value
        elif op == '>' or op == 'gt':
            return column_obj > value
        elif op == '>=' or op == 'gte':
            return column_obj >= value
        elif op == '<' or op == 'lt':
            return column_obj < value
        elif op == '<=' or op == 'lte':
            return column_obj <= value
        elif op == 'like':
            return column_obj.like(value)
        elif op == 'ilike':
            return column_obj.ilike(value)
        elif op == 'in':
            return column_obj.in_(value)
        elif op == 'not in':
            return column_obj.notin_(value)
        elif op == 'is null':
            return column_obj.is_(None)
        elif op == 'is not null':
            return column_obj.isnot(None)
        elif op == 'between':
            return column_obj.between(value[0], value[1])
        else:
            raise ValueError(f"Unsupported operator: {operator}")
    
    def _compile_filters(self):
        """Compile all filters into single condition"""
        all_conditions = []
        
        # Add AND filters
        if self._filters:
            all_conditions.append(and_(*self._filters))
        
        # Add OR filters
        if self._or_filters:
            all_conditions.append(or_(*self._or_filters))
        
        # Combine
        if not all_conditions:
            return None
        elif len(all_conditions) == 1:
            return all_conditions[0]
        else:
            return and_(*all_conditions)
    
    def _apply_filters(self):
        """Apply all filters, sorting, limits to query"""
        # Apply WHERE conditions
        combined_filters = self._compile_filters()
        if combined_filters is not None:
            self._query = self._query.filter(combined_filters)
        
        # Apply DISTINCT
        if self._distinct_value:
            self._query = self._query.distinct()
        
        # Apply SELECT
        if self._select_fields:
            self._query = self._query.with_entities(*self._select_fields)
        
        # Apply GROUP BY
        if self._group_by_fields:
            self._query = self._query.group_by(*self._group_by_fields)
        
        # Apply HAVING
        if self._having_conditions:
            for condition in self._having_conditions:
                self._query = self._query.having(condition)
        
        # Apply ORDER BY
        if self._order_by_list:
            for order in self._order_by_list:
                self._query = self._query.order_by(order)
        
        # Apply LIMIT
        if self._limit_value:
            self._query = self._query.limit(self._limit_value)
        
        # Apply OFFSET
        if self._offset_value:
            self._query = self._query.offset(self._offset_value)
        
        # Apply relationships (eager loading)
        if self._relations:
            self._query = self._query.options(*[
                joinedload(getattr(self.model_class, rel)) 
                for rel in self._relations
            ])
    
    def _clone(self) -> 'QueryBuilder':
        """Create a copy of this query builder"""
        clone = QueryBuilder(self.model_class)
        clone._query = self._query
        clone._filters = self._filters.copy()
        clone._or_filters = self._or_filters.copy()
        clone._relations = self._relations.copy()
        clone._select_fields = self._select_fields
        clone._group_by_fields = self._group_by_fields.copy()
        clone._having_conditions = self._having_conditions.copy()
        clone._order_by_list = self._order_by_list.copy()
        clone._limit_value = self._limit_value
        clone._offset_value = self._offset_value
        clone._distinct_value = self._distinct_value
        return clone
    
    def to_sql(self) -> str:
        """Get the SQL query string (for debugging)"""
        self._apply_filters()
        return str(self._query.statement.compile(
            compile_kwargs={"literal_binds": True}
        ))
    
    def dd(self):
        """Dump and die - print SQL and exit (debugging)"""
        print(self.to_sql())
        import sys
        sys.exit()
    
    def dump(self):
        """Print SQL query (debugging)"""
        print(self.to_sql())
        return self