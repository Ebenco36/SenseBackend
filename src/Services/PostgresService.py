from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
# from app import db, app
from dotenv import load_dotenv
import math
import os


class PostgresService:
    """Service class to interact with a PostgreSQL database using method chaining."""

    def __init__(self):
        """
        Initialize the service with a database URL.
        """
        db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(db_url, pool_size=10, max_overflow=20)
        self.reset_query()

    def reset_query(self):
        """Reset query parameters to their initial state."""
        self._table = None
        self._columns = "*"
        self._where_clauses = []
        self._order_by = None
        self._limit = None
        self._offset = None
        self._params = {}
        self._page = None
        self._page_size = None
        return self

    @staticmethod
    def quote_identifier(identifier):
        """Quote an identifier to preserve case sensitivity."""
        if isinstance(identifier, str):
            return f'"{identifier}"'
        return identifier

    def table(self, table_name):
        """Specify the table for the query."""
        self._table = self.quote_identifier(table_name)
        return self

    def add_record(self, data):
        """
        Insert a new record into the table.
        Args:
            data (dict): A dictionary of column-value pairs to insert.
        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        if not self._table:
            raise ValueError("Table name must be specified.")
        if not data:
            raise ValueError("Data to insert cannot be empty.")

        columns = ", ".join(self.quote_identifier(k) for k in data.keys())
        placeholders = ", ".join(f":{k}" for k in data.keys())
        query = f"INSERT INTO {self._table} ({columns}) VALUES ({placeholders})"

        try:
            with self.engine.connect() as conn:
                conn.execute(text(query), data)
            return True
        except SQLAlchemyError as e:
            print(f"Error inserting record: {e}")
            return False

    def update_record(self, record_id, data):
        """
        Update an existing record in the table by ID.
        Args:
            record_id (int): The ID of the record to update.
            data (dict): A dictionary of column-value pairs to update.
        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        if not self._table:
            raise ValueError("Table name must be specified.")
        if not data:
            raise ValueError("Data to update cannot be empty.")

        set_clause = ", ".join(f"{self.quote_identifier(k)} = :{k}" for k in data.keys())
        query = f"UPDATE {self._table} SET {set_clause} WHERE id = :record_id"

        try:
            with self.engine.connect() as conn:
                conn.execute(text(query), {**data, "record_id": record_id})
            return True
        except SQLAlchemyError as e:
            print(f"Error updating record: {e}")
            return False

    def delete_record(self, record_id):
        """
        Delete a record from the table by ID.
        Args:
            record_id (int): The ID of the record to delete.
        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        if not self._table:
            raise ValueError("Table name must be specified.")

        query = f"DELETE FROM {self._table} WHERE id = :record_id"

        try:
            with self.engine.connect() as conn:
                conn.execute(text(query), {"record_id": record_id})
            return True
        except SQLAlchemyError as e:
            print(f"Error deleting record: {e}")
            return False
        
        
    def select(self, *columns):
        """Specify columns to select."""
        self._columns = ", ".join(self.quote_identifier(col) for col in columns) if columns else "*"
        return self

    def where(self, column, value):
        """Add a WHERE clause."""
        column = self.quote_identifier(column)
        self._where_clauses.append(f"{column} = :{column}")
        self._params[column] = value
        return self

    def notWhere(self, column, value):
        """Add a NOT condition."""
        column = self.quote_identifier(column)
        self._where_clauses.append(f"{column} != :{column}_not")
        self._params[f"{column}_not"] = value
        return self

    def orWhere(self, column, value):
        """Add an OR condition."""
        column = self.quote_identifier(column)
        if self._where_clauses:
            self._where_clauses[-1] = f"({self._where_clauses[-1]} OR {column} = :{column})"
        else:
            self._where_clauses.append(f"{column} = :{column}")
        self._params[column] = value
        return self

    def likeWhere(self, column, value):
        """Add a LIKE clause."""
        column = self.quote_identifier(column)
        self._where_clauses.append(f"{column} LIKE :{column}")
        self._params[column] = f"%{value}%"
        return self

    def inWhere(self, column, values):
        """Add an IN condition."""
        column = self.quote_identifier(column)
        param_name = f"{column}_in"
        placeholders = ", ".join([f":{param_name}_{i}" for i in range(len(values))])
        self._where_clauses.append(f"{column} IN ({placeholders})")
        for i, value in enumerate(values):
            self._params[f"{param_name}_{i}"] = value
        return self

    def betweenWhere(self, column, start, end):
        """Add a BETWEEN condition."""
        column = self.quote_identifier(column)
        self._where_clauses.append(f"{column} BETWEEN :{column}_start AND :{column}_end")
        self._params[f"{column}_start"] = start
        self._params[f"{column}_end"] = end
        return self

    def orderBy(self, column, direction="ASC"):
        """Specify the ORDER BY clause."""
        column = self.quote_identifier(column)
        self._order_by = f"{column} {direction.upper()}"
        return self

    def limit(self, limit):
        """Set a LIMIT for the query."""
        self._limit = limit
        return self

    def offset(self, offset):
        """Set an OFFSET for the query."""
        self._offset = offset
        return self

    def paginate(self, page, page_size):
        """Add pagination to the query."""
        self._page = page
        self._page_size = page_size
        self._limit = page_size
        self._offset = (page - 1) * page_size
        return self

    def build_query(self):
        """Build the final query."""
        if not self._table:
            raise ValueError("Table name must be specified.")

        query = f"SELECT {self._columns} FROM {self._table}"

        if self._where_clauses:
            query += f" WHERE {' AND '.join(self._where_clauses)}"

        if self._order_by:
            query += f" ORDER BY {self._order_by}"

        if self._limit is not None:
            query += f" LIMIT {self._limit}"

        if self._offset is not None:
            query += f" OFFSET {self._offset}"

        return query

    def get_total_records(self):
        """Get the total number of records matching the conditions."""
        query = f"SELECT COUNT(*) FROM {self._table}"
        if self._where_clauses:
            query += f" WHERE {' AND '.join(self._where_clauses)}"
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), self._params)
                return result.scalar()
        except SQLAlchemyError as e:
            print(f"Error counting total records: {e}")
            return 0

    def execute(self):
        """Execute the built query and fetch results."""
        query = self.build_query()
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), self._params)
                return [dict(row) for row in result]
        except SQLAlchemyError as e:
            print(f"Error executing query: {e}")
            return []

    def get(self):
        """
        Fetch all results and include pagination metadata if applicable.
        Returns:
            dict: Contains records and metadata if pagination is applied.
        """
        records = self.execute()
        if self._page and self._page_size:
            total_records = self.get_total_records()
            total_pages = math.ceil(total_records / self._page_size)
            return {
                "records": records,
                "total_records": total_records,
                "total_pages": total_pages,
                "current_page": self._page,
                "page_size": self._page_size,
            }
        return {"records": records}

    def first(self):
        """Fetch the first result."""
        self._limit = 1
        results = self.execute()
        return results[0] if results else None
