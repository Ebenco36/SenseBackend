import os
import math
import logging
from sqlalchemy.exc import NoResultFound
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import Executable
from utils.errors import DatabaseError, RecordNotFoundError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class PostgresService:
    """Service class to interact with a PostgreSQL database using a fluent interface."""

    def __init__(self, database_url=None):
        db_url = database_url or os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError(
                "Database URL must be provided or set in DATABASE_URL environment variable.")
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://")
        self.engine = create_engine(db_url, pool_size=10, max_overflow=20)
        self.reset_query()

    def reset_query(self):
        """Resets the query builder to its initial state."""
        self._table = None
        self._columns = "*"
        self._conditions = []
        self._order_by = None
        self._group_by = None
        self._limit = None
        self._offset = None
        self._params = {}
        self._param_counter = 1

        self._table_columns = set()
        self._table_columns_lower = set()

        return self

    def _add_param(self, value):
        param_name = f"p{self._param_counter}"
        self._params[param_name] = value
        self._param_counter += 1
        return f":{param_name}"

    @staticmethod
    def _quote(identifier):
        if not isinstance(identifier, str):
            raise TypeError("Identifier must be a string.")
        if '"' in identifier or ';' in identifier:
            raise ValueError(f"Invalid characters in identifier: {identifier}")
        return f'"{identifier}"'

    def table(self, table_name="all_db"):
        unquoted_name = table_name.strip('"')
        # This is the exact line where the attribute is set
        self._table_columns = self.get_column_names(unquoted_name)
        self._table_columns_lower = {c.lower() for c in self._table_columns}

        self._table = self._quote(table_name)
        return self

    def select(self, *columns):
        """
        Selects columns for the query, now silently ignoring any that don't exist.
        """
        if not self._table:
            raise ValueError("Table must be set before selecting columns.")

        if not columns:
            self._columns = "*"
            return self

        # FIX: Filter the requested columns against the valid columns for the table.
        valid_columns = [col for col in columns if col in self._table_columns]

        if not valid_columns:
            # If no valid columns were found after filtering, raise an error.
            raise ValueError(
                f"None of the requested columns exist on table '{self._table}'.")

        self._columns = ", ".join(self._quote(col) for col in valid_columns)
        return self

    def _add_condition(self, column, operator, value, conjunction="AND"):
        """Generic method to add a condition, handling data type mismatches for specific columns."""
        quoted_column = self._quote(column)

        # FINAL FIX: If the column is 'Id' (a TEXT column) and the comparison value is an integer,
        # convert the value to a string to ensure a proper text-to-text comparison.
        processed_value = value
        if column.lower() in ('id', 'primary_id') and isinstance(value, int):
            processed_value = str(value)

        placeholder = self._add_param(
            processed_value) if processed_value is not None else 'NULL'
        op = operator if processed_value is not None else 'IS'

        self._conditions.append(
            (conjunction, f"{quoted_column} {op} {placeholder}"))
        return self

    def where(self, column, value, operator="="):
        return self._add_condition(column, operator, value, "AND")

    # def or_where(self, column, value, operator="="):
    #     return self._add_condition(column, operator, value, "OR")

    def like(self, column, value, conjunction="AND", case_sensitive=False):
        quoted_column = self._quote(column)
        placeholder = self._add_param(f"%{value}%")
        # Use ILIKE for case-insensitive
        like_operator = "LIKE" if case_sensitive else "ILIKE"
        self._conditions.append(
            (conjunction, f"{quoted_column} {like_operator} {placeholder}"))
        return self

    def where(self, column, value, operator="=", conjunction="AND"):
        return self._add_condition(column, operator, value, conjunction)

    def in_where(self, column, values, conjunction="AND"):
        # The logic was correct, just needed to ensure the signature is consistent
        placeholders = ", ".join([self._add_param(v) for v in values])
        self._conditions.append(
            (conjunction, f"{self._quote(column)} IN ({placeholders})"))
        return self

    def between(self, column, start, end, conjunction="AND"):
        start_ph, end_ph = self._add_param(start), self._add_param(end)
        self._conditions.append(
            (conjunction, f"{self._quote(column)} BETWEEN {start_ph} AND {end_ph}"))
        return self

    def order_by(self, column, direction="ASC"):
        self._order_by = f"{self._quote(column)} {'DESC' if direction.upper() == 'DESC' else 'ASC'}"
        return self

    def group_by(self, *columns):
        self._group_by = ", ".join(self._quote(col) for col in columns)
        return self

    def paginate(self, page=1, page_size=10):
        self._limit = page_size
        self._offset = (int(page) - 1) * int(page_size)
        return self

    def add_aggregation(self, func, column, alias=None):
        col_str = self._quote(column) if column != '*' else '*'
        agg_str = f"{func.upper()}({col_str})"
        if alias:
            agg_str += f" AS {self._quote(alias)}"
        self._columns = agg_str if self._columns == "*" else f"{self._columns}, {agg_str}"
        return self

    def or_where(self, column, value, operator="="):
        """
        Adds an OR condition to the query.
        """
        return self._add_condition(column, operator, value, "OR")

    def where_group(self, conditions, conjunction="AND"):
        """
        Adds a group of conditions with a specified conjunction (AND/OR).
        """
        if not conditions:
            return self

        group_conditions = " AND ".join(conditions)
        self._conditions.append((conjunction, f"({group_conditions})"))
        return self

    def where_group_start(self, conjunction="AND"):
        # Skip the conjunction if None or it's the first condition
        if conjunction is None or not self._conditions:
            self._conditions.append(("", "("))
        else:
            self._conditions.append((conjunction, "("))
        return self

    def where_group_end(self):
        """Adds a closing parenthesis to end a condition group."""
        self._conditions.append(("", ")"))
        return self

    def _build_query(self, is_count=False):
        """Builds the final query string, now with validation for DISTINCT/ORDER BY conflicts."""

        if not self._table:
            self._table = "all_db"
            # raise ValueError("Table name must be specified.")

        cols = "COUNT(*)" if is_count else self._columns
        query = [f"SELECT {cols} FROM {self._table}"]

        if self._conditions:
            conj, clause = self._conditions[0]
            query.append(f"WHERE {clause}")

            for i in range(1, len(self._conditions)):
                conj, clause = self._conditions[i]
                conj = conj if conj else ""
                query.append(f"{conj} {clause}")

        if not is_count:
            if self._group_by:
                query.append(f"GROUP BY {self._group_by}")

            final_order_by = self._order_by
            # Check if this is a DISTINCT query with an ORDER BY clause
            if self._columns.lstrip().upper().startswith('DISTINCT') and self._order_by:
                # Simple parsing to get the column(s) from the DISTINCT clause
                distinct_cols_str = self._columns.lstrip()[8:].strip()
                distinct_cols = {col.strip().strip('"')
                                 for col in distinct_cols_str.split(',')}

                # Parse the column from the ORDER BY clause
                order_by_col = self._order_by.split()[0].strip().strip('"')

                # If the ORDER BY column is NOT in the DISTINCT list, it's an invalid query.
                if order_by_col.lower() not in {c.lower() for c in distinct_cols}:
                    # Correct the query by overriding the ORDER BY to use the first DISTINCT column.
                    first_distinct_col = list(distinct_cols)[0]
                    # logging.warning(
                    #     f"ORDER BY column '{order_by_col}' is not in SELECT DISTINCT list. "
                    #     f"Overriding to order by '{first_distinct_col}'."
                    # )
                    final_order_by = f"{self._quote(first_distinct_col)} ASC"

            if final_order_by:
                query.append(f"ORDER BY {final_order_by}")
            # --- END OF FIX ---

            if self._limit is not None:
                query.append(f"LIMIT {self._limit}")
            if self._offset is not None:
                query.append(f"OFFSET {self._offset}")

        return " ".join(query)

    def show_sql(self):
        """Returns the generated SQL query and its parameters for debugging."""
        query_str = self._build_query()
        return {"query": query_str, "params": self._params}

    def execute_raw_query(self, query, params=None):
        try:
            statement = query if isinstance(query, Executable) else text(query)

            with self.engine.begin() as conn:
                result = conn.execute(statement, params or {})
                return [dict(row) for row in result.mappings()] if result.returns_rows else []

            # with self.engine.begin() as conn:
            #     result = conn.execute(text(query), params or {})
            #     return [dict(row) for row in result.mappings()] if result.returns_rows else []
        except NoResultFound:
            # This is a specific error, e.g., from .scalar_one()
            raise RecordNotFoundError("The requested record was not found.")
        except SQLAlchemyError as e:
            # Log the detailed, technical error for developers
            logging.error(
                f"Database query failed! SQL: {query} PARAMS: {params}")
            # Raise a clean, generic error for the API layer
            raise DatabaseError("A database error occurred.") from e

    def get(self):
        """
        Fetches records, with robust state management to ensure reset is always called.
        """
        try:
            # Check for table existence early.
            if not self._table:
                raise ValueError(
                    "Table name must be specified before calling .get()")

            is_paginated = self._limit is not None and self._offset is not None

            records_query = self._build_query()
            records = self.execute_raw_query(records_query, self._params)

            if not is_paginated:
                return records

            # If paginated, we must have a limit.
            page_size = self._limit
            if page_size is None or page_size <= 0:
                # This is a defensive check against corrupted state.
                page_size = 10
                # raise ValueError("Invalid page size for pagination.")

            count_query = self._build_query(is_count=True)
            if self._group_by:
                count_query = f"SELECT COUNT(*) FROM ({records_query}) as subquery"

            total_records_result = self.execute_raw_query(
                count_query, self._params)
            total_records = total_records_result[0]['count'] if total_records_result else 0
            if records:
                return {
                    "records": records,
                    "pagination": {
                        "total_records": total_records,
                        "total_pages": math.ceil(total_records / page_size),
                        "current_page": ((self._offset if self._offset else 0) // page_size) + 1,
                        "page_size": page_size,
                    }
                }
            else:
                return {}
        finally:
            self.reset_query()

    # def first(self):
    #     self._limit = 1
    #     records = self.get()
    #     return records[0] if records else None

    def first(self):
        """
        Efficiently fetches only the first result of the query by running a
        single database query.
        """
        # 1. Set the limit to 1 to ensure the database only returns one row.
        self._limit = 1

        # 2. Build the specific query string (e.g., SELECT ... WHERE ... LIMIT 1)
        query = self._build_query()

        # 3. Execute the query directly. This avoids the .get() method and its
        #    unnecessary second COUNT(*) query.
        results = self.execute_raw_query(query, self._params)

        # 4. Reset the builder so the next query starts fresh.
        self.reset_query()

        # 5. Return the single record if found, otherwise return None.
        return results[0] if results else None

    # --- New Methods Added for JSONService ---
    def add_record(self, data):
        columns = ", ".join(self._quote(k) for k in data.keys())
        placeholders = ", ".join(f":{k}" for k in data.keys())
        query = f"INSERT INTO {self._table} ({columns}) VALUES ({placeholders})"
        # The actual execution and commit happens here
        self.execute_raw_query(query, data)
        return True

    def update_record(self, record_id, data, pk_column='id'):
        set_clause = ", ".join(f"{self._quote(k)} = :{k}" for k in data.keys())
        query = f"UPDATE {self._table} SET {set_clause} WHERE {self._quote(pk_column)} = :record_id"
        self.execute_raw_query(query, {**data, "record_id": record_id})
        return True

    def delete_record(self, record_id, pk_column='id'):
        query = f"DELETE FROM {self._table} WHERE {self._quote(pk_column)} = :record_id"
        self.execute_raw_query(query, {"record_id": record_id})
        return True

    def get_column_names(self, table_name):
        query = "SELECT column_name FROM information_schema.columns WHERE table_name = :table"
        results = self.execute_raw_query(query, {"table": table_name})
        return [row['column_name'] for row in results]

    def _validate_columns(self, *columns):
        """Checks if provided column names exist in the current table's schema."""
        if not self._table:
            raise ValueError("Table must be set before validating columns.")

        for col in columns:
            if col not in self._table_columns and col != '*':
                raise ValueError(
                    f"Column '{col}' does not exist on table '{self._table}'.")

    def or_like_group(self, column, values, conjunction="AND"):
        """
        Adds a group of OR'd LIKE conditions, e.g., (col LIKE %v1% OR col LIKE %v2%).
        """
        if not values:
            return self

        # self._validate_columns(column)
        like_operator = "ILIKE"  # Use ILIKE for case-insensitive matching by default

        # Build a list of individual 'col ILIKE :param' clauses
        like_clauses = []
        for value in values:
            placeholder = self._add_param(f"%{value}%")
            like_clauses.append(
                f"{self._quote(column)} {like_operator} {placeholder}")

        # Join them with OR and wrap in parentheses
        group_clause = f"({ ' OR '.join(like_clauses) })"
        self._conditions.append((conjunction, group_clause))
        return self

    def or_like_multi_column(self, columns, value, conjunction="AND"):
        """
        Adds a grouped OR LIKE clause across multiple columns for a single value.
        e.g., (col1 ILIKE '%val%' OR col2 ILIKE '%val%')
        """
        if not columns or not value:
            return self

        self._validate_columns(*columns)
        like_operator = "ILIKE"  # Case-insensitive
        placeholder = self._add_param(f"%{value}%")

        # Create an OR'd list of LIKE conditions for each column
        like_clauses = [
            f"{self._quote(col)} {like_operator} {placeholder}" for col in columns]

        # Join them and wrap in parentheses for correct SQL precedence
        group_clause = f"({ ' OR '.join(like_clauses) })"
        self._conditions.append((conjunction, group_clause))
        return self

    # def get_unique_items_from_column(self, table_name, column_name):
    #     """
    #     Fetches a sorted list of unique items from a column, ensuring the
    #     ORDER BY clause is valid.
    #     """
    #     self.table(table_name)
    #     if column_name.lower() not in self._table_columns_lower:
    #         # logging.warning(f"Column '{column_name}' not found in '{table_name}'. Returning empty list.")
    #         self.reset_query()
    #         return []

    #     self._columns = f'DISTINCT {self._quote(column_name)}'

    #     # that is being selected with DISTINCT.
    #     query = self._build_query() + \
    #         f' ORDER BY {self._quote(column_name)} ASC'

    #     results = self.execute_raw_query(query, self._params)
    #     self.reset_query()
    #     return [row[column_name] for row in results]

    def get_unique_items_from_column(self, table_name, column_name):
        """
        Fetches a sorted list of unique items from a column, ensuring the
        ORDER BY clause is valid and applied only once.
        """
        self.table(table_name)
        if column_name.lower() not in self._table_columns_lower:
            self.reset_query()
            return []

        self._columns = f'DISTINCT {self._quote(column_name)}'
        self.order_by(column_name, "ASC")
        query = self._build_query()

        results = self.execute_raw_query(query, self._params)
        self.reset_query()
        return [row[column_name] for row in results]
