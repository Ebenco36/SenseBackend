from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
# from app import db, app
from dotenv import load_dotenv
import math
import os
import pandas as pd
import altair as alt
import math
from vega_datasets import data
from iso3166 import countries as iso_countries

class QueryHelper:
    """Helper class for building SQL queries and parameters."""

    @staticmethod
    def build_query(raw_input, table, order_by=None, pagination=None, additional_conditions=None):
        """
        Build a SQL query string and its parameters based on raw input and additional conditions.
        This version makes all string comparisons case-insensitive by wrapping columns and parameters in LOWER().
        """
        where_clauses = []
        params = {}

        # Construct WHERE clauses for raw_input
        for column, values in raw_input.items():
            col_expr = f"LOWER(\"{column}\")"
            if len(values) > 1:
                # Prepare IN clause with case-insensitive matching
                placeholders = []
                for i, value in enumerate(values):
                    key = f"{column}_in_{i}"
                    placeholders.append(f":{key}")
                    params[key] = value.lower() if isinstance(value, str) else value
                where_clauses.append(f"{col_expr} IN ({', '.join(placeholders)})")
            else:
                key = column
                params[key] = values[0].lower() if isinstance(values[0], str) else values[0]
                where_clauses.append(f"{col_expr} = :{key}")

        # Add additional_conditions to WHERE clauses
        if additional_conditions:
            for condition in additional_conditions:
                column = condition["column"]
                value = condition["value"]
                condition_type = condition.get("type", "where").lower()
                col_expr = f"LOWER(\"{column}\")"

                if condition_type == "where":
                    key = f"{column}_additional"
                    params[key] = value.lower() if isinstance(value, str) else value
                    where_clauses.append(f"{col_expr} = :{key}")

                elif condition_type == "orwhere":
                    key = f"{column}_additional"
                    params[key] = value.lower() if isinstance(value, str) else value
                    clause = f"{col_expr} = :{key}"
                    if where_clauses:
                        where_clauses[-1] = f"({where_clauses[-1]} OR {clause})"
                    else:
                        where_clauses.append(clause)

                elif condition_type == "likewhere":
                    key = f"{column}_additional"
                    params[key] = f"%{value.lower()}%" if isinstance(value, str) else value
                    where_clauses.append(f"{col_expr} LIKE :{key}")

                elif condition_type == "notwhere":
                    key = f"{column}_additional"
                    params[key] = value.lower() if isinstance(value, str) else value
                    where_clauses.append(f"{col_expr} != :{key}")

                elif condition_type == "betweenwhere":
                    start, end = value
                    key_start = f"{column}_start"
                    key_end = f"{column}_end"
                    params[key_start] = start
                    params[key_end] = end
                    where_clauses.append(f"\"{column}\" BETWEEN :{key_start} AND :{key_end}")

                elif condition_type == "orlikewhere":
                    key = f"{column}_additional"
                    params[key] = f"%{value.lower()}%" if isinstance(value, str) else value
                    clause = f"{col_expr} LIKE :{key}"
                    if where_clauses:
                        where_clauses[-1] = f"({where_clauses[-1]} OR {clause})"
                    else:
                        where_clauses.append(clause)

        # Start building the query
        query = f'SELECT * FROM "{table}"'
        if where_clauses:
            query += f" WHERE {' AND '.join(where_clauses)}"

        # Add ORDER BY
        if order_by:
            query += f' ORDER BY "{order_by[0]}" {order_by[1].upper()}'

        # Add pagination (LIMIT and OFFSET)
        if pagination:
            page = pagination.get("page", 1)
            page_size = pagination.get("page_size", 10)
            offset = (page - 1) * page_size
            query += f" LIMIT {page_size} OFFSET {offset}"

        return query, params


    @staticmethod
    def build_count_query(raw_input, table, additional_conditions=None):
        """
        Build a SQL COUNT query string and its parameters.

        :param raw_input: Dictionary where keys are column names and values are lists of terms.
        :param table: Table name for the query.
        :param additional_conditions: List of dictionaries specifying conditions and types.
        :return: A tuple of (count query string, parameters dictionary).
        """
        query, params = QueryHelper.build_query(raw_input, table, additional_conditions=additional_conditions)
        count_query = query.replace("SELECT *", "SELECT COUNT(*)")
        return count_query, params
    

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
        if set_clause and set_clause != "":
            query = f"UPDATE {self._table} SET {set_clause} WHERE id = :record_id"

            try:
                with self.engine.connect() as conn:
                    conn.execute(text(query), {**data, "record_id": record_id})
                return True
            except SQLAlchemyError as e:
                print(f"Error updating record: {e}")
            return False
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

    def likeWhere(self, column, value, linkage="AND"):
        """Add a LIKE clause, casting numeric columns like 'Year' to text if needed."""
        quoted_column = self.quote_identifier(column)

        # Detect columns that need casting
        cast_to_text = column.lower() in {"year", "record_id", "id"}  # Add more if needed

        if cast_to_text:
            clause = f'CAST({quoted_column} AS TEXT) LIKE :{column}_additional'
        else:
            clause = f'{quoted_column} LIKE :{column}_additional'

        if linkage == "AND":
            self._where_clauses.append(clause)
        else:
            self._where_clauses[-1] = f"({self._where_clauses[-1]} OR {clause})"

        self._params[f"{column}_additional"] = f"%{value}%"
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

    def groupBy(self, *columns):
        """
        Add a GROUP BY clause to the query.
        
        :param columns: Columns to group by.
        :return: self (for method chaining).
        """
        grouped_columns = ", ".join(self.quote_identifier(col) for col in columns)
        self._group_by = grouped_columns
        return self

    def add_aggregation(self, func, column, alias=None):
        """
        Add an aggregation function (e.g., SUM, AVG, COUNT) to the query.
        
        :param func: Aggregation function (e.g., SUM, AVG, COUNT).
        :param column: The column to aggregate.
        :param alias: Optional alias for the aggregated column.
        :return: self (for method chaining).
        """
        column = self.quote_identifier(column)
        agg_column = f"{func}({column})"
        if alias:
            agg_column += f" AS {self.quote_identifier(alias)}"
        if self._columns == "*":
            self._columns = agg_column
        else:
            self._columns += f", {agg_column}"
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

        if hasattr(self, '_group_by') and self._group_by:
            query += f" GROUP BY {self._group_by}"
        
        if self._order_by:
            query += f" ORDER BY {self._order_by}"

        if self._limit is not None:
            query += f" LIMIT {self._limit}"

        if self._offset is not None:
            query += f" OFFSET {self._offset}"

        return query

    def from_json(self, query_json):
        """
        Populate the query builder from a JSON query structure.
        
        :param query_json: JSON object representing the query structure.
        :return: self (for method chaining)
        """
        if "table" in query_json:
            self.table(query_json["table"])

        if "columns" in query_json:
            self.select(*query_json["columns"])

        if "filters" in query_json:
            for filter_item in query_json["filters"]:
                filter_type = filter_item.get("type", "where")
                column = filter_item["column"]
                value = filter_item["value"]

                if filter_type == "where":
                    self.where(column, value)
                elif filter_type == "not":
                    self.notWhere(column, value)
                elif filter_type == "or":
                    self.orWhere(column, value)
                elif filter_type == "like":
                    self.likeWhere(column, value)
                elif filter_type == "in":
                    self.inWhere(column, value)
                elif filter_type == "between":
                    self.betweenWhere(column, value[0], value[1])

        if "order_by" in query_json:
            order_by = query_json["order_by"]
            self.orderBy(order_by["column"], order_by.get("direction", "ASC"))

        if "pagination" in query_json:
            pagination = query_json["pagination"]
            self.paginate(pagination["page"], pagination["page_size"])

        return self
    
    def show_sql(self):
        """
        Display the built SQL query as a string.
        This method is useful for debugging or inspecting the query before execution.

        :return: SQL query string
        """
        try:
            query = self.build_query()
            params = self._params

            # Replace parameter placeholders with actual values for display
            for key, value in params.items():
                placeholder = f":{key}"
                if isinstance(value, str):
                    value = f"'{value}'"  # Add quotes for string values
                query = query.replace(placeholder, str(value))

            return query
        except Exception as e:
            return f"Error building query: {str(e)}"
        
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
            import traceback
            traceback.print_exc()
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

    def extend_query(self, order_by=None, additional_conditions=None):
        """
        Extend the built SQL query with additional clauses.

        :param order_by: A tuple (column, direction) for ordering results.
        :param additional_conditions: Dictionary with column-value pairs for additional WHERE clauses.
        :return: self (for method chaining).
        """
        if additional_conditions:
            for column, value in additional_conditions.items():
                self.where(column, value)

        if order_by:
            self.orderBy(order_by[0], order_by[1])

        return self

    def from_raw_input(self, raw_input):
        """
        Populate the query builder using raw input data.
        
        :param raw_input: Dictionary where keys are column names, and values are lists of search terms.
        :return: self (for method chaining).
        """
        for column, values in raw_input.items():
            if len(values) > 1:
                self.inWhere(column, values)  # Use IN for multiple values
            else:
                self.where(column, values[0])  # Use WHERE for single value
        return self

    def first(self):
        """Fetch the first result."""
        self._limit = 1
        results = self.execute()
        return results[0] if results else None

    def get_column_names(self, table_name):
        """
        Fetch column names from a specified table in the database.
        
        :param table_name: The name of the table for which to fetch column names.
        :return: A list of column names or an error message.
        """
        query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, (table_name,))
                return [row[0] for row in result.fetchall()]
        except SQLAlchemyError as e:
            print(f"Error fetching column names: {str(e)}")
            return {"error": str(e)}
        
    def execute_raw_query(self, query, params=None):
        """
        Execute a SQL query with parameters.

        :param query: SQL query string.
        :param params: Dictionary of query parameters.
        :return: List of results or an error message.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return [dict(row) for row in result]
        except SQLAlchemyError as e:
            import traceback
            traceback.print_exc()
            print(f"Error executing query: {e}")
            return {"error": str(e)}

    def count_query(self, query, params=None):
        """
        Execute a COUNT SQL query with parameters.

        :param query: SQL COUNT query string.
        :param params: Dictionary of query parameters.
        :return: Total count or an error message.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.scalar()
        except SQLAlchemyError as e:
            print(f"Error executing COUNT query: {e}")
            return {"error": str(e)}

    def read_with_conditions(self, query, count_query, params, pagination=None):
        """
        Perform a read operation with dynamic query conditions.

        :param query: SQL query string.
        :param count_query: SQL COUNT query string for pagination.
        :param params: Dictionary of query parameters.
        :param pagination: Dictionary with `page` and `page_size`.
        :return: Dictionary with query results and pagination metadata.
        """
        # Execute the count query for total records
        total_records = self.count_query(count_query, params)
        if isinstance(total_records, dict):  # Error occurred
            return total_records

        # Apply pagination
        if pagination:
            page = pagination.get("page", 1)
            page_size = pagination.get("page_size", 10)
            offset = (page - 1) * page_size
            query += f" LIMIT {page_size} OFFSET {offset}"
        else:
            page, page_size = 1, len(params)

        # Execute the main query
        records = self.execute_query(query, params)
        if isinstance(records, dict):  # Error occurred
            return records

        # Calculate total pages
        total_pages = math.ceil(total_records / page_size)

        # Return results with pagination metadata
        return {
            "success": True,
            "data": records,
            "pagination": {
                "total_records": total_records,
                "total_pages": total_pages,
                "current_page": page,
                "page_size": page_size,
            },
        }
        
    def get_unique_items_from_column(self, table_name, column_name):
        from src.Utils.Helpers import extract_unique_countries
        """
        Retrieves a list of unique items from a specified column using SQLAlchemy engine.

        Parameters:
            table_name (str): The name of the table.
            column_name (str): The name of the column to retrieve unique items from.

        Returns:
            list: A list of unique items from the column.
        """
        try:
            # Construct the SQL query
            query = text(f'SELECT DISTINCT "{column_name}" FROM "{table_name}" ORDER BY "{column_name}"')
            
            # Execute the query
            with  self.engine .connect() as connection:
                result = connection.execute(query)
                # Extract and return unique values
                return extract_unique_countries([row[0] for row in result])
        except Exception as e:
            print(f"Error: {e}")
            return []

    def create_country_choropleth(self, df_country, color_scheme='blues'):
        """
        Generate a choropleth world map using Altair based on country-level record counts.

        Parameters:
        - df_country: DataFrame with columns ['Country', 'record_count']
        - color_scheme: Altair color scheme for the map (default: 'blues')

        Returns:
        - Altair Chart object
        """
        # Clean unknown country labels
        df_country['Country'] = df_country['Country'].replace('[]', 'Unknown')
        df_country.to_csv("testing_country.csv")
        # Convert country names to ISO numeric codes (for matching map IDs)
        def get_iso_code(name):
            try:
                return iso_countries.get(name).numeric
            except:
                return None

        df_country['id'] = df_country['Country'].apply(get_iso_code)
        df_country = df_country.dropna(subset=['id']).copy()
        df_country['id'] = df_country['id'].astype(int)

        # Load TopoJSON world map
        countries = alt.topo_feature(data.world_110m.url, 'countries')

        # Create choropleth map
        world_map = (
            alt.Chart(countries)
            .mark_geoshape()
            .encode(
                color=alt.Color(
                    'record_count:Q',
                    title='Total Records',
                    # scale=alt.Scale(scheme=color_scheme)
                    scale=alt.Scale(
                        domain=[0, df_country['record_count'].max()],
                        range=['#d3d3d3', '#08306b']  # light grey → dark blue
                    )
                ),
                tooltip=[
                    alt.Tooltip('Country:N', title='Country'),
                    alt.Tooltip('record_count:Q', title='Total Records')
                ]
            )
            .transform_lookup(
                lookup='id',
                from_=alt.LookupData(df_country, 'id', ['Country', 'record_count'])
            )
            .transform_calculate(
                record_count="datum.record_count !== null ? datum.record_count : 0"
            )
            .properties(
                width="container",
                height=450,
                title='Records by Country (Map)'
            )
            .project(
                type='naturalEarth1',
                scale=180,
                center=[0, 20],  # Shift map center (longitude, latitude)
                translate=[400, 200],
                # clipExtent=[[0, 0], [800, 350]]  # [x0, y0], [x1, y1] – crops lower part
            )
        )

        return world_map
   
    def generate_chart(self, data):
        df_year_source = pd.DataFrame(data['year_source'])
        df_country = pd.DataFrame(data['country'])
        df_journal = pd.DataFrame(data['journal'])
        df_source = pd.DataFrame(data['source'])
        
        # Colorblind-friendly palette

        colorblind_palette = "tableau20"  # Alternative: "viridis", "set2"
        okabe_ito_colors = ["#a6cee3", "#1f78b4", "#b2df8a", "#33a02c", "#0072B2", "#D55E00", "#CC79A7"]
        # Compute dynamic y-axis max
        grouped_max = df_year_source.groupby(['Year'])['record_count'].sum().max()
        y_max = math.ceil(grouped_max)
        source_color_mapping = {
            "LOVE": "#a6cee3", 
            "OVID": "#1f78b4", 
            "Cochrane": "#b2df8a", 
            "Medline": "#33a02c"
        }
        source_domain = list(source_color_mapping.keys())  # List of categories
        source_range_ = list(source_color_mapping.values())  # Corresponding colors

        # Year & Source bar chart
        chart_year_source = (
            alt.Chart(df_year_source)
            .transform_calculate(
                Year="datum.Year !== null ? datum.Year : 'Unknown'"  # Replaces null with "Unknown"
            )
            .mark_bar()
            .encode(
                x=alt.X('Year:O', title='Year', axis=alt.Axis(labelAngle=-45)),
                y=alt.Y(
                    'sum(record_count):Q', 
                    axis=alt.Axis(title='Total Records', tickMinStep=1, values=list(range(0, y_max + 1))),
                    scale=alt.Scale(domain=[0, y_max])
                ),
                color=alt.Color(
                    'Source:N', 
                    legend=alt.Legend(orient="bottom", direction="horizontal"), 
                    title='Source', 
                    scale=alt.Scale(
                        domain=source_domain, 
                        range=source_range_
                    )
                ), #scheme=colorblind_palette
                tooltip=[
                    alt.Tooltip('Year:O', title='Year'),
                    alt.Tooltip('Source:N', title='Source'),
                    alt.Tooltip('sum(record_count):Q', title='Total Records')
                ]
            )
            .properties(width="container", height=500, title='Cummulative Records Over Time by Source')
            .configure(autosize="fit")
            .interactive()
        )

        # Country bar chart
        df_country['Country'] = df_country['Country'].replace('[]', 'Unknown')
        chart_country = (
            alt.Chart(df_country)
            .mark_bar()
            .encode(
                x=alt.X('Country:N', title='Country', axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('record_count:Q', title='Total Records'),
                color=alt.Color(
                    'Country:N', 
                    legend=alt.Legend(orient="bottom", direction="horizontal"), 
                    scale=alt.Scale(scheme=colorblind_palette)
                ),
                tooltip=[
                    alt.Tooltip('Country:N', title='Country'),
                    alt.Tooltip('record_count:Q', title='Total Records')
                ]
            )
            .properties(width="container", height=300, title='Records by Country')
            .configure(
                autosize="fit"
            ).interactive()
        )

        country_map = self.create_country_choropleth(df_country, color_scheme='blues')
        
        # Journal bar chart
        chart_journal = (
            alt.Chart(df_journal)
            .mark_bar()
            .encode(
                x=alt.X('Journal:N', title='Journal', axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('record_count:Q', title='Total Records'),
                color=alt.Color(
                    'Journal:N', 
                    legend=alt.Legend(orient="bottom", direction="horizontal"), 
                    scale=alt.Scale(
                        domain=source_domain, 
                        range=source_range_
                    )
                ),
                tooltip=[
                    alt.Tooltip('Journal:N', title='Journal'),
                    alt.Tooltip('record_count:Q', title='Total Records')
                ]
            )
            .properties(width="container", height=300, title='Records by Journal')
            .configure(
                autosize="fit"
            )
            .interactive()
        )

        # Source bar chart
        chart_source = (
            alt.Chart(df_source)
            .mark_bar()
            .encode(
                x=alt.X('Source:N', title='Source', axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('record_count:Q', title='Total Records'),
                color=alt.Color(
                    'Source:N', 
                    legend=alt.Legend(orient="bottom", direction="horizontal"), 
                    scale=alt.Scale(domain=df_source['Source'].unique(), range=okabe_ito_colors)
                ),
                tooltip=[
                    alt.Tooltip('Source:N', title='Source'),
                    alt.Tooltip('record_count:Q', title='Total Records')
                ]
            )
            .properties(width="container", height=300, title='Records by Source')
            .configure(
                autosize="fit"
            )
            .interactive()
        )
        
        return {
            "year_source": chart_year_source.to_dict(), 
            "country": chart_country.to_dict(), 
            "country_map": country_map.to_dict(),
            "journal": chart_journal.to_dict(), 
            "source": chart_source.to_dict()
        }
        
    def get_summary_statistics(self):
        """
        Fetches summary statistics grouped individually and by Year for each feature: Country, Journal, and Source.
        Returns a dictionary containing grouped data.
        """
        try:
            # Queries for individual grouping and grouping with Year
            queries = {
                # "year_country": """
                #     SELECT "Year", "Country", COUNT(*) as record_count
                #     FROM all_db
                #     GROUP BY "Year", "Country"
                #     ORDER BY "Year", "Country";
                # """,
                # "year_journal": """
                #     SELECT "Year", "Journal", COUNT(*) as record_count
                #     FROM all_db
                #     GROUP BY "Year", "Journal"
                #     ORDER BY "Year", "Journal";
                # """,
                "year_source": """
                    SELECT "Year", "Source", COUNT(*) as record_count
                    FROM all_db
                    GROUP BY "Year", "Source"
                    ORDER BY "Year", "Source";
                """,
                "country": """
                    SELECT "Country", COUNT(*) as record_count
                    FROM all_db
                    GROUP BY "Country"
                    ORDER BY record_count DESC;
                """,
                "journal": """
                    SELECT "Journal", COUNT(*) as record_count
                    FROM all_db
                    GROUP BY "Journal"
                    ORDER BY record_count DESC
                    LIMIT 3;
                """,
                "source": """
                    SELECT "Source", COUNT(*) AS record_count
                    FROM all_db
                    GROUP BY "Source"
                    ORDER BY record_count DESC
                    LIMIT 4;
                """,
                "ovid_grouping": """
                    SELECT "Database", COUNT(*) AS record_count
                    FROM all_db
                    WHERE "Source"='OVID'
                    GROUP BY "Database"
                    ORDER BY record_count DESC
                    LIMIT 4;
                """
            }
            with self.engine.connect() as connection:
                # Execute each query and transform results into a dictionary
                summary_stats = {}
                for key, query in queries.items():
                    result = connection.execute(query).fetchall()
                    summary_stats[key] = [
                        dict(row) for row in result
                    ]

            # print(summary_stats)
            charts = self.generate_chart(summary_stats)
            return {
                "status": "success",
                "data": summary_stats,
                "charts": charts
            }

        except Exception as e:
            raise Exception(f"Error fetching summary statistics: {e}")