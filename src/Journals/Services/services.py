import json
from collections import defaultdict
from sqlalchemy.exc import SQLAlchemyError
from src.Commands.regexp import searchRegEx
from src.Utils.Helpers import preprocess_languages
from src.Services.PostgresService import PostgresService, QueryHelper
from src.Journals.Services.APIResponseNormalizer import APIResponseNormalizer
class JSONService:
    """Service for interacting with the database using JSON payloads for CRUD operations."""

    def __init__(self):
        self.db_service = PostgresService()
        
    def create(self, payload):
        """
        Create a new record in the specified table.
        Example Payload:
        {
            "table": "my_table",
            "data": {"name": "John Doe", "status": "active"}
        }
        """
        try:
            table_name = payload.get("table")
            data = payload.get("data")
            if not table_name or not data:
                return {"error": "Table name and data are required"}

            success = self.db_service.table(table_name).add_record(data)
            return {"success": success, "message": "Record created successfully"} if success else {"error": "Failed to create record"}
        except Exception as e:
            return {"error": str(e)}

    def search_by_id(self, payload):
        """
        Search for a single record by its ID.
        Example Payload:
        {
            "table": "my_table",
            "id": 123
        }
        """
        try:
            table_name = payload.get("table")
            record_id = payload.get("id")
            
            if not table_name or not record_id:
                return {"error": "Table name and ID are required"}

            # Construct the query with the correct placeholder syntax for psycopg2
            query = f'SELECT * FROM "{table_name}" WHERE "primary_id" = '+ str(record_id) +' LIMIT 1'

            # Execute the query using the db_service
            record = self.db_service.execute_raw_query(query)

            if record and len(record) > 0:
                return {"success": True, "data": record[0]}
            else:
                return {"success": False, "message": "Record not found"}
        except SQLAlchemyError as e:
            return {"error": f"Database error: {str(e)}"}
        except Exception as e:
            return {"error": f"An unexpected error occurred: {str(e)}"}
        
    def read(self, payload):
        """
        Read records from the specified table with filters, pagination, column selection, GROUP BY, and aggregations.
        Example Payload:
        {
            "table": "my_table",
            "columns": ["id", "name"],
            "filters": [{"type": "where", "column": "status", "value": "active"}],
            "group_by": ["column1", "column2"],
            "aggregations": [
                {"func": "COUNT", "column": "*", "alias": "record_count"},
                {"func": "AVG", "column": "price", "alias": "avg_price"}
            ],
            "order_by": {"column": "id", "direction": "ASC"},
            "pagination": {"page": 1, "page_size": 10}
        }
        """
        try:
            table_name = payload.get("table")
            if not table_name:
                return {"error": "Table name is required"}

            service = self.db_service.table(table_name)

            # Handle column selection
            columns = payload.get("columns", [])
            if columns:
                service = service.select(*columns)

            # Handle filters
            filters = payload.get("filters", [])
            for filter_obj in filters:
                filter_type = filter_obj["type"]
                column = filter_obj["column"]
                value = filter_obj["value"]
                if filter_type == "where":
                    service = service.where(column, value)
                elif filter_type == "orWhere":
                    service = service.orWhere(column, value)
                elif filter_type == "likeWhere":
                    service = service.likeWhere(column, value)
                elif filter_type == "inWhere":
                    service = service.inWhere(column, value)
                elif filter_type == "betweenWhere":
                    service = service.betweenWhere(column, value[0], value[1])

            # Handle GROUP BY
            group_by = payload.get("group_by", [])
            if group_by:
                service = service.groupBy(*group_by)

            # Handle aggregations
            aggregations = payload.get("aggregations", [])
            for agg in aggregations:
                func = agg["func"]
                column = agg["column"]
                alias = agg.get("alias")
                service = service.add_aggregation(func, column, alias)

            # Handle order by
            order_by = payload.get("order_by", {})
            if order_by:
                service = service.orderBy(order_by["column"], order_by.get("direction", "ASC"))

            # Handle pagination
            pagination = payload.get("pagination", {})
            if pagination:
                service = service.paginate(pagination.get("page", 1), pagination.get("page_size", 10))

            # Execute query and return results
            return {"success": True, "data": service.get()}
        except Exception as e:
            return {"error": str(e)}

    def read_with_raw_input(
            self, raw_input=None, table="all_db", columns="*", pagination=None, order_by=None, additional_conditions=None, return_sql=False
        ):
            """
            Read records using raw input data with support for additional conditions.

            :param raw_input: Dictionary where keys are column names and values are lists of terms.
            :param table: Table name.
            :param columns: List of columns to select or '*' for all columns.
            :param pagination: Dictionary with `page` and `page_size`.
            :param order_by: Tuple with column name and direction.
            :param additional_conditions: List of dictionaries specifying conditions and types.
            :param return_sql: Boolean indicating if the generated SQL query should be returned.
            :return: Dictionary with results or error message, optionally includes the SQL query.
            """
            try:
                # Default raw_input to an empty dictionary if None
                raw_input = raw_input or {}

                # Validate and build the query
                query, params = QueryHelper.build_query(
                    raw_input, table, order_by=order_by, pagination=pagination, additional_conditions=additional_conditions
                )

                # If return_sql is True, return the query string
                if return_sql:
                    formatted_query = query
                    for key, value in params.items():
                        placeholder = f":{key}"
                        if isinstance(value, str):
                            value = f"'{value}'"  # Add quotes for string values
                        formatted_query = formatted_query.replace(placeholder, str(value))
                    return {"success": True, "sql": formatted_query}

                # Pass the query and params to PostgresService
                self.db_service._table = table

                # Handle column selection
                if columns == "*":
                    self.db_service._columns = "*"
                elif isinstance(columns, list) and columns:
                    valid_columns = [col for col in columns if isinstance(col, str) and col.isidentifier()]
                    if not valid_columns:
                        raise ValueError("Invalid columns provided for selection.")
                    self.db_service.select(*valid_columns)
                else:
                    raise ValueError("Columns must be '*' or a list of valid column names.")

                # Handle where clauses
                if " WHERE " in query:
                    where_clause_part = query.split(" WHERE ", 1)[1].split(" ORDER BY ", 1)[0]
                    self.db_service._where_clauses = where_clause_part.split(" AND ")
                else:
                    self.db_service._where_clauses = []

                self.db_service._params = params

                # Handle pagination
                if pagination:
                    page = pagination.get("page", 1)
                    page_size = pagination.get("page_size", 10)
                    self.db_service._limit = page_size
                    self.db_service._offset = (page - 1) * page_size

                # Execute the query
                records = self.db_service.execute()

                # Handle COUNT query for pagination
                total_records = None
                if pagination:
                    count_query, count_params = QueryHelper.build_count_query(
                        raw_input, table, additional_conditions=additional_conditions
                    )
                    if " WHERE " in count_query:
                        count_where_clause = count_query.split(" WHERE ", 1)[1]
                        self.db_service._where_clauses = count_where_clause.split(" AND ")
                    else:
                        self.db_service._where_clauses = []
                    self.db_service._params = count_params

                    total_records = self.db_service.get_total_records()
                    total_pages = -(-total_records // pagination["page_size"])  # Ceil division

                    return {
                        "success": True,
                        "data": records,
                        "pagination": {
                            "total_records": total_records,
                            "total_pages": total_pages,
                            "current_page": pagination["page"],
                            "page_size": pagination["page_size"]
                        }
                    }

                return {"success": True, "data": records}
            except Exception as e:
                import traceback
                traceback.print_exc()
                return {"error": str(e)}

    def update(self, payload):
        """
        Update an existing record in the specified table.
        Example Payload:
        {
            "table": "my_table",
            "record_id": 1,
            "data": {"status": "inactive"}
        }
        """
        try:
            table_name = payload.get("table")
            record_id = payload.get("record_id")
            data = payload.get("data")
            if not table_name or not record_id or not data:
                return {"error": "Table name, record ID, and data are required"}

            success = self.db_service.table(table_name).update_record(record_id, data)
            return {"success": success, "message": "Record updated successfully"} if success else {"error": "Failed to update record"}
        except Exception as e:
            return {"error": str(e)}

    def delete(self, payload):
        """
        Delete a record from the specified table.
        Example Payload:
        {
            "table": "my_table",
            "record_id": 1
        }
        """
        try:
            table_name = payload.get("table")
            record_id = payload.get("record_id")
            if not table_name or not record_id:
                return {"error": "Table name and record ID are required"}

            success = self.db_service.table(table_name).delete_record(record_id)
            return {"success": success, "message": "Record deleted successfully"} if success else {"error": "Failed to delete record"}
        except Exception as e:
            return {"error": str(e)}

    def show_sql(self, payload):
        """
        Build and return the SQL query string without executing it.
        Example Payload:
        {
            "table": "my_table",
            "columns": ["id", "name"],
            "filters": [{"type": "where", "column": "status", "value": "active"}],
            "order_by": {"column": "id", "direction": "ASC"},
            "pagination": {"page": 1, "page_size": 10}
        }
        """
        try:
            table_name = payload.get("table")
            if not table_name:
                return {"error": "Table name is required"}

            service = self.db_service.from_json(payload)

            # Generate and return the SQL query as a string
            sql_query = service.build_query()
            params = service._params  # Access the query parameters

            # Replace placeholders with actual values
            for key, value in params.items():
                placeholder = f":{key}"
                if isinstance(value, str):
                    value = f"'{value}'"  # Add quotes for strings
                sql_query = sql_query.replace(placeholder, str(value))

            return {"success": True, "query": sql_query}
        except Exception as e:
            return {"error": str(e)}
        
    def get_columns_from_table(self, payload):
        """
        Fetch and return the columns for a specified table.
        
        Example Payload:
        {
            "table": "my_table"
        }
        """
        table_name = payload.get("table")
        if not table_name:
            return {"error": "Table name is required"}
        
        columns = self.db_service.get_column_names(table_name)
        if "error" in columns:
            return columns 
        
        return columns
    
    def process_columns_with_hash(self, filtered_columns, searchRegEx):
        """
        Processes a list of columns containing '__HASH__' into a structured dictionary,
        integrating synonyms and additional context from searchRegEx.

        :param filtered_columns: List of column names containing '__HASH__'.
        :param searchRegEx: Dictionary with predefined synonyms for certain categories, subgroups, and values.
        :return: Structured dictionary of categories, subgroups, and values.
        """
        # Initialize a nested dictionary to structure the data
        structured_data = defaultdict(lambda: defaultdict(dict))

        # Process the list of columns
        for column in filtered_columns:
            # Split the column using "__HASH__"
            parts = column.split("__HASH__")
            if len(parts) == 3:
                category, subgroup, value = parts

                # Check for corrections in the regex data
                corrected_value = value
                if category in searchRegEx and subgroup in searchRegEx[category]:
                    for key in searchRegEx[category][subgroup]:
                        # Correct the value if a close match exists in searchRegEx
                        if key.lower().startswith(value.lower()):
                            corrected_value = key
                            break

                # Fetch synonyms from searchRegEx if available
                synonyms = []
                if category in searchRegEx and subgroup in searchRegEx[category]:
                    tuple_vals = searchRegEx[category][subgroup].get(corrected_value, [])
                    synonyms = [f"{item[0]}:{item[1]}" for item in tuple_vals if isinstance(item, tuple) and len(item) == 2]
                    

                # Ensure display value is included in synonyms
                if corrected_value not in synonyms:
                    synonyms.append(corrected_value)

                # Initialize with display, synonyms, and additional details
                structured_data[category][subgroup][corrected_value] = {
                    "display": corrected_value,  # Corrected display name
                    "synonyms": synonyms,  # Include display value if not already in synonyms
                    "additional_context": None  # Placeholder for additional metadata
                }
        
        data = {key: dict(value) for key, value in structured_data.items()}
        # Normalize the response
        normalizer = APIResponseNormalizer()
        normalized_response = normalizer.normalize_response(data)
        # print(normalized_response)
        # Convert to a standard dictionary for output
        return {"success": True, "data": normalized_response}
    
    def map_user_selection_to_column(self, user_selections):
        """
        Maps a user's selection to the corresponding column name for database search.

        :param user_selection: The term or keyword selected by the user (e.g., "efficacy").
        :param structured_data: The structured dictionary containing mappings of display values and synonyms.
        :return: The original column name from filtered_columns or None if no match is found.
        """
        filtered_columns = self.get_columns_from_table({"table": "all_db"})
        structured_data = self.process_columns_with_hash(filtered_columns, searchRegEx).get("data", {})
        
        result = {}

        for user_selection in user_selections:
            user_selection_lower = user_selection.lower()
            matched = False

            # Iterate through the structured data to find the matching value
            for category, subgroups in structured_data.items():
                for subgroup, values in subgroups.items():
                    for value, details in values.items():
                        # Check if the user input matches the display or any synonym
                        if user_selection_lower == details["display"].lower() or user_selection_lower in [syn.lower() for syn in details["synonyms"]]:
                            # Construct the column name from category, subgroup, and value
                            constructed_column = f"{category}__HASH__{subgroup}__HASH__{value}"

                            # Truncate the constructed column name to 63 characters if needed
                            if len(constructed_column) > 63:
                                constructed_column = constructed_column[:63]

                            # Validate that the constructed column exists in the filtered_columns
                            if constructed_column in filtered_columns:
                                # Add the user_selection to the corresponding constructed_column key in the result dictionary
                                if constructed_column not in result:
                                    result[constructed_column] = []
                                result[constructed_column].append(user_selection)
                                matched = True
                                break

                    if matched:
                        break
                if matched:
                    break

        return {"success": True, "data": result}
    
    def get_summary_statistics(self):
        return self.db_service.get_summary_statistics()
    
    def get_other_filters(self):
        table_name = "region_country"
        unique_region_items = self.db_service.get_unique_items_from_column(table_name, "region")
        unique_country_items = self.db_service.get_unique_items_from_column(table_name, "country")
        unique_languages_items = preprocess_languages(self.db_service.get_unique_items_from_column("all_db", "Language"))
        unique_year_items = self.db_service.get_unique_items_from_column("all_db", "Year")
        return unique_region_items, unique_country_items, unique_languages_items, unique_year_items