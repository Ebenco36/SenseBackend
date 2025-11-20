import numpy as np
import pandas as pd
class DatabaseHandler:
    def __init__(self, query=None):
        self.query = query

    def fetch_papers(self):
        """
        Fetches papers using the predefined query.
        """
        from app import db, app
        with app.app_context():
            conn = db.engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute(self.query)
            papers = cursor.fetchall()
            cursor.close()
            conn.close()
            return papers
        
    def fetch_papers_with_column_names(self):
        """
        Fetches papers using the predefined query and maps data to column names.
        """
        from app import db, app
        with app.app_context():
            # Establish a raw database connection
            conn = db.engine.raw_connection()
            cursor = conn.cursor()

            try:
                # Execute the query
                cursor.execute(self.query)

                # Fetch all rows
                rows = cursor.fetchall()

                # Get column names from cursor description
                column_names = [desc[0] for desc in cursor.description]

                # Map rows to column names
                papers = [dict(zip(column_names, row)) for row in rows]

                return papers
            finally:
                # Ensure resources are released
                cursor.close()
                conn.close()
    
    
    def update_query(self, new_query):
        """
        Updates the query for this DatabaseHandler instance.
        """
        self.query = new_query
        
    def execute_query(self, query, params=None, use_executemany=False):
        """
        Executes a general-purpose SQL query with optional parameters.

        :param query: SQL query string
        :param params: Optional parameters for the query (list of tuples for executemany or a single tuple for execute)
        :param use_executemany: Whether to use executemany for batch inserts
        :return: Fetched results for SELECT queries, otherwise None
        """
        def convert_params(params):
            """
            Recursively converts numpy types to native Python types.
            """
            if isinstance(params, dict):
                return {k: (int(v) if isinstance(v, np.integer) else v) for k, v in params.items()}
            elif isinstance(params, (list, tuple)):
                return [
                    tuple(None if pd.isna(v) else (int(v) if isinstance(v, np.integer) else v) for v in row)
                    for row in params
                ] if isinstance(params, list) else tuple(None if pd.isna(v) else v for v in params)
            return params

        from app import db, app
        with app.app_context():
            conn = db.engine.raw_connection()
            cursor = conn.cursor()
            try:
                # Convert params before executing query
                sanitized_params = convert_params(params)
                # print(f"Sanitized Params: {sanitized_params}")
                # print(f"Query: {query}")

                if use_executemany:
                    cursor.executemany(query, sanitized_params or [])
                else:
                    cursor.execute(query, sanitized_params or ())

                if query.strip().lower().startswith("select"):
                    results = cursor.fetchall()
                    return results
                else:
                    conn.commit()  # Commit for non-SELECT queries
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"Error executing query: {e}")
                # print(f"Query: {query}")
                # print(f"Params: {params}")
                conn.rollback()  # Rollback in case of an error
            finally:
                cursor.close()
                conn.close()

    def fetch_existing_dois(self):
        """
        Fetches all existing DOIs from the database.
        :return: A list of DOIs.
        """
        query = "SELECT DOI FROM all_db"
        results = self.execute_query(query)
        print(f"Fetched {len(results)} existing DOIs from the database.")
        return [row[0] for row in results]

    def insert_records(self, records):
        """
        Inserts new records into the database.
        :param records: A pandas DataFrame containing the records to insert.
        """
        query = """
        INSERT INTO all_db ("authors", "year", "title", "doi", "open_access", "abstract", "id", "source", "language", "country", "database", "journal")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # Ensure the DataFrame has the correct columns
        required_columns = ["authors", "year", "title", "doi", "open_access", "abstract", "id", "source", "language", "country", "database", "journal"]
        if not all(col in records.columns for col in required_columns):
            raise ValueError(f"Missing required columns in records DataFrame. Expected columns: {required_columns}")

        # Replace NaN values with None
        records = records.where(pd.notnull(records), None)

        # Convert the DataFrame to a list of tuples
        values = records[required_columns].to_records(index=False).tolist()

        # Ensure the number of placeholders matches the number of columns
        if len(values[0]) != query.count("%s"):
            raise ValueError("Number of placeholders in the query does not match the number of columns in the records.")

        # Replace any remaining NaN values in the tuples with None
        sanitized_values = [
            tuple(None if pd.isna(value) else value for value in record)
            for record in values
        ]

        self.execute_query(query, params=sanitized_values, use_executemany=True)
        print(f"Inserted {len(records)} new records into the database.")

    def fetch_new_records(self, dois):
        """
        Fetches records from the database based on a list of DOIs.
        :param dois: A list of DOIs to fetch.
        :return: A list of records as dictionaries.
        """
        query = f"""
        SELECT authors, year, title, doi, open_access, abstract, id, source, language, country, database, journal
        FROM all_db
        WHERE doi IN ({','.join(['%s'] * len(dois))})
        """
        results = self.execute_query(query, params=dois)
        column_names = ["authors", "year", "title", "doi", "open_access", "abstract", "id", "source", "language", "country", "database", "journal"]
        return [dict(zip(column_names, row)) for row in results]
