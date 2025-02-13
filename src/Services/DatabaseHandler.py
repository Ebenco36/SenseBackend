from app import db, app
import numpy as np

class DatabaseHandler:
    def __init__(self, query=None):
        self.query = query

    def fetch_papers(self):
        """
        Fetches papers using the predefined query.
        """
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
        
    def execute_query(self, query, params=None):
        """
        Executes a general-purpose SQL query with optional parameters.

        :param query: SQL query string
        :param params: Optional parameters for the query (dictionary or tuple)
        :return: Fetched results for SELECT queries, otherwise None
        """
        def convert_params(params):
            """
            Recursively converts numpy types to native Python types.
            """
            if isinstance(params, dict):
                return {k: (int(v) if isinstance(v, np.integer) else v) for k, v in params.items()}
            elif isinstance(params, (list, tuple)):
                return tuple(int(v) if isinstance(v, np.integer) else v for v in params)
            return params

        with app.app_context():
            conn = db.engine.raw_connection()
            cursor = conn.cursor()
            try:
                # Convert params before executing query
                sanitized_params = convert_params(params)
                cursor.execute(query, sanitized_params or ())
                if query.strip().lower().startswith("select"):
                    results = cursor.fetchall()
                    return results
                else:
                    conn.commit()  # Commit for non-SELECT queries
            except Exception as e:
                print(f"Error executing query: {e}")
                conn.rollback()  # Rollback in case of an error
            finally:
                cursor.close()
                conn.close()
