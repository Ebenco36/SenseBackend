import numpy as np

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
        from app import db, app
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

    def fetch_existing_dois(self):
        """
        Fetches all existing DOIs from the database.
        :return: A list of DOIs.
        """
        query = "SELECT DOI FROM all_db"
        results = self.execute_query(query)
        return [row[0] for row in results]

    def insert_records(self, records):
        """
        Inserts new records into the database.
        :param records: A pandas DataFrame containing the records to insert.
        """
        query = """
        INSERT INTO all_db (Authors, Year, Title, DOI, Open_Access, Abstract, Id, Source, Language, Country, Database, Journal)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = records.to_records(index=False).tolist()
        self.execute_query(query, params=values)

    def fetch_new_records(self, dois):
        """
        Fetches records from the database based on a list of DOIs.
        :param dois: A list of DOIs to fetch.
        :return: A list of records as dictionaries.
        """
        query = f"""
        SELECT Authors, Year, Title, DOI, Open_Access, Abstract, Id, Source, Language, Country, Database, Journal
        FROM all_db
        WHERE DOI IN ({','.join(['%s'] * len(dois))})
        """
        results = self.execute_query(query, params=dois)
        column_names = ["Authors", "Year", "Title", "DOI", "Open_Access", "Abstract", "Id", "Source", "Language", "Country", "Database", "Journal"]
        return [dict(zip(column_names, row)) for row in results]