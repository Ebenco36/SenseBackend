import psycopg
import pandas as pd

class PostgreSQLUpdater:
    def __init__(self, db_params):
        """
        Initialize the database connection.

        Args:
            db_params (dict): Database connection parameters including 'dbname', 'user', 'password', 'host', and 'port'.
        """
        self.db_params = db_params
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish connection to the PostgreSQL database."""
        try:
            self.connection = psycopg.connect(**self.db_params)
            self.cursor = self.connection.cursor()
            print("Connected to the database.")
        except Exception as e:
            raise Exception(f"Error connecting to database: {e}")
    
    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Database connection closed.")
    
    def get_table_columns(self, table_name):
        """
        Retrieve the column names of the specified table from the database.

        Args:
            table_name (str): The name of the table to query.

        Returns:
            list: List of column names in the table.
        """
        if self.connection is None:
            raise Exception("No database connection. Call `connect` first.")
        
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """
        self.cursor.execute(query, (table_name,))
        columns = [row[0] for row in self.cursor.fetchall()]
        return columns

    def update_table(self, dataframe, id_column, table_name):
        """
        Update the PostgreSQL table using a DataFrame.

        Args:
            dataframe (pd.DataFrame): The pandas DataFrame with data to update.
            id_column (str): The column in the DataFrame and database used for matching rows (usually the primary key).
            table_name (str): The name of the table to update.
        """
        if self.connection is None:
            raise Exception("No database connection. Call `connect` first.")
        
        # Get the table columns
        table_columns = self.get_table_columns(table_name)

        # Filter DataFrame columns to match table columns
        update_columns = [col for col in dataframe.columns if col in table_columns and col != id_column]

        if not update_columns:
            print(f"No matching columns found in the database for table '{table_name}'.")
            return

        # Generate dynamic SQL for updating the filtered columns
        set_clause = ", ".join([f'"{col}" = %s' for col in update_columns])
        quoted_id_column = f'"{id_column}"'
        update_query = f"""
        UPDATE "{table_name}"
        SET {set_clause}
        WHERE {quoted_id_column} = %s;
        """

        # Iterate over DataFrame rows and execute the update query
        for _, row in dataframe.iterrows():
            values = [row[col] for col in update_columns] + [row[id_column]]
            try:
                self.cursor.execute(update_query, values)
            except Exception as e:
                print(f"Error updating row with {id_column}={row[id_column]}: {e}")
        
        # Commit the changes
        self.connection.commit()
        print(f"{self.cursor.rowcount} rows updated.")

# Example usage
# if __name__ == "__main__":
#     # Sample DataFrame
#     data = {
#         'Id': [1, 2],
#         'Column1': ['new_value1', 'new_value2'],  # Assume Column1 exists in the table
#         'Column2': [10, 20],                     # Assume Column2 exists in the table
#         'NonExistentColumn': ['val1', 'val2']    # This column will be ignored
#     }
#     df = pd.DataFrame(data)

#     # Database connection parameters
#     db_params = {
#         'dbname': 'your_database',
#         'user': 'your_username',
#         'password': 'your_password',
#         'host': 'localhost',
#         'port': 5432
#     }

#     # Instantiate the updater class
#     updater = PostgreSQLUpdater(db_params)

#     try:
#         updater.connect()
#         updater.update_table(df, id_column='id', table_name='your_table')
#     except Exception as e:
#         print(f"An error occurred: {e}")
#     finally:
#         updater.close()
