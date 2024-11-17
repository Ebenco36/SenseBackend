import re
import pandas as pd
from sqlalchemy import MetaData, Table, String, Integer, Float, Boolean, DateTime, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from app import db, app

class DatabaseUpdater:
    def __init__(self, table_name, column_mapping=None):
        """
        Initializes the DatabaseUpdater with the target table name and optional column mapping.

        :param table_name: Name of the table to update
        :param column_mapping: Optional dictionary to map DataFrame columns to database table columns, e.g., {'Id': 'primary_id'}
        """
        with app.app_context():
            self.table_name = table_name
            self.engine = db.engine
            self.metadata = MetaData(bind=self.engine)
            self.metadata.reflect()

        if table_name in self.metadata.tables:
            self.table = self.metadata.tables[table_name]
        else:
            raise Exception(f"Table '{table_name}' does not exist in the database.")

        self.Session = sessionmaker(bind=self.engine)
        self.column_mapping = column_mapping or {}

    @staticmethod
    def infer_column_types(df):
        type_mapping = {
            'object': String,
            'int64': Integer,
            'float64': Float,
            'bool': Boolean,
            'datetime64[ns]': DateTime
        }
        inferred_columns = {}
        for column, dtype in df.dtypes.items():
            inferred_columns[column] = type_mapping.get(str(dtype), String)
        return inferred_columns

    def sanitize_column_name(self, column_name):
        return column_name.replace("#", "__HASH__")

    def ensure_columns_exist(self, columns):
        for column_name, column_type in columns.items():
            sanitized_name = self.sanitize_column_name(column_name)
            if sanitized_name not in self.table.columns:
                try:
                    column_type_sql = self.get_sql_column_type(column_type)
                    alter_table_query = text(
                        f'ALTER TABLE "{self.table_name}" ADD COLUMN "{sanitized_name}" {column_type_sql}'
                    )
                    with self.engine.connect() as conn:
                        conn.execute(alter_table_query)
                    print(f"Column '{sanitized_name}' added to the table '{self.table_name}'.")
                except ProgrammingError as e:
                    print(f"Error adding column '{sanitized_name}': {e}")
            else:
                print(f"Column '{sanitized_name}' already exists in the table '{self.table_name}'.")

    def get_sql_column_type(self, column_type):
        if column_type == String:
            return "VARCHAR"
        elif column_type == Integer:
            return "INTEGER"
        elif column_type == Float:
            return "FLOAT"
        elif column_type == Boolean:
            return "BOOLEAN"
        elif column_type == DateTime:
            return "TIMESTAMP"
        else:
            return "VARCHAR"

    def update_columns_for_existing_records(self, df, id_column):
        # Map DataFrame identifier column to database identifier column if provided
        db_id_column = self.column_mapping.get(id_column, id_column)
        
        inferred_columns = self.infer_column_types(df)
        self.ensure_columns_exist(inferred_columns)

        with self.Session() as session:
            for _, row in df.iterrows():
                record_id = row.get(id_column)
                
                if pd.isnull(record_id):
                    print(f"Skipping row with {id_column} = None")
                    continue

                # Filter row data to exclude the ID column and include only existing columns in the table
                row_data = {
                    self.sanitize_column_name(col): row[col]
                    for col in inferred_columns.keys()
                    if col != id_column and self.sanitize_column_name(col) in self.table.columns
                }

                # Check if the record exists by the mapped identifier column
                existing_record = session.query(self.table).filter_by(**{db_id_column: record_id}).first()

                if existing_record:
                    session.execute(
                        self.table.update()
                        .where(self.table.c[db_id_column] == record_id)
                        .values(**row_data)
                    )
                    print(f"Updated record with {db_id_column} = {record_id}")
                else:
                    print(f"Record with {db_id_column} = {record_id} not found, skipping update.")

            session.commit()
            print("Database records have been updated successfully.")

    def close_connection(self):
        self.engine.dispose()