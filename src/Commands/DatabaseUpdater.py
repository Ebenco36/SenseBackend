import re
import pandas as pd
from sqlalchemy import MetaData, Table, String, Integer, Float, Boolean, DateTime, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from app import db, app
import sys
import os
sys.path.append(os.getcwd())

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
        return {column: type_mapping.get(str(dtype), String) for column, dtype in df.dtypes.items()}

    def sanitize_column_name(self, column_name):
        """
        Sanitize column names to be compatible with SQL standards.
        """
        return column_name.replace("#", "__HASH__")

    def ensure_columns_exist(self, columns):
        """
        Ensure the required columns exist in the database table.
        """
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
            # else:
                # print(f"Column '{sanitized_name}' already exists in the table '{self.table_name}'.")

    def get_sql_column_type(self, column_type):
        """
        Maps Python types to SQL column types.
        """
        mapping = {
            String: "VARCHAR",
            Integer: "INTEGER",
            Float: "FLOAT",
            Boolean: "BOOLEAN",
            DateTime: "TIMESTAMP",
        }
        return mapping.get(column_type, "VARCHAR")

    def update_columns_for_existing_records(self, df, id_column):
        """
        Updates columns for existing records in the database table.
        """
        db_id_column = self.column_mapping.get(id_column, id_column)
        inferred_columns = self.infer_column_types(df)
        self.ensure_columns_exist(inferred_columns)

        with self.Session() as session:
            for _, row in df.iterrows():
                record_id = row.get(id_column)
                if pd.isnull(record_id):
                    print(f"Skipping row with {id_column} = None")
                    continue

                row_data = {
                    self.sanitize_column_name(col): row[col]
                    for col in df.columns
                    if col != id_column and self.sanitize_column_name(col) in self.table.columns
                }

                try:
                    session.execute(
                        self.table.update()
                        .where(self.table.c[db_id_column] == record_id)
                        .values(**row_data)
                    )
                    print(f"Updated record with {db_id_column} = {record_id}")
                except Exception as e:
                    print(f"Failed to update record {record_id}: {e}")

            session.commit()
            print("Database records have been updated successfully.")

    def insert_new_records(self, df, id_column):
        """
        Inserts new records into the database table if they don't exist.
        """
        db_id_column = self.column_mapping.get(id_column, id_column)
        inferred_columns = self.infer_column_types(df)
        self.ensure_columns_exist(inferred_columns)

        with self.Session() as session:
            for _, row in df.iterrows():
                record_id = row.get(id_column)
                if pd.isnull(record_id):
                    print(f"Skipping row with {id_column} = None")
                    continue

                row_data = {
                    self.sanitize_column_name(col): row[col]
                    for col in df.columns
                    if self.sanitize_column_name(col) in self.table.columns
                }

                # Check if record exists
                existing_record = session.query(self.table).filter_by(**{db_id_column: record_id}).first()

                if not existing_record:
                    try:
                        session.execute(self.table.insert().values(**row_data))
                        print(f"Inserted new record with {db_id_column} = {record_id}")
                    except Exception as e:
                        print(f"Failed to insert record {record_id}: {e}")

            session.commit()
            print("New records have been inserted successfully.")

    def close_connection(self):
        """
        Disposes the database connection.
        """
        self.engine.dispose()