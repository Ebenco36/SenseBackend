import re
import pandas as pd
from sqlalchemy import MetaData, Table, String, Integer, Float, Boolean, DateTime, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
import sys
import os
import traceback
from datetime import datetime

sys.path.append(os.getcwd())

class DatabaseUpdater:
    def __init__(self, table_name, column_mapping=None):
        from app import db, app
        with app.app_context():
            self.table_name = table_name
            self.engine = db.engine
            self.metadata = MetaData()
            self._refresh_metadata() # Initial load of schema

        self.Session = sessionmaker(bind=self.engine)
        self.column_mapping = column_mapping or {}

    # ✅ NEW HELPER: A dedicated function to refresh the schema from the database.
    def _refresh_metadata(self):
        """Clears and reloads the table metadata from the database."""
        self.metadata.clear()
        self.metadata.reflect(bind=self.engine, only=[self.table_name])
        self.table = self.metadata.tables[self.table_name]

    @staticmethod
    def infer_column_types(df):
        type_mapping = {
            'object': String, 'int64': Integer, 'float64': Float,
            'bool': Boolean, 'datetime64[ns]': DateTime
        }
        return {column: type_mapping.get(str(dtype), String) for column, dtype in df.dtypes.items()}

    def sanitize_column_name(self, column_name):
        return column_name.replace("#", "__hash__")

    # ✅ CHANGED: This function now refreshes the metadata if it changes the schema.
    def ensure_columns_exist(self, columns):
        """
        Ensures columns exist and refreshes the in-memory schema if new
        columns are added.
        """
        schema_changed = False
        with self.engine.connect() as conn:
            for column_name, column_type in columns.items():
                sanitized_name = self.sanitize_column_name(column_name)
                if sanitized_name not in self.table.columns:
                    try:
                        column_type_sql = self.get_sql_column_type(column_type)
                        alter_query = text(
                            f'ALTER TABLE "{self.table_name}" ADD COLUMN "{sanitized_name}" {column_type_sql}'
                        )
                        conn.execute(alter_query)
                        print(f"Column '{sanitized_name}' added to the table '{self.table_name}'.")
                        schema_changed = True
                    except ProgrammingError as e:
                        # Handle potential race conditions in parallel processing
                        if "already exists" in str(e):
                            print(f"Column '{sanitized_name}' already exists (likely added by another process).")
                            schema_changed = True
                        else:
                            print(f"Error adding column '{sanitized_name}': {e}")
        
        # If we added any columns, refresh our knowledge of the table.
        if schema_changed:
            self._refresh_metadata()

    def get_sql_column_type(self, column_type):
        mapping = {
            String: "VARCHAR", Integer: "INTEGER", Float: "FLOAT",
            Boolean: "BOOLEAN", DateTime: "TIMESTAMP",
        }
        return mapping.get(column_type, "VARCHAR")

    def flatten_dict(self, d, parent_key='', sep='__'):
        # ... (this function is fine, no changes needed) ...
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict): items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list): items.append((new_key, ', '.join(map(str, v))))
            else: items.append((new_key, str(v)))
        return dict(items)

    # ✅ CHANGED: The main update function is now corrected.
    def update_columns_for_existing_records(self, df, id_column):
        """
        Creates columns if they don't exist and then updates values without skipping.
        """
        db_id_column = self.column_mapping.get(id_column, id_column)
        inferred_columns = self.infer_column_types(df)
        self.ensure_columns_exist(inferred_columns)

        with self.Session() as session:
            try:
                for _, row in df.iterrows():
                    record_id = row.get(id_column)
                    if pd.isnull(record_id):
                        print(f"Skipping row with {id_column} = None")
                        continue

                    # FIX: The restrictive check `in self.table.columns` is removed.
                    # The `ensure_columns_exist` call above guarantees they exist.
                    row_data = {
                        self.sanitize_column_name(col): row[col]
                        for col in df.columns
                        if col != id_column
                    }

                    if "updated_at" in self.table.columns:
                        row_data["updated_at"] = datetime.utcnow()

                    flat_row_data = self.flatten_dict(row_data)
                    
                    if not flat_row_data:
                        print(f"Skipping update for record ID {record_id}: No columns to update.")
                    else:
                        session.execute(
                            self.table.update()
                            .where(self.table.c[db_id_column] == str(record_id))
                            .values(**flat_row_data)
                        )
                    print(f"Updated record with {db_id_column} = {record_id}")

                session.commit()
                print("Database records have been updated successfully.")
            except Exception as e:
                session.rollback()
                print(f"Failed to update records: {e}")
                print(traceback.format_exc())

    # The insert function is also corrected.
    def insert_new_records(self, df, id_column):
        """
        Creates columns if they don't exist and then inserts new records.
        """
        db_id_column = self.column_mapping.get(id_column, id_column)
        inferred_columns = self.infer_column_types(df)
        self.ensure_columns_exist(inferred_columns)

        with self.Session() as session:
            try:
                for _, row in df.iterrows():
                    record_id = row.get(id_column)
                    if pd.isnull(record_id):
                        print(f"Skipping row with {id_column} = None")
                        continue

                    # The restrictive check `in self.table.columns` is removed.
                    row_data = {
                        self.sanitize_column_name(col): row[col]
                        for col in df.columns
                    }

                    existing_record = session.query(self.table).filter(self.table.c[db_id_column] == record_id).first()

                    if not existing_record:
                        session.execute(self.table.insert().values(**row_data))
                        print(f"Inserted new record with {db_id_column} = {record_id}")

                session.commit()
                print("New records have been inserted successfully.")
            except Exception as e:
                session.rollback()
                print(f"Failed to insert records: {e}")
                print(traceback.format_exc())

    def close_connection(self):
        self.engine.dispose()