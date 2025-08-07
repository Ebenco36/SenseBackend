import sys
import os
sys.path.append(os.getcwd())
import csv
import argparse
import logging
from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Float, Text, inspect, MetaData, Table, text
from sqlalchemy.orm import sessionmaker, declarative_base
from app import db, app

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
csv.field_size_limit(10**7)
Base = declarative_base()

def parse_csv(file_path):
    """Parse the CSV file and return columns and rows."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            columns = reader.fieldnames
            rows = list(reader)
            if not rows:
                raise ValueError("CSV file is empty or could not be read.")
            return columns, rows
    except Exception as e:
        logging.error(f"Failed to parse CSV file at {file_path}: {e}")
        sys.exit(1)

def infer_column_type(value):
    """Infer SQLAlchemy column type based on a sample value."""
    if value is None or value == '':
        return Text()
    try:
        num = int(value)
        if -2147483648 <= num <= 2147483647:
            return Integer()
        else:
            return BigInteger()
    except (ValueError, TypeError):
        pass
    try:
        float(value)
        return Float()
    except (ValueError, TypeError):
        pass
    return Text()

def ensure_table_exists(engine, table_name, columns, sample_rows):
    """Ensure the table exists; if not, create it with the provided columns."""
    inspector = inspect(engine)
    metadata = MetaData(bind=engine)
    metadata.reflect()

    if not inspector.has_table(table_name):
        logging.info(f"Table '{table_name}' does not exist. It will be created.")
        table_columns = [
            Column('primary_id', Integer, primary_key=True, autoincrement=True)
        ]
        sample_row = sample_rows[0] if sample_rows else {}

        for column in columns:
            sample_value = sample_row.get(column, "")
            if column == 'verification_id':
                column_type = Text()
            else:
                column_type = infer_column_type(sample_value)
            table_columns.append(Column(column, column_type))

        table = Table(table_name, metadata, *table_columns)
        metadata.create_all(tables=[table])
        logging.info(f"Table '{table_name}' created with initial columns.")
    else:
        logging.info(f"Table '{table_name}' already exists.")

def update_table_schema(engine, table_name, columns, sample_rows):
    """Ensure the table has all required columns; add missing columns."""
    inspector = inspect(engine)
    existing_columns = {col['name'] for col in inspector.get_columns(table_name)}
    columns_to_add = set(columns) - existing_columns

    if columns_to_add:
        with engine.connect() as connection:
            with connection.begin():
                for column in columns_to_add:
                    sample_value = sample_rows[0].get(column, "")
                    if column == 'verification_id':
                        sql_type = 'TEXT'
                    else:
                        column_type = infer_column_type(sample_value)
                        sql_type = column_type.compile(engine.dialect)
                    alter_command = text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column}" {sql_type}')
                    connection.execute(alter_command)
                    logging.info(f"Added column '{column}' to '{table_name}'")

def create_dynamic_model(columns, table_name, sample_rows):
    """Dynamically create a SQLAlchemy model based on CSV columns."""
    attrs = {
        '__tablename__': table_name,
        'primary_id': Column(Integer, primary_key=True, autoincrement=True)
    }
    sample_row = sample_rows[0] if sample_rows else {}
    for column in columns:
        sample_value = sample_row.get(column, "")
        if column == 'verification_id':
            column_type = Text()
        else:
            column_type = infer_column_type(sample_value)
        attrs[column] = Column(column_type)

    model = type(table_name, (Base,), attrs)

    with app.app_context():
        engine = db.engine
        # Ensure table exists first
        ensure_table_exists(engine, table_name, columns, sample_rows)
        # Then ALTER for any missing columns
        update_table_schema(engine, table_name, columns, sample_rows)

    return model

def clean_data_for_insertion(row, model):
    """Clean data before insertion to match the model's column types."""
    cleaned_data = {}
    for column, value in row.items():
        if column not in model.__table__.columns:
            continue
        column_type = model.__table__.columns[column].type
        try:
            if value == '':
                cleaned_data[column] = None
            elif isinstance(column_type, (Integer, BigInteger)):
                cleaned_data[column] = int(float(value))
            elif isinstance(column_type, Float):
                cleaned_data[column] = float(value)
            else:
                cleaned_data[column] = value
        except (ValueError, TypeError):
            logging.warning(f"Could not convert value '{value}' for column '{column}'. Inserting NULL.")
            cleaned_data[column] = None
    return cleaned_data

def seed_data(file_path, model):
    """
    Seeds the database, updating records if they exist (checked by verification_id,
    then DOI), or inserting them if they are new.
    """
    error_log, updated_count, inserted_count = [], 0, 0
    with app.app_context():
        Session = sessionmaker(bind=db.engine)
        session = Session()
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for i, row in enumerate(reader, 1):
                try:
                    cleaned_row = clean_data_for_insertion(row, model)

                    existing_record = None
                    verification_id = cleaned_row.get('verification_id')
                    cleaned_doi = cleaned_row.get('cleaned_doi')
                    doi = cleaned_row.get('doi')

                    if verification_id and verification_id != '':
                        existing_record = session.query(model).filter_by(verification_id=verification_id).first()
                    if cleaned_doi and cleaned_doi != '':
                        existing_record = session.query(model).filter_by(cleaned_doi=cleaned_doi).first()
                    elif doi and doi != '':
                        existing_record = session.query(model).filter_by(doi=doi).first()

                    if existing_record:
                        for key, value in cleaned_row.items():
                            setattr(existing_record, key, value)
                        updated_count += 1
                    else:
                        record = model(**cleaned_row)
                        session.add(record)
                        inserted_count += 1

                    if i % 1000 == 0:
                        session.commit()
                        logging.info(f"Processed {i} rows...")

                except Exception as e:
                    session.rollback()
                    error_log.append({'row': i, 'data': row, 'error': str(e)})
                    continue

            session.commit()  # Final commit
        logging.info(f"Data seeding complete! Inserted: {inserted_count}, Updated: {updated_count}")
    
    if error_log:
        logging.warning('The following rows encountered errors:')
        for error in error_log:
            logging.warning(f"Row: {error['row']} - Error: {error['error']}")

def main():
    parser = argparse.ArgumentParser(description="Create/update and seed a table from a CSV file.")
    parser.add_argument('csv_file', type=str, help='Path to the CSV file.')
    parser.add_argument('table_name', type=str, help='Name of the database table.')
    args = parser.parse_args()

    logging.info(f"Starting process for file '{args.csv_file}' into table '{args.table_name}'.")
    columns, rows = parse_csv(args.csv_file)
    model = create_dynamic_model(columns, args.table_name, rows)
    seed_data(args.csv_file, model)
    logging.info("Process finished successfully.")

if __name__ == '__main__':
    main()
