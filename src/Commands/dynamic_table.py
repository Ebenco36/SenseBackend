import sys
import os
sys.path.append(os.getcwd())
import csv
import argparse
from sqlalchemy import create_engine, Column, Integer, String, Float, Text, inspect, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app import db, app

Base = declarative_base()

def parse_csv(file_path):
    """Parse the CSV file and return columns and rows."""
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        columns = reader.fieldnames
        rows = list(reader)
        return columns, rows

def infer_column_type(value, max_length=255):
    """Infer SQLAlchemy column type based on the value and handle length."""
    if value.isdigit():
        return Integer
    try:
        float(value)
        return Float
    except ValueError:
        pass
    if len(value) > max_length:
        return Text()
    return Text()

def update_table_schema(engine, table_name, columns):
    """Update the table schema by adding new columns if necessary or create the table if it does not exist."""
    metadata = MetaData(bind=engine)
    inspector = inspect(engine)
    
    if table_name in inspector.get_table_names():
        print(f"Table '{table_name}' exists. Updating schema...")
        existing_columns = set(col['name'] for col in inspector.get_columns(table_name))
        new_columns = set(columns)
        
        columns_to_add = new_columns - existing_columns
        if columns_to_add:
            with engine.connect() as conn:
                for column in columns_to_add:
                    column_type = infer_column_type("")
                    alter_command = f"ALTER TABLE {table_name} ADD COLUMN {column} {column_type.__visit_name__}"
                    conn.execute(alter_command)
                    print(f"Added column {column} to {table_name}")
    else:
        print(f"Table '{table_name}' does not exist. Creating table...")
        Base.metadata.create_all(engine)

def create_dynamic_model(columns, table_name, sample_rows):
    """Dynamically create or update a SQLAlchemy model based on CSV columns."""
    attrs = {'__tablename__': table_name}
    attrs['primary_id'] = Column(Integer, primary_key=True, autoincrement=True)

    max_length = 255
    for column in columns:
        sample_value = sample_rows[0].get(column, "")
        column_type = infer_column_type(sample_value, max_length)
        attrs[column] = Column(column_type)

    additional_columns = {
        "intervention_vaccinePredictableDisease_tags": Text,
        "population_OtherSpecificGroup_tags": Text,
        "population_specificGroup_acronyms_": Text,
        "topic_efficacyEffectiveness_tags": Text,
        "intervention_vaccineOptions_tags": Text,
        "population_specificGroup_tags": Text,
        "population_immuneStatus_tags": Text,
        "outcome_hospitalization_tags": Text,
        "topic_immunogenicity_tags": Text,
        "topic_administration_tags": Text,
        "topic_ethicalIssues_tags": Text,
        "population_ageGroup_tags": Text,
        "outcome_infection_tags": Text,
        "number_of_studies_tags": Text,
        "topic_acceptance_tags": Text,
        "topic_coverage_tags": Text,
        "topic_economic_tags": Text,
        "outcome_death_tags": Text,
        "topic_safety_tags": Text,
        "outcome_ICU_tags": Text
    }
    attrs.update(additional_columns)

    model = type('DynamicModel', (Base,), attrs)

    with app.app_context():
        engine = db.engine
        update_table_schema(engine, table_name, columns)

    return model

def clean_data_for_insertion(row, model):
    """Clean data before insertion to handle type mismatches."""
    cleaned_data = {}
    for column, value in row.items():
        if column not in model.__table__.columns:
            continue
        column_type = model.__table__.columns[column].type
        if isinstance(column_type, Integer):
            cleaned_data[column] = int(value) if value.isdigit() else None
        elif isinstance(column_type, Float):
            try:
                cleaned_data[column] = float(value)
            except ValueError:
                print("there is a problem with " + column)
                cleaned_data[column] = None
        else:
            cleaned_data[column] = value
    return cleaned_data

def check_existing_record(session, model, row, primary_columns, fallback_columns):
    """Check if a record already exists based on primary and fallback columns."""
    filters = [getattr(model, col) == row.get(col) for col in primary_columns if col in row and row.get(col)]
    fallback_filters = [getattr(model, col) == row.get(col) for col in fallback_columns if col in row and row.get(col)]
    if filters:
        existing_record = session.query(model).filter(*filters).first()
        if existing_record:
            return existing_record
    
    if fallback_filters:
        existing_record = session.query(model).filter(*fallback_filters).first()
        if existing_record:
            return existing_record
    
    return None

def seed_data(file_path, model, primary_columns, fallback_columns):
    """Seed the database with data from the CSV file and log errors."""
    error_log = []
    with app.app_context():
        Session = sessionmaker(bind=db.engine)
        session = Session()
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    cleaned_row = clean_data_for_insertion(row, model)
                    existing_record = check_existing_record(session, model, cleaned_row, primary_columns, fallback_columns)
                    
                    if existing_record:
                        for key, value in cleaned_row.items():
                            setattr(existing_record, key, value)
                    else:
                        record = model(**cleaned_row)
                        session.add(record)
                    
                except Exception as e:
                    error_log.append({'row': row, 'error': str(e)})
                    continue
            session.commit()
        print('Data seeded successfully!')
    
    if error_log:
        print('The following rows were not inserted:')
        for error in error_log:
            print(f"Row: {error['row']} - Error: {error['error']}")
        with open('error_log.csv', 'w', newline='') as csvfile:
            fieldnames = ['row', 'error']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for error in error_log:
                writer.writerow({'row': error['row'], 'error': error['error']})

def main():
    """Main function to create and seed a dynamic table from a CSV file."""
    parser = argparse.ArgumentParser(description="Create and seed a dynamic table from a CSV file.")
    parser.add_argument('csv_file', type=str, help='Path to the CSV file.')
    parser.add_argument('table_name', type=str, help='Name of the table to be created.')
    parser.add_argument('--primary', nargs='*', default=[], help='Primary columns to use for validation (to avoid duplicates).')
    parser.add_argument('--fallback', nargs='*', default=[], help='Fallback columns to use if primary columns are empty or missing.')
    args = parser.parse_args()

    file_path = args.csv_file
    table_name = args.table_name
    primary_columns = args.primary
    fallback_columns = args.fallback

    columns, rows = parse_csv(file_path)
    model = create_dynamic_model(columns, table_name, rows)
    seed_data(file_path, model, primary_columns, fallback_columns)

if __name__ == '__main__':
    main()