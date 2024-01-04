import os
import re
import click
import pandas as pd
import importlib
from datetime import datetime
from database.db import db

def create_instance(module_name, class_name):
    try:
        # Import the module dynamically
        module = importlib.import_module(module_name)

        # Get the class from the module
        if hasattr(module, class_name):
            class_ = getattr(module, class_name)
            # Create an instance of the class with optional arguments
            instance = class_
            return instance
        else:
            raise AttributeError(f"Class '{class_name}' not found in module '{module_name}'.")
    except ImportError:
        raise ImportError(f"Module '{module_name}' not found.")


def get_column_type(dtype):
    # Map pandas data types to SQLAlchemy data types
    type_mapping = {
        'int64': db.Integer,
        'float64': db.Float,
        'datetime64[ns]': db.DateTime,
        'object': db.String
    }
    return type_mapping.get(str(dtype), db.String)

def shorten_column_name(column_name):
    # Split column name by '_' and select the first character of the first 4 words
    words = column_name.split('_')
    shortened_name = ''.join([word[:3] for word in words[:2]]) + '_'.join(words[2:]) if len(words) > 4 else column_name
    # Convert spaces to underscores
    cleaned_string = shortened_name.replace(' ', '_')
    
    # Remove special characters using regex
    cleaned_string = re.sub(r'[^a-zA-Z0-9_]', '', cleaned_string)
    return cleaned_string.lower()

def generate_model_class(csv_path, output_file='model.py'):
    # Load CSV data to inspect headers and datatypes
    df = pd.read_csv(csv_path, low_memory=False)
    df = processColumns(df)
    
    
    module_name = "src.MP.model"
    class_name = "MembraneProteinData"
    instance = create_instance(module_name, class_name)
    if instance:
        return instance
    

    class MembraneProteinData(db.Model):
        __table_args__ = {'extend_existing': True}
        __tablename__ = 'membrane_proteins'
        id = db.Column(db.Integer, primary_key=True)
        created = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
        updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Add columns dynamically based on CSV headers and datatypes
    for column_name, dtype in zip(df.columns, df.dtypes):
        column_type = get_column_type(dtype)
        setattr(MembraneProteinData, shorten_column_name(column_name), db.Column(column_type))

    # Create the output file with the generated model class
    with open(output_file, 'w') as file:
        file.write("from datetime import datetime\n")
        file.write("from database.db import db\n\n")
        file.write(f"class {MembraneProteinData.__name__}(db.Model):\n")
        file.write("    __tablename__ = 'membrane_proteins'\n")
        file.write("    id = db.Column(db.Integer, primary_key=True)\n")

        # Add columns to the file
        for column_name, dtype in zip(df.columns, df.dtypes):
            column_type = get_column_type(dtype)
            shortened_name = shorten_column_name(column_name)
            file.write(f"    {shortened_name} = db.Column(db.{column_type.__name__})\n")

    print(f"Model class has been generated and saved to {output_file}")
    return MembraneProteinData

def processColumns(df):
    # Remove columns that start with 'Unnamed: 0.1'
    columns_to_exclude = [col for col in df.columns if col.startswith('Unnamed')]
    df = df.drop(columns=columns_to_exclude)
    # Identify columns to be removed
    columns_to_remove = [col for col in df.columns if '_id' in col.lower()]
    
    # Remove identified columns
    df = df.drop(columns=columns_to_remove)
    
    return df
    

def load_csv_data(model_class, csv_path):
    # Load CSV data into a pandas DataFrame
    df = pd.read_csv(csv_path, low_memory=False)
    
    df = processColumns(df)
    # Loop through columns and update names.
    # We are doing this because of the issue with the column length
    for old_name in df.columns:
        # Create a new name (you can modify this logic based on your requirements)
        new_name = shorten_column_name(old_name)

        # Rename the column
        df = df.rename(columns={old_name: new_name})

    # Insert records into the database
    for index, row in df.iterrows():
        record = model_class(**row.to_dict())
        db.session.add(record)

    # Commit the changes
    db.session.commit()

