import os
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

def create_model_class(table_name, df):
    """
    Create a dynamic SQLAlchemy model class based on CSV headers and inferred column types.
    :param table_name: Name of the table.
    :param df: Pandas DataFrame with the CSV data.
    :return: Dynamic SQLAlchemy model class.
    """
    class MembraneProteins(Base):
        __tablename__ = table_name
        id = Column(Integer, primary_key=True, autoincrement=True)

    for column_name, dtype in zip(df.columns, df.dtypes):
        column_type = get_sqlalchemy_column_type(dtype)
        setattr(MembraneProteins, column_name, Column(column_type))

    return MembraneProteins

def get_sqlalchemy_column_type(dtype):
    """
    Map Pandas data types to SQLAlchemy data types.
    :param dtype: Pandas data type.
    :return: Corresponding SQLAlchemy data type.
    """
    if np.issubdtype(dtype, np.integer):
        return Integer
    elif np.issubdtype(dtype, np.floating):
        return String  # Change this if you prefer Float or other numeric types
    elif np.issubdtype(dtype, np.datetime64):
        return Date
    else:
        return String  # Default to String for other types

def read_csv_and_create_model(csv_path, table_name):
    """
    Read CSV file, create a dynamic model based on headers and inferred column types,
    and insert data into the database.
    :param csv_path: Path to the CSV file.
    :param table_name: Name of the table.
    """
    df = pd.read_csv(csv_path)
    
    engine = create_engine(os.getenv("DATABASE_URL"))
    Base.metadata.create_all(engine)

    model_class = create_model_class(table_name, df)

    Session = sessionmaker(bind=engine)
    session = Session()

    # Insert data into the dynamically created table
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    session.commit()
    session.close()

if __name__ == '__main__':
    # Get the project directory path
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset = project_dir+"/results/ProcessedData.csv"
    
    if os.path.exists(dataset):
        # Proceed with further actions, for example, reading the file or using its path
        print(f"The file '{dataset}' exists.")
        table_name = 'membrane_proteins'
        # Read CSV and create model with inferred column types
        read_csv_and_create_model(dataset, table_name)
    else:
        print(f"The file '{dataset}' does not exist. Please check the file path.")
