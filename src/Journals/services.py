from flask import abort, send_file
import pandas as pd
from sqlalchemy import func
from database.db import db
from sqlalchemy import select, func, desc, or_, Table, Column, String, inspect
from sqlalchemy import text
from sqlalchemy.sql import select
from sqlalchemy.orm import Query
from sqlalchemy import or_
from sqlalchemy.ext.declarative import declarative_base
from collections import OrderedDict
from sqlalchemy import MetaData

Base = declarative_base()

def get_all_items():
    return get_table_as_dataframe("common_columns")

def getJournals():
    Journal = db.Table("common_columns", db.metadata, autoload_with=db.engine)
    return Journal


def apply_search_and_filter(query, search_term):
    Journal = getJournals()
    if search_term:
        filters = []
        for column, value in search_term.items():
            if value:
                # Determine the prefix based on the key name
                prefix = get_prefix(column)
                
                # Split the value by comma and create ilike condition for each split value
                split_values = [v.strip() for v in value.split(',')]
                value_filters = [func.lower(getattr(Journal.c, f"{prefix}{column}")).ilike(f"%{v.lower()}%") for v in split_values]
                filters.append(or_(*value_filters))
        
        # Combine all filters with OR condition
        if filters:
            query = query.filter(or_(*filters))
    
    return query

def get_prefix(column):
    prefixes = {
        "name": "",
        "Outcome#": "Outcome#",
        "Population#": "Population#",
        "Topic#": "Topic#"
    }
    for key, value in prefixes.items():
        if column.startswith(key):
            return value
    return ""

def get_items(request):
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    search_term = request.args.get('search_term', default='', type=object)

    items = get_all_items()
    
    # Apply search and filter
    items = apply_search_and_filter(items, search_term)

    # Perform pagination using LIMIT and OFFSET
    paginated_items = items.paginate(page=page, per_page=per_page, error_out=False)
    return paginated_items


def get_table_as_dataframe(table_name):
    # Reflect the table using SQLAlchemy
    table = db.Table(table_name, db.metadata, autoload_with=db.engine)
    
    # Create a SQLAlchemy SELECT query
    query = db.select([table])

    # Execute the query using the db session and fetch all results
    result = db.session.execute(query)
    records = result.fetchall()

    # Create a list of dictionaries from the query result
    records_dict = [dict(record) for record in records]

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(records_dict)

    return df
    


class PaginatedQuery(Query):
    def paginate(self, page, per_page=10, error_out=True):
        if error_out and page < 1:
            abort(404)

        items = self.limit(per_page).offset((page - 1) * per_page).all()
        total = self.order_by(None).count()

        return {'items': items, 'total': total, 'page': page, 'per_page': per_page}

# Apply the PaginatedQuery to the SQLAlchemy session
db.session.query_class = PaginatedQuery

def get_table_as_dataframe_exception(table_name, filter_column=None, filter_value=None, page=1, per_page=10):
    page = int(page)
    per_page = int(per_page)
    # Reflect the table using SQLAlchemy
    table = db.Table(table_name, db.metadata, autoload_with=db.engine)

    # Create a SQLAlchemy SELECT query
    query = select([table])

    # Add a filter condition if provided
    if filter_column and filter_value:
        query = query.where(getattr(table.columns, filter_column) == filter_value)

    # Execute the query using the db session
    result = db.session.execute(query.limit(per_page).offset((page - 1) * per_page))

    # Fetch the paginated result as a list
    paginated_data = result.fetchall()

    # Convert the paginated result to a DataFrame
    df = pd.DataFrame(paginated_data, columns=result.keys())

    # Calculate the total_rows separately
    total_rows = db.session.execute(select([func.count()]).select_from(table).where(getattr(table.columns, filter_column) == filter_value)).scalar()

    return {'data': df.to_dict(orient='records'), 'total_rows': total_rows, 'page': page, 'per_page': per_page}


def get_table_as_dataframe_download(table_name, filter_column=None, filter_value=None):
    # Reflect the table using SQLAlchemy
    table = db.Table(table_name, db.metadata, autoload_with=db.engine)

    # Create a SQLAlchemy SELECT query
    query = select([table])

    # Add a filter condition if provided
    if filter_column and filter_value:
        if isinstance(filter_value, (list, tuple, set,)):
            query = query.where(getattr(table.columns, filter_column).in_(filter_value))
        else:
            query = query.where(getattr(table.columns, filter_column) == filter_value)

    # Execute the query using the db session
    result = db.session.execute(query)

    # Fetch all the data
    all_data = result.fetchall()

    # Convert the result to a DataFrame
    df = pd.DataFrame(all_data, columns=result.keys())

    # Calculate the total_rows separately
    total_rows = db.session.execute(select([func.count()]).select_from(table).where(getattr(table.columns, filter_column) == filter_value)).scalar()

    return {'data': df, 'total_rows': total_rows}

def get_table_columns(table):
    column_names = {column.name: column.name for column in table.columns}
    return column_names

def preprocess_columns(columns):
    Journal = getJournals()
    data = get_table_columns(Journal)
    grouped_data = {}
    for column_name in columns:
        unique_values = db.session.query(distinct(getattr(Journal.c, column_name))).all()
        values = [value[0] for value in unique_values if value[0] is not None]
        # Remove complex variables and None values
        if(column_name == "year"):
            grouped_data[column_name] = list(set([int(value) if value.isdigit() else float(value) for value in values]))
        else:
            simple_values = [value for value in values if isinstance(value, str) and not value.startswith("[{") and not value.endswith("}]") and not value.startswith("[") and not value.endswith("]")]
            grouped_data[column_name] = simple_values
    return grouped_data

from sqlalchemy import distinct

def preprocess_grouped_columns():
    Journal = getJournals()
    data = get_table_columns(Journal)
    grouped_data = {}
    
    # Initialize grouped_data with categories
    for key, value in data.items():
        if len(key.split("#")) == 2:
            category, subcategory = key.split("#", maxsplit=1)
            if category not in grouped_data:
                grouped_data[category] = {}
    
    # Fetch unique values for each subcategory from the database
    for key, value in data.items():
        if len(key.split("#")) == 2:
            category, subcategory = key.split("#", maxsplit=1)
            if subcategory not in grouped_data[category]:
                grouped_data[category][subcategory] = set()
            if value:
                # Query the database to retrieve unique values for the column
                unique_values = db.session.query(distinct(getattr(Journal.c, key))).all()
                # Extract values, split them by comma, and flatten the list
                subcategory_values = [val[0].split(', ') if val[0] else [] for val in unique_values]
                subcategory_values = [item for sublist in subcategory_values for item in sublist]
                # Add unique values to the set
                grouped_data[category][subcategory].update(subcategory_values)
    
    # Convert sets to sorted lists
    for category, subcategories in grouped_data.items():
        for subcategory, values in subcategories.items():
            # Check if the set contains only one element equal to the subcategory
            if len(values) == 1 and subcategory in values:
                grouped_data[category][subcategory] = subcategory
            else:
                grouped_data[category][subcategory] = sorted(values)

    # Convert subcategory dictionaries to lists if applicable
    for category, subcategories in grouped_data.items():
        if all(isinstance(value, str) for value in subcategories.values()):
            grouped_data[category] = sorted(subcategories.values())

    return grouped_data