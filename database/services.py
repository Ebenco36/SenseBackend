from database.db import db
from sqlalchemy import select, func

def get_columns_from_tablessss(table_name, columns_to_select, page=1, per_page=10, search_query=None, search_columns=None):
    # Start building the SQL query
    query = f'SELECT {columns_to_select} FROM {table_name}'

    # Add search conditions for specified columns if a search query is provided
    if search_query and search_columns:
        search_conditions = [f"{column} LIKE '%{search_query}%'" for column in search_columns]
        query += f" WHERE {' OR '.join(search_conditions)}"

    # Add pagination
    offset = (page - 1) * per_page
    query += f' LIMIT {per_page} OFFSET {offset}'

    # Execute the query
    result = db.engine.execute(query)
    rows = result.fetchall()
    data = [dict(row) for row in rows]
    return data


def get_columns_from_table(table_name, columns_to_select, page=1, per_page=10, search_query=None):
    # Start building the SQL query
    query = f'SELECT {columns_to_select} FROM {table_name}'
    total_query = f'SELECT COUNT(*) FROM {table_name}'
    # Add search conditions for specified columns if a search query is provided
    search_values = []
    search_conditions = []
    if search_query :
        for column, values in search_query.items():
            if values:
                placeholder = ', '.join(['?'] * len(values))
                search_conditions.append(f"`{column}` IN ({placeholder})")
                search_values.extend(values)

        if search_conditions:
            query += f" WHERE {' AND '.join(search_conditions)}"
            total_query += f" WHERE {' AND '.join(search_conditions)}"
            
    if search_values:
        # Execute the query to get total rows count
        total_result = db.engine.execute(total_query, search_values)
    else:
        total_result = db.engine.execute(total_query)
    
    total_rows = total_result.scalar()
    
    # Add pagination
    offset = (page - 1) * per_page
    query += f' LIMIT {per_page} OFFSET {offset}'
    
    # Execute the query
    if search_values:
        result = db.engine.execute(query, search_values)
    else:
        result = db.engine.execute(query)
    rows = result.fetchall()
    data = [dict(row) for row in rows]

    return {'data': data, 'total_rows': total_rows, 'page': page, 'per_page': per_page}


def filter_non_empty_lists(dictionary):
    """
    Filters out key-value pairs where the value is an empty list.

    Args:
    dictionary (dict): The input dictionary.

    Returns:
    dict: A new dictionary containing key-value pairs where the value is a non-empty list.
    """
    return {key: value for key, value in dictionary.items() if value}