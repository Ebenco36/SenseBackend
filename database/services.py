from database.db import db

def get_columns_from_table(table_name, columns_to_select, page=1, per_page=10, search_query=None, search_columns=None):
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
