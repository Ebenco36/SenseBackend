import os
from app import app
import logging
from logging.handlers import RotatingFileHandler
from database.services import db, get_columns_from_table
from flask import request, jsonify

port = os.getenv("FLASK_RUN_PORT")
host = os.getenv("FLASK_RUN_HOST")
app.config.from_object(os.getenv('APP_SETTINGS'))


@app.route('/datatable')
def index():
    # Replace 'your_table_name' with the actual table name in your database
    table_name = 'common_columns'
    
    # Replace 'column1, column2' with the columns you want to retrieve

    columns_to_select = """`Outcome#Death`, `Intervention#Vaccine-Options`, 
        `Intervention#Vaccine-preventable-disease`, `Outcome#Hospitalization`, `Outcome#ICU`, 
        `Outcome#Infection`, PII, `Population#AgeGroup`, `Population#ImmuneStatus`, 
        `Population#SpecificGroup`, `Topic#Acceptance`, `Topic#Administration`, 
        `Topic#Coverage`, `Topic#Economic-Aspects`, `Topic#Efficacy-Effectiveness`, 
        `Topic#Ethical-Issues`, `Topic#Modeling`, `Topic#Risk-Factor`, `Topic#Safety`,
        `abstract`, `country`, `full_text_URL`, `doi`, `journal`, `region`, `title`, `year`
    """
    # Get parameters from the request
    page = request.args.get('page', default=1, type=int)
    per_page = request.args.get('per_page', default=10, type=int)
    search_query = request.args.get('search_query', default='')
    search_columns = request.args.get('search_columns', default='').split(',')

    # Get columns using the service function
    data = get_columns_from_table(table_name, columns_to_select, page, per_page, search_query, search_columns)


    return jsonify(data)

# Configure logging
if not app.debug:
    # Set the log level
    log_level = logging.INFO

    # Create a file handler that logs messages to a file
    log_file = './error.log'
    file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 10, backupCount=5)
    file_handler.setLevel(log_level)

    # Create a log formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    # Add the file handler to the app's logger
    app.logger.addHandler(file_handler)
    
if __name__ == '__main__':
    app.run(host=host, port=port)
