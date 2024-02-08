import os
import time
import logging
from flask import Flask, g, request, jsonify
from database.db import db
from flask_mail import Mail
from flask_cors import CORS
from flask_admin import Admin
from dotenv import load_dotenv
from src import RouteInitialization
from utils.errors import BadRequestException
from logging.handlers import RotatingFileHandler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from utils.http import bad_request, not_found, not_allowed, internal_error
from src.middlewares.auth_middleware import token_required
from database.services import filter_non_empty_lists, get_columns_from_table
from src.Journals.services import preprocess_columns, preprocess_grouped_columns
load_dotenv()  # load env files

def create_app():
    app = Flask(__name__)
    app.config.from_object(os.getenv('APP_SETTINGS'))
    app.url_map.strict_slashes = False
    db.init_app(app)
    CORS(app, resources={r"/api/v1/*": {"origins": "*"}}, 
     allow_headers=["Content-Type", "Authorization"],
     supports_credentials=True)
    Mail(app)
    admin = Admin(app)

    
    # Configure logging to write to a file
    log_handler = RotatingFileHandler('error.log', maxBytes=1024 * 1024, backupCount=10)
    log_handler.setLevel(logging.ERROR)
    app.logger.addHandler(log_handler)

    @app.errorhandler(500)
    def internal_server_error(e):
        # Log the error to the configured file
        app.logger.error('An internal server error occurred', exc_info=e)
        return 'Internal Server Error', 500

    """
        Route Implementation. Well structured
    """
    init_route = RouteInitialization()
    init_route.init_app(app)
    
    @app.route('/api/v1/data/filter')
    def indexFilters():
        data = preprocess_grouped_columns()
        other_data = preprocess_columns(["country", "region", "year"])
        data.update(other_data)
        return jsonify(data)


    @app.route('/api/v1/datatable', methods=['POST'])
    def search():
        # Get JSON request data
        request_data = request.json
        search_data = filter_non_empty_lists(request_data if request_data else {})
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=10, type=int)
        search_columns = request.args.get('search_columns', default='').split(',')

        # Replace 'your_table_name' with the actual table name in your database
        table_name = 'common_columns'

        # Replace 'column1, column2' with the columns you want to retrieve
        columns_to_select = "id, `abstract`, `country`, `full_text_URL`, `doi`, `journal`, `region`, `title`, `year`"

    
        # Get columns using the service function
        data = get_columns_from_table(table_name, columns_to_select, page, per_page, search_query=search_data)
        
        return jsonify(data)

    @app.route('/api/v1/datatablesss')
    def indexsssss():
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
        columns_to_select = "`abstract`, `country`, `full_text_URL`, `doi`, `journal`, `region`, `title`, `year`"
        # Get parameters from the request
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=10, type=int)
        search_query = request.args.get('search_query', default='')
        search_columns = request.args.get('search_columns', default='').split(',')

        # Get columns using the service function
        data = get_columns_from_table(table_name, columns_to_select, page, per_page, search_query, search_columns)
        return jsonify(data)


    @app.route('/api/v1/protected_route')
    @token_required
    def protected_route():
        current_user = g.current_user
        return f'This route is protected. Current user: {current_user.username}'

    @app.errorhandler(BadRequestException)
    def bad_request_exception(e):
        return bad_request(e)

    @app.errorhandler(404)
    def route_not_found(e):
        return not_found('route')

    @app.errorhandler(405)
    def method_not_allowed(e):
        return not_allowed()

    @app.errorhandler(Exception)
    def internal_server_error(e):
        # Log the error to the configured file
        app.logger.error('An internal server error occurred', exc_info=e)
        return internal_error()

    return app

app = create_app()
