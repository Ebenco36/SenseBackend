import os
from flask import Flask, jsonify
from database.db import db
from flask_cors import CORS
from dotenv import load_dotenv
from utils.errors import BadRequestException
from blueprints.users import users_blueprint
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from Controllers.DataController import dataPage, getFieldValues, get_row_by_id
from App.api_utils import embase_access, ilove_access, furtherProcessEmbase
from utils.http import bad_request, not_found, not_allowed, internal_error

load_dotenv()  # load env files

def create_app():
    app = Flask(__name__)
    app.config.from_object(os.getenv('APP_SETTINGS'))
    app.url_map.strict_slashes = False
    db.init_app(app)
    CORS(app)

    app.register_blueprint(users_blueprint, url_prefix='/api/v1')
    
    
    @app.route('/spool-from-embase', methods=['GET', 'POST'])
    def embase_endpoint():
        embase_access()
        return jsonify({})

    @app.route('/spool-from-embase-process', methods=['GET', 'POST'])
    def embase_endpoint_processing():
        # add a job for this.
        furtherProcessEmbase()
        return jsonify({})

    @app.route('/spool-from-ilove', methods=['GET', 'POST'])
    def iLove_endpoint():
        ilove_access()
        return jsonify({})

    @app.route('/page-content', methods=['GET', 'POST'])
    def get():
        return dataPage()


    @app.route('/get_row/<int:row_id>', methods=['GET'])
    def getSingleDataById(row_id):
        return get_row_by_id(row_id)

    @app.route('/page-filter', methods=['GET'])
    def getFilters():
        return getFieldValues()


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
        return internal_error()

    return app


app = create_app()
