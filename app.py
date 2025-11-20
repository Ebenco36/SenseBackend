import os
import time
import logging
import logging.config
from flask import Flask, g, request, jsonify
from database.db import db
from flask_mail import Mail
from flask_cors import CORS
from flask_admin import Admin
from dotenv import load_dotenv
from src.Services.ApplicationService import ApplicationService
from src.Services.DBservices.SafeRegistryInit import initialize_registry_safely
from src.Utils.filter_structure import FILTER_STRUCTURE
from src.core.route_initializer import RouteInitialization
from utils.errors import BadRequestException
from logging.handlers import RotatingFileHandler
from utils.http import bad_request, not_found, not_allowed, internal_error
from src.middlewares.auth_middleware import token_required
from celery_app import make_celery

load_dotenv()

# Configure logging
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s [%(name)s:%(lineno)d] %(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        },
        'file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'app.log',
            'maxBytes': 10485760,
            'backupCount': 5,
            'formatter': 'detailed'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file']
    }
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def create_app():
    """Application factory"""
    app = Flask(__name__)
    app.config.from_object(os.getenv('APP_SETTINGS'))
    app.url_map.strict_slashes = False
    
    # Initialize extensions BEFORE app_context
    db.init_app(app)
    CORS(app, resources={r"/api/v1/*": {"origins": "*"}}, 
         allow_headers=["Content-Type", "Authorization"],
         supports_credentials=True)
    
    # EMAIL CONFIGURATION
    # app.py - Development/Testing Configuration
    app.config['MAIL_SERVER'] = 'sandbox.smtp.mailtrap.io'
    app.config['MAIL_PORT'] = 2525
    app.config['MAIL_USE_TLS'] = True
    app.config['MAIL_USE_SSL'] = False

    # USE THESE (from Mailtrap dashboard)
    app.config['MAIL_USERNAME'] = 'e0cf39adac5123'
    app.config['MAIL_PASSWORD'] = 'a6276302bb93da'

    # THESE CAN BE ANYTHING (not used for auth)
    app.config['MAIL_DEFAULT_SENDER'] = 'noreply@research.com'
    app.config['CONTACT_RECIPIENT_EMAIL'] = 'ebenco94@gmail.com'

    Mail(app)
    admin = Admin(app)
    celery = make_celery(app)
    
    log_handler = RotatingFileHandler('error.log', maxBytes=1024 * 1024, backupCount=10)
    log_handler.setLevel(logging.ERROR)
    app.logger.addHandler(log_handler)
    
    # ALL initialization INSIDE app_context
    with app.app_context():
        try:
            db.create_all()
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"❌ Database creation error: {str(e)}", exc_info=True)
    
    with app.app_context():
        try:
            logger.info("Starting registry initialization...")
            
            # Initialize registry
            result = initialize_registry_safely(
                app,
                table_names=['all_db']
            )
            
            registry = result['registry']
            app.config['MODEL_REGISTRY'] = registry
            
            logger.info(f"Registry initialized successfully ({result['reflected_count']} tables)")
            
            # Initialize application service WITH registry
            logger.info("Starting application service initialization...")
            app_service = ApplicationService(
                db,
                filter_structure=FILTER_STRUCTURE,
                registry=registry
            )
            
            init_result = app_service.initialize(tables=['all_db'])
            
            app.config['APP_SERVICE'] = app_service
            
            if init_result.get('success'):
                logger.info("Application service initialized successfully")
            else:
                logger.warning(f"⚠️ Application service initialization: {init_result.get('message')}")
        
        except Exception as e:
            logger.error(f"❌ Service initialization error: {str(e)}", exc_info=True)
    
    # Error handlers
    @app.errorhandler(500)
    def internal_server_error(e):
        app.logger.error('An internal server error occurred', exc_info=e)
        return internal_error()
    
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
    def handle_exception(e):
        app.logger.error('An internal server error occurred', exc_info=e)
        return internal_error()
    
    # Protected routes
    @app.route('/api/v1/protected_route')
    @token_required
    def protected_route():
        current_user = g.current_user
        return f'This route is protected. Current user: {current_user.username}'
    
    # Route initialization
    init_route = RouteInitialization()
    init_route.init_app(app)
    
    return app


app = create_app()

if __name__ == '__main__':
    app.run(debug=True)