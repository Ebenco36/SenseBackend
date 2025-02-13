from app import app
import os, click
from database.db import db
from flask_script import Manager
from flask_migrate import Migrate
from flask.cli import FlaskGroup

migrate = Migrate(app, db)
manager = Manager(app)
    
if __name__ == '__main__':
    manager.run()
