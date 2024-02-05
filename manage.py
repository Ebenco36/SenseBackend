from app import app
import os, click
from database.db import db
from flask_script import Manager
from flask_migrate import Migrate
from flask.cli import FlaskGroup
from src.Commands.migrateCommand import seed_data

migrate = Migrate(app, db)
manager = Manager(app)

@app.cli.command("sync-journal-database")
def init_migrate_upgrade():
    seed_data()
    
if __name__ == '__main__':
    manager.run()
