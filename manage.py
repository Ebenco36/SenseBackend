from app import app
import os
from database.db import db
from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand
from models.membraneProteinsModel import read_csv_and_create_model


#read_csv_and_create_model('./results/ProcessedData.csv', "membrane_proteins")
migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command('db', MigrateCommand)

if __name__ == '__main__':
    manager.run()
