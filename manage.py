from app import app
import os, click
from database.db import db
from flask_script import Manager
from flask_migrate import Migrate
from flask.cli import FlaskGroup
from src.Commands.migrateCommand import generate_model_class, load_csv_data
from src.Commands.migrateCommandMPstruct import generate_model_class_MPSTRUCT, load_csv_data_MPSTRUCT
from src.Commands.migrateCommandPDB import generate_model_class_PDB, load_csv_data_PDB

migrate = Migrate(app, db)
manager = Manager(app)

@app.cli.command("sync-protein-database")
def init_migrate_mpstruct_upgrade():
    
    # Path to the directory where migrations will be stored
    migrations_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'migrations')

    if not os.path.exists(migrations_dir):
        with app.app_context():
            click.echo("Running 'Initialize Database Migrations'")
            os.system('flask db init')
    # Example usage:
    model_class_mpstruct = generate_model_class_MPSTRUCT('./datasets/Mpstruct_dataset.csv', 'src/MP/model_mpstruct.py')
    model_class_pdb = generate_model_class_PDB('./datasets/PDB_data.csv', 'src/MP/model_pdb.py')
    model_class = generate_model_class('./datasets/Quantitative_data.csv', 'src/MP/model.py')

    """Initialize, migrate, and upgrade the database."""
    with app.app_context():
        """Initialize, migrate, and upgrade the database."""
        click.echo("Running 'flask db init'")
        click.echo("This step is optional if you have already initialized migrations manually.")
        click.echo("If you haven't, you can run 'flask db init' separately.")
        
        click.echo("Running 'flask db migrate'")
        os.system('flask db migrate')
        
        click.echo("Running 'flask db upgrade'")
        os.system('flask db upgrade')
        
        click.echo("Database initialization, migration, and upgrade completed.")

    db.create_all()   
    # Call the function to load data into the database
    load_csv_data_MPSTRUCT(model_class_mpstruct, './datasets/Mpstruct_dataset.csv')
    load_csv_data_PDB(model_class_pdb, './datasets/PDB_data.csv')
    load_csv_data(model_class, './datasets/Quantitative_data.csv')
    
if __name__ == '__main__':
    manager.run()
