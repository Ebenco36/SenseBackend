import os
import re
import click
import pandas as pd
import importlib
from app import app
from datetime import datetime
from database.db import db

# Read CSV files into DataFrames
df1 = pd.read_csv('./EMBASE/EMBASE.csv', low_memory=False)
df2 = pd.read_csv('./L-OVE/LOVE.csv', low_memory=False)
df3 = pd.read_csv('./Medline/Medline.csv', low_memory=False)

# Other database
# df4 = pd.read_csv('../Cochrane/Cochrane.csv')
# ...

# Identify common columns
common_columns = list(set(df1.columns) & set(df2.columns) & set(df3.columns))

# Define SQLAlchemy Model for the Database Table
class CommonColumns(db.Model):
    id = db.Column(db.Integer, primary_key=True)

    # Add common columns dynamically with data type
    for col, data_type in [(col, db.String) for col in common_columns]:
        locals()[col] = db.Column(data_type)


# Create database table
with app.app_context():
    db.create_all()

# Seed data into the database
def seed_data():
    for _, row in df1[common_columns].iterrows():
        db.session.add(CommonColumns(**row.to_dict()))

    for _, row in df2[common_columns].iterrows():
        db.session.add(CommonColumns(**row.to_dict()))

    for _, row in df3[common_columns].iterrows():
        db.session.add(CommonColumns(**row.to_dict()))

    db.session.commit()
