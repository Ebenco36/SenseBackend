# Define the RegionCountry model
from database.db import db
class RegionCountry(db.Model):
    __tablename__ = 'region_country'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    region = db.Column(db.String(100), nullable=False)
    country = db.Column(db.String(100), nullable=False)

    def __init__(self, region, country):
        self.region = region
        self.country = country

    def __repr__(self):
        return f"<RegionCountry(region_name='{self.region_name}', country_name='{self.country_name}')>"