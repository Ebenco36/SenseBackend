import sys
import os
sys.path.append(os.getcwd())
from sqlalchemy import create_engine, Column, String, Table, MetaData
from src.Services.PostgresService import PostgresService
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
import pycountry_convert as pc


class CountryRegionManager:
    def __init__(self):
        """
        Initialize the CountryRegionManager with a database URL.
        """
        db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(db_url, pool_size=10, max_overflow=20)
        self.metadata = MetaData()
        self.table_name = "region_country"

        # Define the table schema
        self.region_country_table = Table(
            self.table_name, self.metadata,
            Column("country", String, primary_key=True),
            Column("region", String)
        )

        # Create the table if it doesn't exist
        self.metadata.create_all(self.engine)

        # Initialize session
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        
        self.db_service = PostgresService()
        
        self.country_aliases = {
            "Kyrgyztan": "Kyrgyzstan",
            "China (Republic : 1949- )": "China",
            "England": "United Kingdom",
            "Korea (South)": "South Korea",
            "Russia (Federation)": "Russia",
            "Russian Federation": "Russia",
            "Scotland": "United Kingdom",
            "Taiwan (Republic of China)": "Taiwan",
            "Taiwan, Province of China": "Taiwan"
        }

    def get_region_by_country(self, country_name):
        """
        Get the region for a country. If not available, attempt to fetch and update it.

        Parameters:
            country_name (str): The name of the country.

        Returns:
            str: The region of the country or an appropriate message.
        """
        conn = self.engine.connect()

        # Check if the country exists in the table
        query = self.region_country_table.select().where(
            self.region_country_table.c.country == country_name
        )
        result = conn.execute(query).fetchone()

        if result:
            if result["region"]:
                return result["region"]  # Return the existing region
            else:
                # Fetch and update the region if not set
                region = self._fetch_region(country_name)
                self.update_region(country_name, region)
                return region
        else:
            # Add country to the table and fetch its region
            region = self._fetch_region(country_name)
            self.add_country(country_name, region)
            return region

    def get_regions_for_countries(self, countries):
        """
        Retrieves or updates the regions for a list of countries in bulk.

        Parameters:
            countries (list): A list of country names.

        Returns:
            dict: A dictionary mapping country names to their regions.
        """
        conn = self.engine.connect()

        # Fetch existing countries from the database
        query = self.region_country_table.select().where(
            self.region_country_table.c.country.in_(countries)
        )
        existing_entries = conn.execute(query).fetchall()

        # Map existing countries to their regions
        existing_countries = {entry["country"]: entry["region"] for entry in existing_entries}

        # Prepare results and identify missing regions
        results = {}
        countries_to_update = []

        for country in countries:
            if country in existing_countries:
                if existing_countries[country]:  # Region is already populated
                    results[country] = existing_countries[country]
                else:  # Region is missing, need to update
                    countries_to_update.append(country)
            else:
                # Country not in database, need to fetch and insert
                countries_to_update.append(country)

        # Fetch regions for missing countries
        regions_to_update = {country: self._fetch_region(country) for country in countries_to_update}

        # Insert or update missing countries in the database
        for country, region in regions_to_update.items():
            if country in existing_countries:
                # Update existing country
                self.update_region(country, region)
            else:
                # Add new country
                self.add_country(country, region)

            # Add to results
            results[country] = region

        return results

    def add_country(self, country_name, region=None):
        """
        Add a country to the table.

        Parameters:
            country_name (str): The name of the country.
            region (str): The region of the country (optional).
        """
        conn = self.engine.connect()
        try:
            insert_query = self.region_country_table.insert().values(
                country=country_name, region=region
            )
            conn.execute(insert_query)
        except IntegrityError:
            print(f"Country '{country_name}' already exists.")

    def update_region(self, country_name, region):
        """
        Update the region for a country.

        Parameters:
            country_name (str): The name of the country.
            region (str): The new region value.
        """
        conn = self.engine.connect()
        update_query = self.region_country_table.update().where(
            self.region_country_table.c.country == country_name
        ).values(region=region)
        conn.execute(update_query)

    def _fetch_region(self, country_name):
        """
        Fetch the region of a country using pycountry_convert, with support for alternate names.

        Parameters:
            country_name (str): The name of the country.

        Returns:
            str: The region of the country or 'Unknown' if not found.
        """
        try:
            # Use alias if available
            standardized_name = self.country_aliases.get(country_name, country_name)

            # Convert country name to ISO alpha-2 code
            country_code = pc.country_name_to_country_alpha2(standardized_name)

            # Convert country code to continent code and name
            continent_code = pc.country_alpha2_to_continent_code(country_code)
            return pc.convert_continent_code_to_continent_name(continent_code)
        except Exception as e:
            print(f"Error fetching region for {country_name}: {e}")
            return "Unknown"


    def apply(self):
        # List of countries to process
        countries = self.db_service.get_unique_items_from_column("all_db", "Country")

        # Retrieve all regions in one call
        regions = self.get_regions_for_countries(countries)
        
        return regions


if __name__ == '__main__':
    obj = CountryRegionManager()
    print(obj.apply())
