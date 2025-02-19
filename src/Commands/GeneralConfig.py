import os, sys
sys.path.append(os.getcwd())
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()
class DatabaseGeneralSettingManager:
    def __init__(self, database_url):
        """
        Initialize the database connection using DATABASE_URL.
        :param database_url: The database connection URL (e.g., postgresql://user:password@host:port/dbname).
        """
        self.db_params = self.parse_database_url(database_url)
        self.connection = psycopg2.connect(**self.db_params)
        self.cursor = self.connection.cursor()

    def parse_database_url(self, database_url):
        """
        Parses the DATABASE_URL and returns connection parameters.
        :param database_url: The database connection URL string.
        :return: A dictionary of database connection parameters.
        """
        result = urlparse(database_url)
        return {
            "dbname": result.path[1:],  # Remove leading '/' from path
            "user": result.username,
            "password": result.password,
            "host": result.hostname,
            "port": result.port
        }

    def create_table_if_not_exists(self):
        """
        Creates the sense_config table if it does not exist.
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sense_config (
            id SERIAL PRIMARY KEY,
            key VARCHAR(255) UNIQUE NOT NULL,
            values TEXT NOT NULL
        )
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()

    def insert_or_update_row(self, key_name, value):
        """
        Inserts a row into sense_config or updates it if the key already exists.
        """
        insert_query = """
        INSERT INTO sense_config (key, values)
        VALUES (%s, %s)
        ON CONFLICT (key) 
        DO UPDATE SET values = EXCLUDED.values;
        """
        self.cursor.execute(insert_query, (key_name, value))
        self.connection.commit()

    def close_connection(self):
        """
        Closes the database connection.
        """
        self.cursor.close()
        self.connection.close()

# Example usage
DATABASE_URL = os.environ.get("DATABASE_URL")
db_manager = DatabaseGeneralSettingManager(DATABASE_URL)
db_manager.create_table_if_not_exists()
db_manager.insert_or_update_row("JSESSIONID", "cspbwgreclprt160x1~D9C7C63FEB15FC64FD789EF521A41CA0")
db_manager.insert_or_update_row("ovid_url", "https://ovidsp.dc1.ovid.com/ovid-new-a/ovidweb.cgi?=&S=CPJMFPFCENACLECFKPIJKFPMJMMIAA00&Get Bib Display=Titles|G|S.sh.30|1|100&on_msp=1&CitManPrev=S.sh.30|1|100&undefined=Too many results to sort&cmRecords=Ex: 1-4, 7&cmRecords=Ex: 1-4, 7&results_per_page=100&results_per_page=100&startRecord=1&FORMAT=title&FIELDS=SELECTED&output mode=display&WebLinkReturn=Titles=S.sh.30|1|100&FORMAT=title&FIELDS=SELECTED&Datalist=S.sh.30|1|100&gsrd_params=S.sh.30|1|100|&analytics_display=msp&startRecord_subfooter=1&SELECT=S.sh.30|&issue_record_range=1-22563")
db_manager.close_connection()

print("Done Loading the settings...")
