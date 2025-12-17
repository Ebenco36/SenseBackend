import psycopg
import requests
import re
import random
import string
import json

class OpenAccessSearch:
    """
    A class to search for open access information and raw data of a given DOI from multiple sources.
    It can connect to a PostgreSQL database to fetch DOIs and update records.
    """

    def __init__(self, db_params=None):
        """
        Initializes the OpenAccessSearch class.

        Args:
            db_params (dict, optional): A dictionary with database connection parameters.
                                        Example: {'dbname': 'your_db', 'user': 'your_user', ...}
                                        If None, database functions will not be available.
        """
        self.db_params = db_params
        # The order in this list determines the priority for find_open_access
        self.api_endpoints = [
            'unpaywall',
            'openalex',
            'europepmc',
        ]

    def _get_db_connection(self):
        """Establishes and returns a connection to the PostgreSQL database."""
        if not self.db_params:
            print("Database parameters not configured.")
            return None
        try:
            conn = psycopg.connect(**self.db_params)
            return conn
        except psycopg.OperationalError as e:
            print(f"Error connecting to the database: {e}")
            return None

    def get_dois_from_db(self, table_name, doi_column_name, primary_key_column_name):
        """
        Retrieves primary keys and DOIs for unprocessed records from a database table.

        Args:
            table_name (str): The name of the table to query.
            doi_column_name (str): The name of the column containing DOIs.
            primary_key_column_name (str): The name of the primary key column.

        Returns:
            list: A list of (primary_key, doi) tuples. Returns an empty list on error.
        """
        conn = self._get_db_connection()
        if not conn:
            return []

        records = []
        try:
            with conn.cursor() as cur:
                # Query for records that haven't been processed yet (where open_access is NULL or empty).
                # Using f-string for table/column names is safe here as they come from the script, not user input.
                cur.execute(f'SELECT "{primary_key_column_name}", "{doi_column_name}" FROM "{table_name}" WHERE "open_access" IS NULL OR "open_access"=\'\';')
                rows = cur.fetchall()
                # returns a list of tuples (primary_key, doi)
                records = [(row[0], row[1]) for row in rows if row[1]]
        except psycopg.Error as e:
            print(f"Database query error: {e}")
        finally:
            if conn:
                conn.close()
        return records

    def _extract_doi(self, doi_string):
        """
        Extracts a DOI from a string, which could be a URL or just the DOI itself.

        Args:
            doi_string (str): The string containing the DOI.

        Returns:
            str: The extracted DOI, or None if no DOI is found.
        """
        if not doi_string:
            return None
        # Regex to find a DOI, including those in URLs
        doi_regex = r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)"
        match = re.search(doi_regex, doi_string, re.IGNORECASE)
        return match.group(1) if match else None

    def generate_random_email(self):
        """Generates a random email address for API requests that require one."""
        domain = "example.com"
        username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
        return f"{username}@{domain}"

    def fetch_from_europepmc(self, doi):
        """
        Uses Europe PMC API to fetch publication data.

        Args:
            doi (str): The DOI to search for.

        Returns:
            dict: The JSON response from the API, or an empty dictionary on error.
        """
        try:
            api_url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:{doi}&format=json"
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Europe PMC: {e}")
            return {"error": str(e)}

    def fetch_from_openalex(self, doi):
        """
        Uses OpenAlex API to fetch publication data.

        Args:
            doi (str): The DOI to search for.

        Returns:
            dict: The JSON response from the API, or an empty dictionary on error.
        """
        try:
            api_url = f"https://api.openalex.org/works?filter=doi:{doi}"
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from OpenAlex: {e}")
            return {"error": str(e)}

    def fetch_from_unpaywall(self, doi):
        """
        Uses Unpaywall API to fetch open access data.

        Args:
            doi (str): The DOI to search for.

        Returns:
            dict: The JSON response from the API, or an empty dictionary on error.
        """
        email = self.generate_random_email()
        try:
            api_url = f"https://api.unpaywall.org/v2/{doi}?email={email}"
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Unpaywall: {e}")
            return {"error": str(e)}

    def get_all_data(self, doi_string):
        """
        Fetches the complete raw data from all configured API endpoints for a given DOI.

        Args:
            doi_string (str): The string containing the DOI (can be a URL).

        Returns:
            dict: A dictionary containing the raw JSON response from each data source.
        """
        doi = self._extract_doi(doi_string)
        if not doi:
            return {"error": "Invalid DOI or URL provided."}

        all_data = {}
        all_data['europepmc'] = self.fetch_from_europepmc(doi)
        all_data['openalex'] = self.fetch_from_openalex(doi)
        all_data['unpaywall'] = self.fetch_from_unpaywall(doi)

        return all_data

    def find_open_access(self, doi_string):
        """
        Sequentially queries APIs to find metadata and the best open access link for a DOI.
        It gathers metadata regardless of OA status.

        Args:
            doi_string (str): The string containing the DOI (can be a URL).

        Returns:
            dict: A dictionary with the publication details, or a message if not found.
        """
        doi = self._extract_doi(doi_string)
        if not doi:
            return {"error": "Invalid DOI or URL provided."}

        print(f"\nSearching for metadata for DOI: {doi}")
        all_data = self.get_all_data(doi)
        
        # This dictionary will hold the best data we can find.
        found_data = {'doi': doi, 'is_oa': False, 'url': None, 'publication_date': None, 'publication_year': None, 'journal_name': None, 'authors': None, 'source': None}
        
        # --- Data Extraction from Unpaywall ---
        unpaywall_data = all_data.get('unpaywall')
        if unpaywall_data and not unpaywall_data.get('error'):
            found_data['is_oa'] = unpaywall_data.get('is_oa', False)
            if found_data['is_oa']:
                 found_data['url'] = unpaywall_data.get('best_oa_location', {}).get('url_for_pdf') or unpaywall_data.get('best_oa_location', {}).get('url')
            found_data['publication_date'] = unpaywall_data.get('published_date')
            found_data['publication_year'] = unpaywall_data.get('year')
            found_data['journal_name'] = unpaywall_data.get('journal_name')
            authors_list = [f"{author.get('given', '')} {author.get('family', '')}".strip() for author in unpaywall_data.get('z_authors', [])]
            found_data['authors'] = ', '.join(filter(None, authors_list))
            found_data['source'] = 'Unpaywall' # Mark that we got data from here

        # --- Data Extraction from OpenAlex (can override Unpaywall if better) ---
        openalex_data = all_data.get('openalex')
        if openalex_data and openalex_data.get('results'):
            result = openalex_data['results'][0]
            
            # Safely get primary_location, defaulting to an empty dict if it's missing or None
            primary_location = result.get('primary_location') or {}

            # Update fields only if they are currently empty
            if not found_data['publication_date']: found_data['publication_date'] = result.get('publication_date')
            if not found_data['publication_year']: found_data['publication_year'] = result.get('publication_year')
            
            # Safely get journal name
            if not found_data['journal_name']:
                # Default to an empty dict if 'source' is missing or None to prevent error
                source = primary_location.get('source') or {}
                found_data['journal_name'] = source.get('display_name')

            # Safely get URL from landing page if our main URL is still empty
            if not found_data['url']:
                found_data['url'] = primary_location.get('landing_page_url')

            if not found_data['authors']:
                authorships = result.get('authorships') or []
                authors_list = [authorship.get('author', {}).get('display_name') for authorship in authorships]
                found_data['authors'] = ', '.join(filter(None, authors_list))
            
            if not found_data['source']: found_data['source'] = 'OpenAlex'

        # --- Data Extraction from Europe PMC (lowest priority, fills remaining gaps) ---
        europepmc_data = all_data.get('europepmc')
        if europepmc_data and europepmc_data.get('resultList', {}).get('result'):
            result = europepmc_data['resultList']['result'][0]
            if not found_data['publication_date']: found_data['publication_date'] = result.get('firstPublicationDate')
            if not found_data['publication_year']: found_data['publication_year'] = result.get('pubYear')
            if not found_data['journal_name']: found_data['journal_name'] = result.get('journalTitle')
            if not found_data['authors']: found_data['authors'] = result.get('authorString', '')
            if not found_data['source']: found_data['source'] = 'Europe PMC'

        # If we couldn't find any data from any source, return a failure message.
        if not found_data['source']:
            print("No data found for this DOI from any source.")
            return {"doi": doi, "message": "Record not found in any data source."}

        print(f"Data compiled for DOI {doi} from best available sources.")
        return found_data

    def update_db_record(self, table_name, primary_key_column_name, primary_id, access_info):
        """
        Updates a record in the database with the fetched publication information.

        Args:
            table_name (str): The name of the table to update.
            primary_key_column_name (str): The name of the primary key column.
            primary_id (any): The primary key of the record to update.
            access_info (dict): A dictionary containing the information to update.
        """
        # Do not update if the search returned an error or a 'not found' message.
        if not access_info or access_info.get('message') or access_info.get('error'):
            print(f"Skipping DB update for record ID {primary_id}: No valid data found.")
            return

        conn = self._get_db_connection()
        if not conn:
            print(f"Skipping DB update for record ID {primary_id}: Could not connect to DB.")
            return

        try:
            with conn.cursor() as cur:
                # IMPORTANT: Your table needs these columns.
                # Based on your error logs, the columns are: open_access, url, authors, date, year, journal
                
                # Quoting all identifiers (table and column names) to prevent errors with reserved words.
                # Removed the "source" column from the update statement to fix the trailing comma error.
                update_query = f"""
                    UPDATE "{table_name}"
                    SET "open_access" = %s,
                        "authors" = %s,
                        "publication_date" = %s,
                        "year" = %s,
                        "journal" = %s
                    WHERE "{primary_key_column_name}" = %s;
                """
                # Convert boolean is_oa to descriptive text for the database
                is_oa_text = "Open Access" if access_info.get('is_oa') else "Not Open Access"
                
                # Prepare data for the query, using None for missing keys
                # The COALESCE function in the query will handle the conditional URL update.
                data_tuple = (
                    is_oa_text,
                    access_info.get('authors'),
                    access_info.get('publication_date'),
                    access_info.get('publication_year'),
                    access_info.get('journal_name'),
                    primary_id  # Use the primary key to find the record
                )
                cur.execute(update_query, data_tuple)
                conn.commit()
                print(f"Successfully updated database for record ID: {primary_id}")

        except psycopg.Error as e:
            print(f"Database update error for record ID {primary_id}: {e}")
            conn.rollback() # Rollback the transaction on error
        finally:
            if conn:
                conn.close()
                
                
                
if __name__ == '__main__':
    # --- Main Execution Block ---

    # 1. Define database connection parameters
    db_connection_params = {
        "dbname": "sense_project",
        "user": "sense_project_user",
        "password": "sense_project_user",
        "host": "localhost",
        "port": "5433"
    }
    
    # 2. Initialize the searcher with DB parameters
    db_searcher = OpenAccessSearch(db_params=db_connection_params)
    
    # 3. Define the table and column names
    table = 'all_db'
    doi_column = 'doi'
    primary_key_column = 'primary_id' # Assuming this is your primary key column
    
    # 4. Fetch all unprocessed records from the specified table
    all_records_from_db = db_searcher.get_dois_from_db(table, doi_column, primary_key_column)
    
    print(f"\nFetched {len(all_records_from_db)} unprocessed records from the database.")
    
    # 5. Process each record
    for primary_id, single_doi in all_records_from_db:
        if not single_doi:
            continue
            
        # Find publication information
        access_info = db_searcher.find_open_access(single_doi)
        print(f"Result for DOI {single_doi}: {json.dumps(access_info, indent=2)}")
        
        # Update the database with the new information using the primary key
        db_searcher.update_db_record(table, primary_key_column, primary_id, access_info)

    print("\n--- Processing complete. ---")