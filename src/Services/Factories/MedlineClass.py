from Bio import Entrez, Medline
import pandas as pd
from io import StringIO
import time
import requests
from tqdm import tqdm  # Progress bar library

class MedlineClass:
    def __init__(self):
        """
        Initializes the MedlineClass with email and API key for Entrez.
        """
        Entrez.email = "ebenco94@gmail.com"
        Entrez.api_key = "d4658719b8b55fb6817d221776bbddece608"

    def validate_query(self, query):
        """
        Validates the query to ensure it is meaningful.
        
        Parameters:
            query (str): The search query.
        
        Returns:
            bool: True if the query is valid, False otherwise.
        """
        if not query.strip():
            print("Empty query. Skipping.")
            return False
        if len(query.strip()) < 3:  # Avoid single characters or very short queries
            print(f"Query '{query}' is too short. Skipping.")
            return False
        return True

    def search_medline(self, query):
        """
        Searches for article IDs in PubMed using a query, retrieving all available records.
        
        Parameters:
            query (str): The search term for PubMed.
            
        Returns:
            list: A list of PubMed article IDs.
        """
        all_ids = []
        retmax = 10000
        retstart = 0

        while True:
            try:
                handle = Entrez.esearch(db="pubmed", term=query, retmax=retmax, retstart=retstart)
                record = Entrez.read(handle)
                handle.close()

                all_ids.extend(record["IdList"])

                # If fewer results than retmax are returned, we're done
                if len(record["IdList"]) < retmax:
                    break

                retstart += retmax
                time.sleep(0.5)  # To avoid exceeding rate limits
            except Exception as e:
                print(f"Error during search with query '{query}': {e}")
                break

        print(f"Total IDs retrieved for query '{query}': {len(all_ids)}")
        return all_ids

    def fetch_details(self, id_list):
        """
        Fetches article details in MEDLINE format for given article IDs in batches.
        
        Parameters:
            id_list (list): List of PubMed article IDs.
            
        Returns:
            list: A list of article records as dictionaries.
        """
        records = []
        batch_size = 5000

        print(f"📥 Fetching details for {len(id_list)} articles...")
        for start in tqdm(range(0, len(id_list), batch_size), desc="📄 Downloading records", unit="batch"):
            batch_ids = ",".join(id_list[start:start + batch_size])

            try:
                handle = Entrez.efetch(db="pubmed", id=batch_ids, rettype="medline", retmode="text")
                batch_records = Medline.parse(StringIO(handle.read()))
                records.extend(batch_records)
                handle.close()
                time.sleep(0.5)  # Respect NCBI rate limits
            except Exception as e:
                print(f"Error fetching batch {start}: {e}")
                continue

        print(f"Total records fetched: {len(records)}")
        return list(records)

    def clean_doi(self, doi_list):
        """
        Cleans DOI strings to remove unwanted annotations like '[doi]'.
        
        Parameters:
            doi_list (list): List of DOIs from the record.
        
        Returns:
            str: Cleaned DOI string.
        """
        return "; ".join([doi.split()[0] for doi in doi_list if "doi" in doi.lower()])

    def clean_dataset(self, df):
        """
        Cleans a dataset by normalizing, parsing, and handling missing or inconsistent data.

        Parameters:
            df (DataFrame): Raw dataset containing Medline data.

        Returns:
            DataFrame: A cleaned and processed dataset.
        """
        # Normalize column names
        df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

        # Handle missing data
        df.fillna({
            'abstract': 'No abstract available', 
            'authors': 'Unknown',
            'doi': 'Unknown DOI',
            'publication_type': 'Unknown',
            'country': 'Unknown'
        }, inplace=True)

        # Clean abstract field
        if 'abstract' in df.columns:
            df['abstract'] = df['abstract'].str.replace('\n', ' ').str.strip()

        # Extract publication year
        if 'publication_date' in df.columns:
            df['publication_year'] = pd.to_datetime(df['publication_date'], errors='coerce').dt.year

        # Remove duplicate entries based on PMID
        if 'pmid' in df.columns:
            df.drop_duplicates(subset=['pmid'], inplace=True)

        return df

    def generate_random_email(self, domain="gmail.com"):
        import random
        import string
        """Generates a random email address for Unpaywall API access."""
        local_part = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        email = f"{local_part}@{domain}"
        # print(f"Generated email: {email}")
        return email
    
    def fetch_from_unpaywall(self, doi):
        """
        Uses Unpaywall API to fetch open access PDF URL for a given DOI.

        Args:
            doi (str): The DOI to fetch the open access link.

        Returns:
            list: List containing the open access PDF URL if available.
        """
        email = self.generate_random_email()
        try:
            unpaywall_api_url = f"https://api.unpaywall.org/v2/{doi}?email={email}"
            response = requests.get(unpaywall_api_url)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Unpaywall: {e}")
            return {}
        
    def fetch(self, queries):
        """
        Fetches articles for all queries, preprocesses, and saves cleaned data in CSV format.
        
        Parameters:
            queries (list): List of search queries for PubMed.
        """
        all_data = []

        for query in queries:
            if not self.validate_query(query):
                continue  # Skip invalid queries

            # print(f"Processing query: {query}")
            id_list = self.search_medline(query)

            if not id_list:
                print(f"No results found for query: {query}")
                continue

            print(f"Found {len(id_list)} articles for query: {query}")
            records = self.fetch_details(id_list)
                
            # Convert each Medline record to a dictionary
            print("📝 Processing records...")
            for record in tqdm(records, desc="🛠 Processing", unit="record"):
                get_other_fields = {}
                doi = self.clean_doi(record.get("AID", []))
                if doi:
                    get_other_fields = self.fetch_from_unpaywall(doi)
                
                record_data = {
                    "PMID": record.get("PMID", ""),
                    "Title": record.get("TI", ""),
                    "Abstract": record.get("AB", ""),
                    "Authors": "; ".join(record.get("AU", [])),
                    "Publication Date": record.get("DP", ""),
                    "Journal": record.get("JT", ""),
                    "Country": record.get("PL", ""),
                    "Language": "; ".join(record.get("LA", [])),
                    "MeSH Terms": "; ".join(record.get("MH", [])),
                    "Publication Type": "; ".join(record.get("PT", [])),
                    "DOI": doi,
                    # "Query": query,
                    "year": get_other_fields.get("year", ""),
                    "open_access": "Open Access" if get_other_fields.get("is_oa", False) == True else "Not Open Access",
                }
                all_data.append(record_data)

        # Create a DataFrame and preprocess it
        df = pd.DataFrame(all_data)

        if not df.empty:
            print(f"🧹 Cleaning dataset with {len(df)} records...")
            df = self.clean_dataset(df)
            output_path = "Data/MedlineData/medline_results.csv"
            df.to_csv(output_path, index=False)
            print(f"Data saved to {output_path}")
        else:
            print("No data to save.")

# Example usage:
# queries = ["antimicrobial resistance", "machine learning in healthcare"]
# medline = MedlineClass()
# medline.fetch(queries)
