from Bio import Entrez, Medline
import pandas as pd
from io import StringIO

class MedlineClass:
    def __init__(self):
        """
        Initializes the MedlineClass with email and API key for Entrez.
        
        Parameters:
            email (str): Your email address for NCBI access.
            api_key (str): Your NCBI API key.
        """
        Entrez.email = "ebenco94@gmail.com"
        Entrez.api_key = "d4658719b8b55fb6817d221776bbddece608"

    def search_medline(self, query):
        """
        Searches for article IDs in PubMed using a query, retrieving all available records.
        
        Parameters:
            query (str): The search term for PubMed.
            
        Returns:
            list: A list of PubMed article IDs.
        """
        # Set retmax to a high number to retrieve all records (up to NCBI's limit)
        handle = Entrez.esearch(db="pubmed", term=query, retmax=100000)
        record = Entrez.read(handle)
        handle.close()
        return record["IdList"]

    def fetch_details(self, id_list):
        """
        Fetches article details in MEDLINE format for given article IDs.
        
        Parameters:
            id_list (list): List of PubMed article IDs.
            
        Returns:
            list: A list of article records as dictionaries.
        """
        ids = ",".join(id_list)
        handle = Entrez.efetch(db="pubmed", id=ids, rettype="medline", retmode="text")
        records = Medline.parse(StringIO(handle.read()))
        handle.close()
        return list(records)

    def fetch(self, queries):
        """
        Fetches articles for all queries, and saves data in CSV format.
        
        Parameters:
            queries (list): List of search queries for PubMed.
            output_file (str): Name of the output CSV file.
        """
        all_data = []

        for query in queries:
            id_list = self.search_medline(query)
            records = self.fetch_details(id_list)

            # Process each record and convert to a dictionary
            for record in records:
                record_data = {
                    "PMID": record.get("PMID", ""),
                    "Status": record.get("STAT", ""),
                    "Journal ID": record.get("JID", ""),
                    "Volume": record.get("VI", ""),
                    "Issue": record.get("IP", ""),
                    "Publication Date": record.get("DP", ""),
                    "Title": record.get("TI", ""),
                    "Pages": record.get("PG", ""),
                    "DOI": record.get("AID", ""),
                    "Abstract": record.get("AB", ""),
                    "Authors": "; ".join(record.get("AU", [])),
                    "Author Affiliations": "; ".join(record.get("AD", [])),
                    "Language": record.get("LA", ""),
                    "Publication Type": record.get("PT", ""),
                    "Country": record.get("PL", ""),
                    "MeSH Terms": "; ".join(record.get("MH", [])),
                    "Substances": "; ".join(record.get("RN", [])),
                    "Source": record.get("SO", ""),
                    "Query": query
                }
                all_data.append(record_data)

        # Save all data to CSV
        df = pd.DataFrame(all_data)
        df.to_csv("Medline-base/medline_results.csv", index=False)
        print(f"Data saved to Medline-base/medline_results.csv")

# Example usage:
# email = "your_email@example.com"
# api_key = "your_api_key"
# queries = ["antimicrobial resistance", "machine learning in healthcare"]
# medline = MedlineClass(email, api_key)
# medline.fetch(queries)
