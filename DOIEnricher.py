import pandas as pd
import requests
import time
from urllib.parse import urlparse

class DOIEnricher:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.df = pd.read_csv(csv_file)
    
    def fetch_metadata(self, doi):
        """Fetch metadata from CrossRef using DOI."""
        url = f"https://api.crossref.org/works/{doi}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            work = data.get("message", {})
            
            return {
                "Journal": work.get("container-title", [None])[0],
                "Publisher": work.get("publisher", None),
                "Publication_Year": work.get("published-print", {}).get("date-parts", [[None]])[0][0],
                "Year": work.get("published-print", {}).get("date-parts", [[None]])[0][0],
                "Publication_Date": work.get("issued", {}).get("date-parts", [[None]])[0][0],
                "Language": work.get("language", None),
                "Abstract": work.get("abstract", None),
                "Country": work.get("institution", {}).get("country", None),
                "Citation_Count": work.get("is-referenced-by-count", None),
                "Keywords": ", ".join(work.get("subject", [])),
            }
        except requests.exceptions.RequestException as e:
            print(f"Error fetching metadata for DOI {doi}: {e}")
            return {"Journal": None, "Publisher": None, "Publication_Year": None, "Publication_Date": None, "Language": None, "Abstract": None, "Country": None, "Citation_Count": None, "Keywords": None}
    
    def extract_source(self, doi_url):
        """Extract the source from the DOI URL by taking the host without '.com' or other TLDs."""
        parsed_url = urlparse(doi_url).netloc
        source = parsed_url.split('.')[-2] if parsed_url else None
        return source
    
    def enrich_data(self):
        """Fetch metadata for each DOI and enrich the dataframe."""
        enriched_data = []
        
        # Set up a counter to generate the numeric ID
        for index, row in self.df.iterrows():
            doi = str(row["DOI"]).split("/doi.org/")[-1]  # Extract DOI part only
            metadata = self.fetch_metadata(doi)
            source = self.extract_source(row["DOI"])
            unique_id = index + 1  # Unique ID ranging from 1 to N
            enriched_data.append({**row, **metadata, "Source": source, "unique_id": unique_id})
            time.sleep(1)  # Respect API rate limits
        
        self.df = pd.DataFrame(enriched_data)
    
    def save_enriched_csv(self, output_file="enriched_output.csv"):
        """Save the enriched dataframe to a CSV file."""
        self.df.to_csv(output_file, index=False)
        print(f"Enriched CSV saved as {output_file}")
        
    def run(self):
        """Run the entire enrichment process."""
        self.enrich_data()
        self.save_enriched_csv()
