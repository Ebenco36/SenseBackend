import sys
import os
sys.path.append(os.getcwd())
import pandas as pd
import requests
import time
from urllib.parse import urlparse

class DOIEnricher:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.df = pd.read_csv(csv_file)
        self.not_found_dois = []  # Store DOIs we couldn't enrich
    
    def fetch_metadata(self, doi):
        """Fetch metadata from CrossRef using DOI."""
        url = f"https://api.crossref.org/works/{doi}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            work = data.get("message", {})
            
            # If metadata is missing or empty
            if not work:
                self.not_found_dois.append(doi)
                return None
            
            return {
                "publisher": work.get("publisher", None),
                "language": work.get("language", None),
                "citation_count": work.get("is-referenced-by-count", None),
            }
        except requests.exceptions.RequestException as e:
            print(f"Error fetching metadata for DOI {doi}: {e}")
            self.not_found_dois.append(doi)
            return {"Journal": None, "Publisher": None, "Publication_Year": None, "Publication_Date": None, "Language": None, "Abstract": None, "Country": None, "Citation_Count": None, "Keywords": None}
    
    def extract_source(self, doi_url):
        """Extract the source from the DOI URL by taking the host without '.com' or other TLDs."""
        parsed_url = urlparse(doi_url).netloc
        source = parsed_url.split('.')[-2] if parsed_url else None
        return source
    
    def enrich_data(self, key="DOI"):
        """Fetch metadata for each DOI and enrich the dataframe."""
        enriched_data = []
        
        # Set up a counter to generate the numeric ID
        for index, row in self.df.iterrows():
            # Skip if language is already set
            if pd.notna(row.get("language")) and str(row["language"]).strip() != "":
                enriched_data.append(row.to_dict())  # retain the row as is
                continue

            doi = str(row[f"{key}"]).split("/doi.org/")[-1]  # Extract DOI part only
            # Skip if DOI is empty
            if doi == "" or pd.isna(doi):
                enriched_data.append(row.to_dict())
                continue

            metadata = self.fetch_metadata(doi)
            enriched_row = {**row.to_dict(), **metadata}
            enriched_data.append(enriched_row)
            time.sleep(1)  # Respect API rate limits
        
        self.df = pd.DataFrame(enriched_data)
    
    def save_enriched_csv(self, output_file="enriched_output.csv"):
        """Save the enriched dataframe to a CSV file."""
        self.df.to_csv(output_file, index=False)

        print(f"Enriched CSV saved as {output_file}")

    def save_missing_dois(self, output_file="missing_dois.csv"):
        """Save the list of DOIs that could not be found."""
        if self.not_found_dois:
            df = pd.DataFrame({"missing_doi": self.not_found_dois})
            df.to_csv(output_file, index=False)
            print(f"Missing DOIs saved to {output_file}")
        else:
            print("No missing DOIs to save.")
        
    def run(self, output_file="./Data/Cochrane/cochrane_combined_output_.csv", key="doi", missing_dois_file="missing_dois.csv"):
        """Run the entire enrichment process."""
        self.enrich_data(key=key)
        self.save_enriched_csv(output_file=output_file)
        self.save_missing_dois(missing_dois_file)


# if __name__ == "__main__":
#     print("Starting enrichment for the combined file...")
#     enricher = DOIEnricher("./Data/Cochrane/cochrane_combined_output.csv")
#     enricher.run()
#     print("Done with enrich...")