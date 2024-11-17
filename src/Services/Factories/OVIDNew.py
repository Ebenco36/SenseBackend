import requests
from bs4 import BeautifulSoup
import csv
import os
import glob
import pandas as pd
from datetime import datetime
from urllib.parse import urlencode
from src.Services.Service import Service

class OvidJournalDataFetcher(Service):
    def __init__(self, base_url="https://ovidsp.dc1.ovid.com/ovid-new-a/ovidweb.cgi", max_pages=100):
        self.session = requests.Session()
        self.base_url = base_url
        # Set the date-based directory for storing data
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        self.data_dir = f"OVIDNew/data_{self.date_str}"
        os.makedirs(self.data_dir, exist_ok=True)
        self.max_pages = max_pages  # Limit to avoid infinite navigation

    def authenticate(self, headers):
        self.auth_headers = headers
        return self

    def fetch(self):
        # Initial URL parameters
        params = {
            "S": "FDLGFPNHNBACLMCJKPIJGHPMHEIJAA00",
            "Get Bib Display": "Titles|B|S.sh.62|1|5",
            "on_msp": "1",
            "CitManPrev": "S.sh.62|1|5",
            "sort_by": "_default",
            "cmRecords": "Ex: 1-4, 7",
            "results_per_page": "5",
            "startRecord": "1",
            "FORMAT": "title",
            "FIELDS": "SELECTED",
            "output mode": "display",
            "WebLinkReturn": "Titles=B|S.sh.62|6|5",
            "Datalist": "S.sh.62|1|5",
            "gsrd_params": "S.sh.62|1|5|",
            "analytics_display": "msp",
            "startRecord_subfooter": "1",
            "SELECT": "S.sh.62|",
            "issue_record_range": "1-21"  # Extract total records from here
        }

        # Extract total_records from issue_record_range parameter
        total_records = int(params["issue_record_range"].split("-")[-1])
        page_number = 1
        fetched_records = 0   # Counter to keep track of fetched records

        while page_number <= self.max_pages:
            # Update startRecord and other pagination parameters
            start_record = (page_number - 1) * int(params["results_per_page"]) + 1
            params["startRecord"] = str(start_record)
            params["startRecord_subfooter"] = str(start_record)
            params["Get Bib Display"] = f"Titles|{'F' if page_number % 2 == 0 else 'B'}|S.sh.62|{start_record}|{params['results_per_page']}"
            params["WebLinkReturn"] = f"Titles={'F' if page_number % 2 == 0 else 'B'}|S.sh.62|{start_record + int(params['results_per_page'])}|{params['results_per_page']}"

            # Set filename to check if page data already exists
            filename = os.path.join(self.data_dir, f"journal_data_page_{page_number}.csv")
            if os.path.exists(filename):
                print(f"Data for page {page_number} already saved as {filename}. Skipping download.")
                page_number += 1
                continue

            # Construct the dynamic URL for the current page
            query_string = urlencode(params)
            url = f"{self.base_url}?{query_string}"
            print(f"Fetching page {page_number}: {url}")

            # Request and parse the page
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract data and save to CSV
            journals = self._extract_data(soup)
            self._save_to_csv(journals, filename)

            # Check if there is a "Next" button to navigate further
            next_button = soup.select_one('.n-mb .next-prev input.titles-nav-button[aria-label="Next"]')
            if next_button:
                page_number += 1
            else:
                print("No more pages available.")
                break

        # Merge all CSV files into a single file
        self.merge_csv_files()

    def _extract_data(self, soup):
        journals = []
        for record in soup.select(".titles-row"):
            title = record.select_one(".citation_title").get_text(strip=True) if record.select_one(".citation_title") else "N/A"
            authors = record.select_one(".article-authors").get_text(strip=True) if record.select_one(".article-authors") else "N/A"
            journal = record.select_one(".titles-source").get_text(strip=True) if record.select_one(".titles-source") else "N/A"
            
            # Fetch the publisher from the titles-dbsegment class
            publisher = record.select_one(".titles-dbsegment").get_text(strip=True) if record.select_one(".titles-dbsegment") else "N/A"
            
            abstract = record.select_one(".titles-ab").get_text(strip=True) if record.select_one(".titles-ab") else "N/A"
            url = record.select_one(".bibrecord-extlink")["href"] if record.select_one(".bibrecord-extlink") else "N/A"
            doi = record.select_one('.bibrecord-extlink[title^="https://dx.doi.org"]')["title"] if record.select_one('.bibrecord-extlink[title^="https://dx.doi.org"]') else "N/A"

            journals.append({
                "Title": title,
                "Authors": authors,
                "Journal": journal,
                "Publisher": publisher,
                "Abstract": abstract,
                "URL": url,
                "DOI": doi
            })
        
        return journals

    def _save_to_csv(self, journals, filename):
        headers = ["Title", "Authors", "Journal", "Publisher", "Abstract", "URL", "DOI"]

        # Write data to CSV
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(journals)

        print(f"Saved data to {filename}")

    def merge_csv_files(self):
        # Define the output file for the merged CSV with date
        merged_filename = os.path.join("OVIDNew", f"merged_journal_data_{self.date_str}.csv")

        # Collect all individual CSV files in the date directory
        all_files = glob.glob(os.path.join(self.data_dir, "*.csv"))

        # Combine all CSV files into one DataFrame
        df_list = [pd.read_csv(file) for file in all_files]
        merged_df = pd.concat(df_list, ignore_index=True)

        # Save the merged DataFrame to a single CSV file
        merged_df.to_csv(merged_filename, index=False, encoding="utf-8")
        print(f"All CSV files merged into {merged_filename}")
