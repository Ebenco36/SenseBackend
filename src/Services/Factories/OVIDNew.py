import csv
import os
import glob
import requests
import pandas as pd
from app import db, app
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from src.Services.Service import Service
from urllib.parse import urlparse, parse_qs, urlencode

class OvidJournalDataFetcher(Service):
    # "https://ovidsp.dc1.ovid.com/ovid-new-b/ovidweb.cgi?=&S=BCGGFPLEGDACKLOAKPIJIHMIOHBKAA00&Get Bib Display=Titles|G|S.sh.25|1|100&on_msp=1&CitManPrev=S.sh.25|1|100&undefined=Too many results to sort&cmRecords=Ex: 1-4, 7&cmRecords=Ex: 1-4, 7&results_per_page=100&results_per_page=100&startRecord=1&FORMAT=title&FIELDS=SELECTED&output mode=display&WebLinkReturn=Titles=S.sh.25|1|100&FORMAT=title&FIELDS=SELECTED&Datalist=S.sh.25|1|100&gsrd_params=S.sh.25|1|100|&analytics_display=msp&startRecord_subfooter=1&SELECT=S.sh.25|&issue_record_range=1-51215"
    def __init__(self, base_url="https://ovidsp.dc1.ovid.com/ovid-new-a/ovidweb.cgi", max_pages=1000000):
        self.session = requests.Session()
        self.base_url = base_url
        # Set the date-based directory for storing data
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        self.data_dir = f"Data/OVIDNew/data_{self.date_str}"
        os.makedirs(self.data_dir, exist_ok=True)
        self.max_pages = max_pages
        self.url = self.get_url_from_config("ovid_url")

    def authenticate(self, headers):
        self.auth_headers = headers
        return self
    

    def extract_params_from_url(self, url):
        """
        Extracts query parameters from a given URL and returns them as a dictionary,
        flattening single-item lists. Also returns the base URL without query parameters.

        :param url: The URL to extract parameters from.
        :return: A tuple containing:
                    - A dictionary of query parameters.
                    - The base URL (scheme + netloc + path) without query parameters.
        """
        # Parse the URL
        parsed_url = urlparse(url)
        
        # Extract query parameters as a dictionary
        params_dict = {
            key: value[0] if len(value) == 1 else value
            for key, value in parse_qs(parsed_url.query).items()
        }

        # Rebuild the base URL (scheme + netloc + path)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

        return params_dict, base_url


    def get_url_from_config(self, key_name):
        """
        Retrieves a URL from the `config` table based on the provided key name.
        
        :param key_name: The key to look up in the config table
        :return: The URL as a string, or None if not found
        """
        try:
            with app.app_context():
                result = db.session.execute(
                    "SELECT values FROM sense_config WHERE key = :key_name",
                    {"key_name": key_name}
                ).fetchone()
                
            return result[0] if result else None
        except Exception as e:
            print(f"Error retrieving URL from config: {e}")
            return None
        
    def rebuild_url(self, base_url, params_dict):
        """
        Rebuilds a URL from a base URL and updated query parameters.

        :param base_url: The base URL (scheme + netloc + path).
        :param params_dict: The updated query parameters dictionary.
        :return: The rebuilt URL with updated query parameters.
        """
        # Rebuild the query string
        query_string = "&".join(
            f"{key}={v}" for key, values in params_dict.items() for v in (values if isinstance(values, list) else [values])
        )

        # Construct the final URL
        rebuilt_url = f"{base_url}?{query_string}"
        return rebuilt_url

    def fetch(self):
        params, self.base_url = self.extract_params_from_url(self.url)
        page_number = 1
        results_per_page = int(params.get("results_per_page", [100])[0])  # Default to 100 if not provided

        while page_number <= self.max_pages:
            if page_number == 1:
                # Page 1 logic
                start_record = 1
                params["Get Bib Display"] = f"Titles|G|S.sh.30|{start_record}|{results_per_page}"
            else:
                # Page 2 and beyond logic
                start_record = (page_number - 1) * results_per_page + 1
                params["Get Bib Display"] = f"Titles|F|S.sh.30|{start_record}|{results_per_page}"

            # Update `startRecord` and `startRecord_subfooter`
            params["startRecord"] = str(start_record)
            params["startRecord_subfooter"] = str(start_record)

            # WebLinkReturn is constant
            params["WebLinkReturn"] = f"Titles=F|S.sh.30|1|{results_per_page}"
            
                  # Set filename to check if page data already exists
            filename = os.path.join(self.data_dir, f"journal_data_page_{page_number}.csv")
            if os.path.exists(filename):
                print(f"Data for page {page_number} already saved as {filename}. Skipping download.")
                page_number += 1
                continue
                
            # Construct the URL
            url = self.rebuild_url(self.base_url, params)
            print(f"Fetching page {page_number}: {url}")

            # Fetch and process the response
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract data
            journals = self._extract_data(soup)
            if not journals:
                print(f"No records found on page {page_number}. Stopping.")
                break

            # Save data to CSV
            filename = os.path.join(self.data_dir, f"journal_data_page_{page_number}.csv")
            self._save_to_csv(journals, filename)

            # Check for "Next" button
            next_button = soup.select_one('.n-mb .next-prev input.titles-nav-button[aria-label="Next"]')
            if not next_button:
                print("No more pages available.")
                break

            page_number += 1

        self.merge_csv_files()

        
        
    def _extract_data(self, soup):
        """
        Extracts data from the HTML for each journal record and splits the Source field.
        :param soup: BeautifulSoup object of the HTML
        :return: List of dictionaries containing cleaned data
        """
        def convert_to_date(value, format):
            """
            Helper function to convert a string to a date object.
            :param value: The string date to convert.
            :param format: The format of the date string.
            :return: Formatted date string in YYYY-MM-DD or original value if conversion fails.
            """
            try:
                return datetime.strptime(value, format).strftime("%Y-%m-%d")
            except ValueError:
                return value  # Return the original value if parsing fails
        
        records = []  # List to store all extracted records

        for record in soup.select(".titles-row"):
            data = {}

            # Title and Link
            title_tag = record.select_one(".titles-title h5 a")
            if title_tag:
                data["Title"] = title_tag.text.strip()
                data["TitleLink"] = title_tag.get("href", "")

            # Authors
            authors_tag = record.select_one(".article-authors")
            if authors_tag:
                data["Authors"] = " ".join(authors_tag.text.split())  # Normalize whitespace

            # Source Information (Split into Database, Journal name, and Other info)
            source_tag = record.select_one(".titles-dbsegment")  # Database
            source_details = record.select_one(".titles-source")  # Journal name and other info

            if source_tag or source_details:
                # Extract Database (e.g., "Embase")
                database = source_tag.text.strip() if source_tag else None

                # Extract Journal name and other info (split intelligently)
                if source_details:
                    source_parts = source_details.text.strip().split(".", 1)  # Split into journal name and other
                    journal_name = source_parts[0].strip() if len(source_parts) > 0 else None
                    other_info = source_parts[1].strip() if len(source_parts) > 1 else None
                else:
                    journal_name = None
                    other_info = None

                # Add extracted fields to the data dictionary
                data["Database"] = database
                data["Journal"] = journal_name
                data["OtherInfo"] = other_info

            # Publication Type
            pub_type_tag = record.select_one(".article-pubtype")
            if pub_type_tag:
                data["PublicationType"] = pub_type_tag.text.strip().strip("[]")

            # Abstract
            abstract_tag = record.select_one(".titles-ab")
            if abstract_tag:
                data["Abstract"] = " ".join(abstract_tag.text.split())  # Normalize whitespace

            # DOI
            doi_tag = record.select_one('.titles-other:has(span:contains("DOI")) a')
            if doi_tag:
                data["DOI"] = doi_tag.get("href", "")

            # Correspondence Email
            correspondence_email_tag = record.select_one('.titles-other:has(span:contains("Correspondence Address")) a')
            if correspondence_email_tag:
                data["CorrespondenceEmail"] = correspondence_email_tag.get("data-cfemail", "")

            # Correspondence Address
            correspondence_address_tag = record.select_one('.titles-other:has(span:contains("Correspondence Address"))')
            if correspondence_address_tag:
                address_text = correspondence_address_tag.text.replace("Correspondence Address", "").strip()
                data["CorrespondenceAddress"] = " ".join(address_text.split())

            # Institution
            institution_tag = record.select_one('.titles-other:has(span:contains("Institution"))')
            if institution_tag:
                institution_text = institution_tag.text.replace("Institution", "").strip()
                data["Institution"] = " ".join(institution_text.split())

            # Country of Publication
            country_tag = record.select_one('.titles-other:has(span:contains("Country of Publication"))')
            if country_tag:
                country_text = country_tag.text.replace("Country of Publication", "").strip()
                data["Country"] = country_text

            # Publisher
            publisher_tag = record.select_one('.titles-other:has(span:contains("Publisher"))')
            if publisher_tag:
                publisher_text = publisher_tag.text.replace("Publisher", "").strip()
                data["Publisher"] = publisher_text

            # Journal Abbreviation
            journal_abbrev_tag = record.select_one('.titles-other:has(span:contains("Journal Abbreviation"))')
            if journal_abbrev_tag:
                abbrev_text = journal_abbrev_tag.text.replace("Journal Abbreviation", "").strip()
                data["JournalAbbreviation"] = abbrev_text

            # URL
            url_tag = record.select_one('.titles-other:has(span:contains("URL")) a')
            if url_tag:
                data["URL"] = url_tag.get("href", "")

            # Emtree Headings
            emtree_tag = record.select_one('.titles-other:has(span:contains("Emtree Heading"))')
            if emtree_tag:
                emtree_text = emtree_tag.text.replace("Emtree Heading", "").strip()
                data["EmtreeHeadings"] = emtree_text

            # Number of References
            references_tag = record.select_one('.titles-other:has(span:contains("Number of References"))')
            if references_tag:
                references_text = references_tag.text.replace("Number of References", "").strip()
                data["NumberOfReferences"] = references_text

            # Language
            language_tag = record.select_one('.titles-other:has(span:contains("Language")):not(:has(span:contains("Summary Language")))')
            if language_tag:
                language_text = language_tag.text.replace("Language", "").strip()
                data["Language"] = language_text

            
            # Update Date (e.g., '202448' -> '2024-12-02')
            update_date_tag = soup.select_one('.titles-other:has(span:contains("Update Date"))')
            if update_date_tag:
                raw_update_date = update_date_tag.text.replace("Update Date", "").strip()
                data["UpdateDate"] = convert_to_date(raw_update_date, "%Y%U")  # Week-based date format

            # Date Delivered (e.g., '20241122' -> '2024-11-22')
            delivered_date_tag = soup.select_one('.titles-other:has(span:contains("Date Delivered"))')
            if delivered_date_tag:
                raw_delivered_date = delivered_date_tag.text.replace("Date Delivered", "").strip()
                data["DateDelivered"] = convert_to_date(raw_delivered_date, "%Y%m%d")

            # Date Created (e.g., '20241122' -> '2024-11-22')
            created_date_tag = soup.select_one('.titles-other:has(span:contains("Date Created"))')
            if created_date_tag:
                raw_created_date = created_date_tag.text.replace("Date Created", "").strip()
                data["DateCreated"] = convert_to_date(raw_created_date, "%Y%m%d")
                
            # Year of Publication
            year_tag = record.select_one('.titles-other:has(span:contains("Year of Publication"))')
            if year_tag:
                year_text = year_tag.text.replace("Year of Publication", "").strip()
                data["Year"] = year_text

            # Append to the list of records
            records.append(data)

        return records


    def _save_to_csv(self, journals, filename):
        headers = [
            "Title", "TitleLink", "Authors", "Database", 
            "Journal", "OtherInfo",  "PublicationType", "Abstract", "URL", "DOI",
            "CorrespondenceEmail", "CorrespondenceAddress", "Institution", "Country",
            "Publisher", "JournalAbbreviation", "EmtreeHeadings", "NumberOfReferences", 
            "Language", "DateCreated", "UpdateDate", "DateDelivered", "Year"
        ]

        # Write data to CSV
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(journals)

        print(f"Saved data to {filename}")

    def merge_csv_files(self):
        # Define the output file for the merged CSV with date
        merged_filename = os.path.join("Data/OVIDNew", f"merged_journal_data_{self.date_str}.csv")

        # Collect all individual CSV files in the date directory
        all_files = glob.glob(os.path.join(self.data_dir, "*.csv"))

        # Combine all CSV files into one DataFrame
        df_list = [pd.read_csv(file) for file in all_files]
        merged_df = pd.concat(df_list, ignore_index=True)

        # Save the merged DataFrame to a single CSV file
        merged_df.to_csv(merged_filename, index=False, encoding="utf-8")
        print(f"All CSV files merged into {merged_filename}")
