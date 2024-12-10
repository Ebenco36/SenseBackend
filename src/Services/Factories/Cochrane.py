import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from src.Services.Service import Service

class Cochrane(Service):
    def __init__(self, pageSize=50):
        self.session = requests.Session()  # Initialize a Session
        self.pageSize = pageSize

        # Define headers
        self.session.headers.update({
            "Host": "www.cochranelibrary.com",
            "Content-Type": "application/x-www-form-urlencoded",
            "accept-language": "en-GB-oxendict,en-US;q=0.9,en;q=0.8,yo;q=0.7",
            "referer": "https://www.cochranelibrary.com/",
            "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        })

    def authenticate(self, headers):
        self.session.headers.update(headers)
        return self

    def fetch(self, searchText=""):
        return self.authenticate(self.session.headers).retrieveRecord(search_text=searchText)

    def retrieveRecord(self, search_text=""):
        self.check_advanced_query()
        output_csv = "Cochrane/data"
        self.fetch_cochrane_data_and_save(search_text, output_csv)

    def check_advanced_query(self):
        url = "https://www.cochranelibrary.com/en/advanced-search?p_p_id=scolarissearchresultsportlet_WAR_scolarissearchresults&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=validate-advanced-search&p_p_cacheability=cacheLevelPage&p_p_col_id=column-1&p_p_col_count=2"
        form_data = {
            "advancedSearchForm": '{"searchType":"advanced","searchText":"COvid"}'
        }
        response = self.session.post(url, data=form_data)

        if response.status_code == 200:
            try:
                validation_result = response.json()
                print("Validation result:", validation_result)
            except ValueError:
                print("Received a non-JSON response. Validation might still be successful.")
                print(response.text)
        else:
            print(f"Failed to validate query. Status code: {response.status_code}")
            print("Response:", response.text)

    def get_last_saved_page(self, output_dir):
        """Returns the last saved page number based on existing files in the output directory."""
        files = [f for f in os.listdir(output_dir) if f.startswith("cochrane_page_") and f.endswith(".csv")]
        if files:
            last_page = max(int(f.split("_")[-1].split(".")[0]) for f in files)
            return last_page
        return 0
    
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
        
        
    def fetch_cochrane_data_and_save(self, search_text, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        page_number = self.get_last_saved_page(output_dir) + 1
        
        while True:
            url = (
                "https://www.cochranelibrary.com/en/c/portal/render_portlet"
                "?p_l_id=20761&p_p_id=scolarissearchresultsportlet_WAR_scolarissearchresults"
                "&p_p_lifecycle=0&p_t_lifecycle=0&p_p_state=normal&p_p_mode=view"
                "&p_p_col_id=column-1&p_p_col_pos=1&p_p_col_count=2&p_p_isolated=1"
                "&currentURL=%2Fadvanced-search&searchType=advanced&database=&status="
                "&publicationYear=allyears&publicationYear=between&startPublicationYear="
                "&endPublicationYear=&publicationDate=alldates&startPublicationDateYear="
                "&startPublicationDateMonth=1&endPublicationDateYear=&endPublicationDateMonth=1"
                f"&wordVariation=true&crgs=&controlOptions=AND&searchOptions=1&resultPerPage={self.pageSize}"
                f"&searchText={search_text}&pathname=%2Fadvanced-search&cur={page_number}"
            )

            try:
                response = self.session.get(url)
                response.raise_for_status()

                response_text = response.text
                data_found, page_numbers, max_page_num = self.extract_csv_from_html(response_text)
                # self.combine_csv_files(output_dir, "Cochrane/cochrane_combined_output.csv")
                if page_number > max_page_num:
                    print("No more pages.")
                    break

                output_file = os.path.join(output_dir, f"cochrane_page_{page_number}.csv")
                data_found.to_csv(output_file, index=False)
                print(f"Page {page_number} saved to {output_file}")
                self.combine_csv_files(output_dir, "Cochrane/cochrane_combined_output.csv")
            except requests.RequestException as req_err:
                print(f"Request failed with error: {req_err}")
                break

            page_number += 1

    def extract_csv_from_html(self, html_content):
        soup = BeautifulSoup(html_content, "html.parser")
        records = []

        for item in soup.select(".search-results-item"):
            cd_identifier = item.find("input", {"type": "checkbox"}).get("value", None)
            title = item.find("h3", class_="result-title").get_text(strip=True) if item.find("h3", class_="result-title") else None
            doi_link = item.find("a", href=True).get("href") if item.find("a", href=True) else None
            modified_date = item.find("div", class_="search-result-date").get_text(strip=True) if item.find("div", class_="search-result-date") else None
            result_type = item.find("div", class_="search-result-type").get_text(strip=True) if item.find("div", class_="search-result-type") else None
            result_stage = item.find("div", class_="search-result-stage").get_text(strip=True) if item.find("div", class_="search-result-stage") else None
            author_list = item.find("div", class_="search-result-authors").get_text(strip=True) if item.find("div", class_="search-result-authors") else None
            abstract = item.find("div", class_="search-result-preview").get_text(strip=True) if item.find("div", class_="search-result-preview") else None
            
            doi_pattern = r'/doi/([0-9.]+/[^/]+)/full'  # Adjusted regex to be more flexible
            doi_match = re.search(doi_pattern, doi_link)
            doi = ""
            if doi_match:
                doi = doi_match.group(1)
            
            get_other_fields = {}
            if doi:
                get_other_fields = self.fetch_from_unpaywall(doi)
                
            # Replace "/full" at the end with "/pdf/full"
            pdf_url = doi_link.replace("/full", "/pdf/full")
            
            # Fetch additional PICO data
            pico_data = self.fetch_pico_data(doi_link) if doi_link else {}
            
            records.append({
                "cdIdentifier": cd_identifier,
                "title": title,
                "doi_link": doi_link,
                "doi": doi,
                "pdf_url": pdf_url,
                "modifiedDate": modified_date,
                "resultType": result_type,
                "resultStage": result_stage,
                "authors": author_list,
                "abstract": abstract,
                "year": get_other_fields.get("year", ""),
                "journal": get_other_fields.get("journal_name", ""),
                "open_access": "Open Access" if get_other_fields.get("is_oa", False) == True else "Not Open Access",
                **pico_data  # Merge PICO data fields
            })

        df = pd.DataFrame(records)
        page_numbers = [int(item.get_text(strip=True)) for item in soup.find_all("li", class_="pagination-page-list-item")]
        max_page_number = max(page_numbers) if page_numbers else 1

        return df, page_numbers, max_page_number

    
    def extract_doi_from_url(self, url):
        """Extract DOI from a given URL with optional .pub suffix."""
        pattern = r"/doi/(10\.\d{4,9}/[^/]+?)(?:\.pub\d+)?/full"  # Match DOI with optional .pub suffix
        match = re.search(pattern, url)
        
        if match:
            return match.group(1)  # Return the captured DOI
        else:
            return None  # Return None if no DOI is found
    
    
    def fetch_pico_data(self, doi_link):
        """Fetch PICO data for a given DOI using the session."""
        try:
            doi = self.extract_doi_from_url(doi_link)
            url = f"https://www.cochranelibrary.com/content?p_p_id=scolariscontentdisplay_WAR_scolariscontentdisplay&p_p_lifecycle=2&p_p_state=exclusive&p_p_mode=view&p_p_resource_id=get-pico-data&doi={doi}"

            # Use session to make request
            response = self.session.get(url)
            response.raise_for_status()  # Check if request was successful

            # Parse JSON response for PICO data fields
            pico_data = response.json()
            patient_population = ', '.join(pico_data.get("Population", {}).keys())
            intervention = ', '.join(pico_data.get("Intervention", {}).keys())
            comparator = ', '.join(pico_data.get("Comparison", {}).keys())
            outcomes = ', '.join(pico_data.get("Outcome", {}).keys())

            return {
                "patient_population": patient_population,
                "intervention": intervention,
                "comparator": comparator,
                "outcomes": outcomes
            }
        except (requests.exceptions.RequestException, KeyError) as e:
            print(f"Error fetching PICO data for DOI {doi_link}: {e}")
            return {}
        

    def combine_csv_files(self, input_directory, output_filename="cochrane_combined_output.csv"):
        # List to store each CSV file's DataFrame
        data_frames = []
        
        # Iterate over all files in the input directory
        for filename in os.listdir(input_directory):
            # Check if the file is a CSV
            if filename.endswith(".csv"):
                file_path = os.path.join(input_directory, filename)
                # Read the CSV file and append the DataFrame to the list
                df = pd.read_csv(file_path)
                data_frames.append(df)
        
        # Concatenate all DataFrames into a single DataFrame
        combined_df = pd.concat(data_frames, ignore_index=True)
        
        # Save the combined DataFrame to a CSV file
        combined_df.to_csv(output_filename, index=False)
        print(f"Combined CSV file saved as '{output_filename}'")