import re
import os
import csv
import requests
import pandas as pd
from bs4 import BeautifulSoup
from src.Commands.services import fetch_first_scholar_result
from src.Services.Service import Service
from src.Request.ApiRequest import ApiRequest
from src.Utils.Helpers import save_json_to_csv
from src.Utils.Helpers import create_directory_if_not_exists, save_json_to_csv, append_json_response_to_file, \
    json_to_dataframe_and_save, get_remainder_and_quotient, convert_json_to_List_of_dict


class Cochrane(Service):
    def __init__(self, pageSize = 50):
        # Define headers
        self.headers = {
            "Host": "www.cochranelibrary.com",
            "Content-Type": "application/x-www-form-urlencoded",
            "accept-language": "en-GB-oxendict,en-US;q=0.9,en;q=0.8,yo;q=0.7",
            "cookie": "GUEST_LANGUAGE_ID=en_US; COOKIE_SUPPORT=true; osano_consentmanager_uuid=ca4d3e91-b57b-4a3d-a016-fde74cd79d96; osano_consentmanager=Nzjf7YZ3KmhYJBGYIahMg5_gokVuae4KhVL-zxFdyAVDtN5z_zqwvLrrsO3w9q00Foyef1VQAUZIGg6tSRF3QMSqPwPCvOnQWoJVXGhc0o_5b12XBqw7aQZLvdmEZYWkM_B5JNyb1ijdX9c12K-E2CscO2useUE1hNFxKSISMz1bGWGew2XIhyn0Dhkgv9tyy4pxXooA7lH5lTxE0YUsziIKDYrA70R8kYCo91Yxw0ZC33Ob0x5CZfGPR1MHNj2Y_C_w1f7S4xq2nBC3fh5j5P9qWpQ=; LOGIN=61776f746f726f6540726b692e6465; SCREEN_NAME=6e496f7855714d50757255616f66666a6850693643773d3d; USER_UUID=6c792f63342b684d70784837652b784b564937554767586144384261587474514348664a4b50417a5038453d; JSESSIONID=cspbwgreclprt160y1~184C6774B199E15D95729A10D23A3F7E; SID_REP=A1F4C5C29BD3D2A8662C3F04B4B4A072; SCOLAUTHSESSIONID=2319E7ECF63D92AF88FE66AC2B03A5D2; __cf_bm=3RNZVknCUghPMU_9FbzhrCqUq0d15SUsrCkp6KupZUQ-1729981431-1.0.1.1-J_bvVhF1EWRt3KuELVnax9SXcyvbBrqQ_wQ5Cnt_u8R9tW2GjNR.QjOUS4aZ5pSZgDlITJleYp566ho39w0yPQ; LFR_SESSION_STATE_20159=1729981565325; cf_clearance=UH6VVuiZ9nU8rV9KJ55kONJCmCIp70X02ETB8aKtC.w-1729981565-1.2.1.1-UphBOiOLQXQVcksUkZ05VqoiMbmPcSG4yVPgibdxeUm0oljB_iqvJ10_qvFqMovE2s6tlWdaM3yoH14NjHMZM3XBR1vUFYWwfUOtNGxcl6k4yUp6uRQilkWmhfHq50600aqC3kvuc1X0gPvQWNwtX2ODUCjklRNckuAOKx8JVTUk5CUon0DCitfOmRYkg0cEEGL6SocLLTW8JBMkRiMuvTUHkKvR67S0_an_WnwT4fAoQUqp07rMwziYqTtkJcyaWq5bFhynYaauFusb30.wQd3uMztA8Y89jvjZklzR_GH2_EdQcnPuADK3hauc_k1sqzX.GGYIFQmr.GC93yZl_hid6uY_G.DN3lJY..8WsvMhdKvy.KBoHZAuuvbiIPZO2Fs8zLB0NllVdnajtp2_JA; SCOL_SESSION_TIMEOUT=1740 Sat, 26 Oct 2024 22:57:39 GMT; _ga_BRLBHKT9XF=GS1.1.1729981720.1.0.1729981720.0.0.0",
            "referer": "https://www.cochranelibrary.com/",
            "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }

        self.pageSize = pageSize


    def authenticate(self, headers):
        self.auth_headers = headers
        return self

    def fetch(self, searchText=""):
        return self.authenticate(
            self.headers
        ).retrieveRecord(
            search_text=searchText
        )
    
    def retrieveRecord(self, search_text=""):
        self.check_advanced_query()
        output_csv = "Cochrane/data"
        self.fetch_cochrane_data_and_save(search_text, output_csv)
        


    def check_advanced_query(self):
        # URL for validating the advanced search
        url = "https://www.cochranelibrary.com/en/advanced-search?p_p_id=scolarissearchresultsportlet_WAR_scolarissearchresults&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=validate-advanced-search&p_p_cacheability=cacheLevelPage&p_p_col_id=column-1&p_p_col_count=2"

        headers = {
            "Host": "www.cochranelibrary.com",
            "Content-Type": "application/x-www-form-urlencoded",
            "accept-language": "en-GB-oxendict,en-US;q=0.9,en;q=0.8,yo;q=0.7",
            "cookie": "GUEST_LANGUAGE_ID=en_US; COOKIE_SUPPORT=true; osano_consentmanager_uuid=ca4d3e91-b57b-4a3d-a016-fde74cd79d96; osano_consentmanager=Nzjf7YZ3KmhYJBGYIahMg5_gokVuae4KhVL-zxFdyAVDtN5z_zqwvLrrsO3w9q00Foyef1VQAUZIGg6tSRF3QMSqPwPCvOnQWoJVXGhc0o_5b12XBqw7aQZLvdmEZYWkM_B5JNyb1ijdX9c12K-E2CscO2useUE1hNFxKSISMz1bGWGew2XIhyn0Dhkgv9tyy4pxXooA7lH5lTxE0YUsziIKDYrA70R8kYCo91Yxw0ZC33Ob0x5CZfGPR1MHNj2Y_C_w1f7S4xq2nBC3fh5j5P9qWpQ=; LOGIN=61776f746f726f6540726b692e6465; SCREEN_NAME=6e496f7855714d50757255616f66666a6850693643773d3d; USER_UUID=6c792f63342b684d70784837652b784b564937554767586144384261587474514348664a4b50417a5038453d; JSESSIONID=cspbwgreclprt160y1~184C6774B199E15D95729A10D23A3F7E; SID_REP=A1F4C5C29BD3D2A8662C3F04B4B4A072; SCOLAUTHSESSIONID=2319E7ECF63D92AF88FE66AC2B03A5D2; __cf_bm=3RNZVknCUghPMU_9FbzhrCqUq0d15SUsrCkp6KupZUQ-1729981431-1.0.1.1-J_bvVhF1EWRt3KuELVnax9SXcyvbBrqQ_wQ5Cnt_u8R9tW2GjNR.QjOUS4aZ5pSZgDlITJleYp566ho39w0yPQ; LFR_SESSION_STATE_20159=1729981565325; cf_clearance=UH6VVuiZ9nU8rV9KJ55kONJCmCIp70X02ETB8aKtC.w-1729981565-1.2.1.1-UphBOiOLQXQVcksUkZ05VqoiMbmPcSG4yVPgibdxeUm0oljB_iqvJ10_qvFqMovE2s6tlWdaM3yoH14NjHMZM3XBR1vUFYWwfUOtNGxcl6k4yUp6uRQilkWmhfHq50600aqC3kvuc1X0gPvQWNwtX2ODUCjklRNckuAOKx8JVTUk5CUon0DCitfOmRYkg0cEEGL6SocLLTW8JBMkRiMuvTUHkKvR67S0_an_WnwT4fAoQUqp07rMwziYqTtkJcyaWq5bFhynYaauFusb30.wQd3uMztA8Y89jvjZklzR_GH2_EdQcnPuADK3hauc_k1sqzX.GGYIFQmr.GC93yZl_hid6uY_G.DN3lJY..8WsvMhdKvy.KBoHZAuuvbiIPZO2Fs8zLB0NllVdnajtp2_JA; SCOL_SESSION_TIMEOUT=1740 Sat, 26 Oct 2024 22:57:39 GMT; _ga_BRLBHKT9XF=GS1.1.1729981720.1.0.1729981720.0.0.0",
            "referer": "https://www.cochranelibrary.com/",
            "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }
        # Define the form data
        form_data = {
            "advancedSearchForm": '{"searchType":"advanced","database":"","status":"","publicationYear":["allyears","between"],"startPublicationYear":["",""],"endPublicationYear":["",""],"publicationDate":"alldates","startPublicationDateYear":["",""],"startPublicationDateMonth":["1","1"],"endPublicationDateYear":["",""],"endPublicationDateMonth":["1","1"],"wordVariation":"true","crgs":"","controlOptions":"AND","searchOptions":"1","searchText":"COvid"}'
        }

        # Send the POST request
        response = requests.post(url, headers=headers, data=form_data)

        # Check the response
        if response.status_code == 200:
            # Assuming that a successful validation returns JSON or a particular text in HTML
            try:
                validation_result = response.json()
                print("Validation result:", validation_result)
            except ValueError:
                print("Received a non-JSON response. Validation might still be successful.")
                print(response.text)  # Print raw HTML response if it's not JSON
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

    def fetch_cochrane_data_and_save_issues(self, search_text, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        page_number = self.get_last_saved_page(output_dir) + 1  # Start from the next page after the last saved one
        while True:
            # Define the URL for the current page
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
            print(url)
            headers = {
                "Host": "www.cochranelibrary.com",
                'accept-language': 'en-GB-oxendict,en-US;q=0.9,en;q=0.8,yo;q=0.7',
                'cookie': 'GUEST_LANGUAGE_ID=en_US; COOKIE_SUPPORT=true; osano_consentmanager_uuid=ca4d3e91-b57b-4a3d-a016-fde74cd79d96; ...',  # Truncated for brevity
                'referer': 'https://www.cochranelibrary.com/',
                'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest'
            }
            # Make the GET request
            response = requests.get(url, headers=headers)

            # Check the response status
            if response.status_code == 200:
                try:
                    response_text = response.text
                    # Extract data (assuming these functions are defined in your class)
                    cd_codes, doi_links = self.extract_cd_codes(response_text)
                    data_found = self.fetch_data_from_cochrane(cd_codes)
                    extracted_dois = self.extract_dois(doi_links)
                    access_info = self.fetch_access_icons_for_dois(extracted_dois)
                    # Merge the data and save it as a CSV for this page
                    dataframe = pd.merge(right=data_found, left=access_info, on="doi", how="outer")
                    
                    print(len(data_found))
                    
                    if dataframe.empty:
                        print("No more records found.")
                        break  # Exit the loop if there are no records

                    # Save this page’s data as a CSV file
                    output_file = os.path.join(output_dir, f"cochrane_page_{page_number}.csv")
                    dataframe.to_csv(output_file, index=False)
                    print(f"Page {page_number} saved to {output_file}")

                except Exception as e:
                    print(f"An error occurred while processing the data: {e}")
                    break  # Exit if there’s an error

            else:
                print(f"Error: {response.status_code}, {response.text}")
                break  # Stop if the request fails

            page_number += 1  # Go to the next page
            
    def fetch_cochrane_data_and_save(self, search_text, output_dir):
        # Ensure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Start from the next page after the last saved page
        page_number = self.get_last_saved_page(output_dir) + 1
        
        while True:
            # Define the URL for the current page
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

            headers = {
                "Host": "www.cochranelibrary.com",
                'accept-language': 'en-GB-oxendict,en-US;q=0.9,en;q=0.8,yo;q=0.7',
                'cookie': 'GUEST_LANGUAGE_ID=en_US; COOKIE_SUPPORT=true; osano_consentmanager_uuid=ca4d3e91-b57b-4a3d-a016-fde74cd79d96; ...',  # Truncated for brevity
                'referer': 'https://www.cochranelibrary.com/',
                'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest'
            }

            # Make the GET request
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise an error for bad status codes

            except requests.RequestException as req_err:
                print(f"Request failed with error: {req_err}")
                break  # Exit if the request fails

            # Process the response if status code is 200
            response_text = response.text
            max_page_num = 1
            try:
                # Extract data (assuming these functions are defined in your class)
                cd_codes, doi_links = self.extract_cd_codes(response_text)
                # data_found = self.fetch_data_from_cochrane(cd_codes)
                data_found, pages, max_page_num = self.extract_csv_from_html(response_text)
                
                if page_number > max_page_num:
                    print(f"We are done!")
                    break
                
                extracted_dois = self.extract_dois(doi_links)
                access_info = self.fetch_access_icons_for_dois(extracted_dois)
                
                self.extract_csv_from_html(response_text)

                # Merge the data
                dataframe = pd.merge(right=data_found, left=access_info, on="doi", how="outer")

                if dataframe.empty:
                    print("No more records found.")
                    break

                # Save this page’s data as a CSV file
                output_file = os.path.join(output_dir, f"cochrane_page_{page_number}.csv")
                dataframe.to_csv(output_file, index=False)
                print(f"Page {page_number} saved to {output_file}")
                
            except KeyError as key_err:
                print(f"Data extraction failed due to missing key: {key_err}")
                break  # Exit if key errors arise in data extraction

            except Exception as e:
                print(f"An error occurred while processing the data: {e}")
                import traceback
                traceback.print_exc()
                break  # Exit if any other error occurs

            # Increment to the next page
            page_number += 1
        
        self.combine_csv_files(output_dir, "Cochrane/cochrane_combined_output.csv")
    
    def extract_dois(self, urls):
        """Extract DOIs from a list of URLs."""
        dois = []
        for url in urls:
            # Check if the URL contains 'doi/'
            if 'doi/' in url:
                # Extract the DOI part by finding the right index
                start = url.find('doi/') + 4  # Move past 'doi/'
                end = url.find('/full', start)  # Find the next '/full' after the DOI starts
                if end == -1:  # If no '/full' is found, take the rest of the string
                    end = len(url)
                doi = url[start:end]  # Extract DOI
                dois.append(doi)
        return dois

    
    def extract_csv_from_html(self, html_content):
        """
        Extracts specified fields from HTML content and returns a DataFrame.
        
        Args:
            html_content (str): The HTML content to parse.

        Returns:
            pd.DataFrame: DataFrame containing extracted fields.
        """
        # Parse the HTML content
        soup = BeautifulSoup(html_content, "html.parser")

        # Initialize list to store records
        records = []
        page_numbers = []
        max_page_number = 0
        
        for item in soup.select(".search-results-item"):
            cd_identifier = item.find("input", {"type": "checkbox"}).get("value", None)
            title = item.find("h3", class_="result-title").get_text(strip=True) if item.find("h3", class_="result-title") else None
            doi_link = item.find("a", href=True).get("href") if item.find("a", href=True) else None
            modified_date = item.find("div", class_="search-result-date").get_text(strip=True) if item.find("div", class_="search-result-date") else None

            # Extract metadata fields
            result_type = item.find("div", class_="search-result-type").get_text(strip=True) if item.find("div", class_="search-result-type") else None
            result_stage = item.find("div", class_="search-result-stage").get_text(strip=True) if item.find("div", class_="search-result-stage") else None

            # Extract authors
            authors = item.find("div", class_="search-result-authors")
            author_list = authors.get_text(strip=True) if authors else None
            
            
            doi_pattern = r'/doi/([0-9.]+/[^/]+)/full'  # Adjusted regex to be more flexible
            doi_match = re.search(doi_pattern, doi_link)
            doi = ""
            if doi_match:
                doi = doi_match.group(1)
                
            # Replace "/full" at the end with "/pdf/full"
            pdf_url = doi_link.replace("/full", "/pdf/full")
    
            # Get all available page numbers as integers
            page_numbers = [int(item.get_text(strip=True)) for item in soup.find_all("li", class_="pagination-page-list-item")]

            # Calculate the maximum page number
            max_page_number = max(page_numbers) if page_numbers else 1


            # Append the extracted data as a dictionary
            records.append({
                "cdIdentifier": cd_identifier,
                "title": title,
                "doi_link": doi_link,
                "doi": doi,
                "pdf_url": pdf_url,
                "modifiedDate": modified_date,
                "resultType": result_type,
                "resultStage": result_stage,
                "authors": author_list
            })

        # Convert the list of records to a DataFrame
        df = pd.DataFrame(records)
        
        return df, page_numbers, max_page_number, 



    def extract_cd_codes(self, html_content):
        # Parse the HTML content
        soup = BeautifulSoup(html_content, "html.parser")

        # Initialize an empty list to store the CD codes
        cd_codes = []
        doi_links = []
        # Find all search result items
        for item in soup.select(".search-results-item"):
            # Extract the CDXXXX code
            cd_code_tag = item.select_one("input[name^='exportCD']")
            cd_code = cd_code_tag["value"] if cd_code_tag else "N/A"
            
            title_tag = item.select_one(".result-title a")
            doi_link = title_tag["href"] if title_tag else ""

            # Remove .PUBX from the code
            cd_code = re.sub(r'\.PUB\d$', '', cd_code)
            
            # Append the cleaned CD code to the list
            cd_codes.append(cd_code)
            doi_links.append(doi_link)

        return cd_codes, doi_links

    def json_data_to_dataframe(self, json_data, fields):
        try:
            # Create a list to hold rows of data
            rows = []

            # Extract rows based on specified fields
            for item in json_data.get('results', []):
                row = [item.get(field, "") for field in fields]
                rows.append(row)

            # Create DataFrame from the rows
            df = pd.DataFrame(rows, columns=fields)
            
            return df

        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()
            
    def fetch_data_from_cochrane(self, cd_codes):
        filters = ",".join(cd_codes)
        url = f"https://data.cochrane.org/search/content?filters={filters}&filtersOp=should&pageSize=1000"

        # Make the GET request with error handling
        headers = {
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Referer': 'https://www.cochranelibrary.com/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Check for HTTP errors

            # Attempt to parse JSON response
            try:
                data = response.json()
                
                fields = [
                    "cdIdentifier", 
                    "title", 
                    # "summary", 
                    "doi", 
                    "modifiedDate"
                ]
                data_frame = self.json_data_to_dataframe(data, fields)
                return data_frame
            except ValueError:
                print("Response is not in JSON format.")
                print(response.text) 

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
        
    def fetch_access_icons_for_dois(self, dois):
        url = "https://www.cochranelibrary.com/search"
        
        params = {
            "p_p_id": "scolarissearchresultsportlet_WAR_scolarissearchresults",
            "p_p_lifecycle": 2,
            "p_p_state": "exclusive",
            "p_p_mode": "view",
            "p_p_resource_id": "get-access-icons-for-dois",
            "dois": ",".join(dois)
        }
        
        headers = {
            "Host": "www.cochranelibrary.com",
            'accept-language': 'en-GB-oxendict,en-US;q=0.9,en;q=0.8,yo;q=0.7',
            'cookie': 'GUEST_LANGUAGE_ID=en_US; COOKIE_SUPPORT=true; osano_consentmanager_uuid=ca4d3e91-b57b-4a3d-a016-fde74cd79d96; osano_consentmanager=Nzjf7YZ3KmhYJBGYIahMg5_gokVuae4KhVL-zxFdyAVDtN5z_zqwvLrrsO3w9q00Foyef1VQAUZIGg6tSRF3QMSqPwPCvOnQWoJVXGhc0o_5b12XBqw7aQZLvdmEZYWkM_B5JNyb1ijdX9c12K-E2CscO2useUE1hNFxKSISMz1bGWGew2XIhyn0Dhkgv9tyy4pxXooA7lH5lTxE0YUsziIKDYrA70R8kYCo91Yxw0ZC33Ob0x5CZfGPR1MHNj2Y_C_w1f7S4xq2nBC3fh5j5P9qWpQ=; LOGIN=61776f746f726f6540726b692e6465; SCREEN_NAME=6e496f7855714d50757255616f66666a6850693643773d3d; USER_UUID=6c792f63342b684d70784837652b784b564937554767586144384261587474514348664a4b50417a5038453d; JSESSIONID=cspbwgreclprt160y1~184C6774B199E15D95729A10D23A3F7E; SID_REP=A1F4C5C29BD3D2A8662C3F04B4B4A072; SCOLAUTHSESSIONID=2319E7ECF63D92AF88FE66AC2B03A5D2; __cf_bm=3RNZVknCUghPMU_9FbzhrCqUq0d15SUsrCkp6KupZUQ-1729981431-1.0.1.1-J_bvVhF1EWRt3KuELVnax9SXcyvbBrqQ_wQ5Cnt_u8R9tW2GjNR.QjOUS4aZ5pSZgDlITJleYp566ho39w0yPQ; LFR_SESSION_STATE_20159=1729981565325; cf_clearance=UH6VVuiZ9nU8rV9KJ55kONJCmCIp70X02ETB8aKtC.w-1729981565-1.2.1.1-UphBOiOLQXQVcksUkZ05VqoiMbmPcSG4yVPgibdxeUm0oljB_iqvJ10_qvFqMovE2s6tlWdaM3yoH14NjHMZM3XBR1vUFYWwfUOtNGxcl6k4yUp6uRQilkWmhfHq50600aqC3kvuc1X0gPvQWNwtX2ODUCjklRNckuAOKx8JVTUk5CUon0DCitfOmRYkg0cEEGL6SocLLTW8JBMkRiMuvTUHkKvR67S0_an_WnwT4fAoQUqp07rMwziYqTtkJcyaWq5bFhynYaauFusb30.wQd3uMztA8Y89jvjZklzR_GH2_EdQcnPuADK3hauc_k1sqzX.GGYIFQmr.GC93yZl_hid6uY_G.DN3lJY..8WsvMhdKvy.KBoHZAuuvbiIPZO2Fs8zLB0NllVdnajtp2_JA; SCOL_SESSION_TIMEOUT=1740 Sat, 26 Oct 2024 22:57:39 GMT; _ga_BRLBHKT9XF=GS1.1.1729981720.1.0.1729981720.0.0.0',
            'referer': 'https://www.cochranelibrary.com/',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }

        try:
            response = requests.get(url, headers=headers, params=params)

            response.raise_for_status()
            
            response_json = response.json()
            
            access_types = response_json.get('accessTypes', {})
            df = pd.DataFrame(access_types.items(), columns=['doi', 'status'])

            return df

        except requests.RequestException as e:
            print(f"Error occurred: {e}")
            return None
        
        
    
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