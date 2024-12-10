import time
import random
import string
import PyPDF2
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.parse import urlparse
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.Commands.SeleniumPool import PDFDownloader

class GeneralPDFWebScraper:
    def __init__(self, url, DB_name=None, header=None):
        self.url = url
        self.DB_name = DB_name
        self.header = header

    def fetch_redirected_url(self):
        try:
            response = requests.head(self.url, allow_redirects=True)
            redirected_url = response.url
            return redirected_url
        except requests.exceptions.RequestException as e:
            print(f"Error fetching redirected URL: {e}")
            return None

    def fetch_pdf_urls_Old(self):
        try:
            redirected_url = self.fetch_redirected_url()
            print(redirected_url)
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {self.url}")
                return []

            response = requests.get(redirected_url)
            print(response.content)
            response.raise_for_status()  # Raise HTTPError for bad responses
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)
            print(links)
            pdf_urls = [urljoin(redirected_url, link['href']) for link in links if link['href'].endswith('.pdf')]
            return pdf_urls
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF URLs: {e}")
            return []

    
    def generate_random_email(self, domain_url="gmail.com"):
        # Generate a random string of 8 characters for the local part of the email
        local_part = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        email = f"{local_part}@{domain_url}"
        print(f"Generated email: {email}")
        return email

    def fetch_pdf_urls(self):
        try:
            redirected_url = self.fetch_redirected_url()
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {self.url}")
                return []

            # Check if the URL contains 'linkinghub', which requires special handling
            if 'linkinghub' in redirected_url:
                # Extract DOI from the redirected URL
                # Assuming DOI is in the URL path after "/pii/"
                print(self.url)
                doi = self.extract_doi_from_url(self.url)
                print(doi)
                # Use Unpaywall API to fetch open access PDF links
                if doi:
                    pdf_urls = self.fetch_from_unpaywall(doi)
                    return pdf_urls if pdf_urls else []
            elif self.DB_name == "Cochrane":
                return [self.url]
            else:
                # For non-'linkinghub' URLs, use the regular requests approach
                response = requests.get(redirected_url)
                response.raise_for_status()  # Raise an HTTPError for bad responses
                soup = BeautifulSoup(response.content, 'html.parser')

                # Extract PDF links
                links = soup.find_all('a', href=True)
                pdf_urls = [urljoin(redirected_url, link['href']) for link in links if link['href'].endswith('.pdf')]
                return pdf_urls

        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF URLs: {e}")
            return []
        except Exception as e:
            print(f"An error occurred: {e}")
            return []

        
    def extract_doi_from_url(self, url):
        try:
            # Parse the URL to get the path
            url = url.lower()
            parsed_url = urlparse(url)
            # Also check using regular patterns
            doi_pattern = r'10.\d{4,9}/[-._;()/:A-Z0-9]+'
            match = re.search(doi_pattern, url, re.IGNORECASE)
            # The DOI is usually the part of the path after 'dx.doi.org/'
            if 'dx.doi.org' in parsed_url.netloc:
                doi = parsed_url.path.lstrip('/')  # Remove leading slashes
                return doi
            elif(match):
                return match.group(0)
            else:
                print("Invalid DOI URL format")
                return None
        except Exception as e:
            print(f"Error extracting DOI from URL: {e}")
            return None
    

    # Helper function to fetch PDF from Unpaywall API
    def fetch_from_unpaywall(self, doi):
        email = self.generate_random_email()
        try:
            unpaywall_api_url = f"https://api.unpaywall.org/v2/{doi}?email=" + email
            response = requests.get(unpaywall_api_url)
            response.raise_for_status()

            data = response.json()
            if data.get("best_oa_location") and data["best_oa_location"].get("url_for_pdf"):
                pdf_url = data["best_oa_location"]["url_for_pdf"]
                # new_url = pdf_url.replace('pdf', 'fulltext')
                return [pdf_url]  # Return as a list for consistency
            else:
                print(f"No open access PDF found for DOI: {doi}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Unpaywall: {e}")
            return []
    
    def fetch_pdf_content(self, pdf_url):
        try:
            response = requests.get(pdf_url, headers=self.header)
            response.raise_for_status()
            pdf_content = response.content
            return pdf_content
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF content from {pdf_url}: {e}")
            return b''

    def extract_text_from_pdf(self, pdf_content):
        try:
            pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
            text = ''
            for page_num in range(len(pdf_reader.pages)):
                text += pdf_reader.pages[page_num].extract_text()
            return text
        except Exception as e:
            print(f"Error extracting text from PDF: {e}")
            return ''

    def fetch_text_from_html(self):
        try:
            redirected_url = self.fetch_redirected_url()
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {self.url}")
                return ''

            response = requests.get(redirected_url)
            response.raise_for_status()  # Raise HTTPError for bad responses
            soup = BeautifulSoup(response.content, 'html.parser')
            text = soup.get_text()
            return text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching HTML content: {e}")
            return ''

    def fetch_text_from_html_for_cochrane(self):
        try:
            response = requests.get(self.url, headers=self.header)
            response.raise_for_status()  # Raise HTTPError for bad responses
            soup = BeautifulSoup(response.content, 'html.parser')
            text = soup.get_text()
            return text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching HTML content: {e}")
            return ''
        
    def fetch_and_extract_first_valid_pdf_text(self):
        pdf_urls = self.fetch_pdf_urls()
        print(pdf_urls)
        for pdf_url in pdf_urls:
            if "valueinhealthjournal" in pdf_url:
                downloader = PDFDownloader()
                downloaded_file_path = downloader.download_pdf(pdf_url)
                print(f"File saved at: {downloaded_file_path}")
                # pdf_content = downloader.read_pdf_content(downloaded_file_path)
                pdf_content = downloader.read_pdf_binary(downloaded_file_path)
            else:
                pdf_content = self.fetch_pdf_content(pdf_url)
            if pdf_content:
                text = self.extract_text_from_pdf(pdf_content)
                print("Pdf content...")
                if text:
                    return text

        print("No valid PDFs found on the page.")
        if self.DB_name == "Cochrane":
            html_text = self.fetch_text_from_html_for_cochrane()
        else:
            html_text = self.fetch_text_from_html()
        return html_text if html_text else ''
