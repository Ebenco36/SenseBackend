import time
import random
import string
import PyPDF2
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from src.Utils.Helpers import get_final_url, get_final_redirected_url

class GeneralPDFWebScraper:
    """
    A base class for scraping PDF documents from websites, processing redirects,
    and extracting content from HTML or PDF files.
    
    Attributes:
        url (str): The initial URL to scrape.
        DB_name (str): Optional name of the database or source (e.g., "Cochrane").
        session (requests.Session): A session for making HTTP requests.
    """

    def __init__(self, url, DB_name=None, session=None):
        self.url = url
        self.DB_name = DB_name
        self.session = session or requests.Session()
    
    def fetch_redirected_url(self, url=None):
        """Fetches the final URL after redirections."""
        set_url = url if url else self.url
        final_url = get_final_url(set_url)
        try:
            return get_final_redirected_url(final_url)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching redirected URL: {e}")
            return final_url
    
    def extract_doi_from_url(self, url):
        """Extracts DOI from a given URL, if in the expected format."""
        try:
            parsed_url = urlparse(url.lower())
            if 'dx.doi.org' in parsed_url.netloc:
                return parsed_url.path.lstrip('/')
            print("Invalid DOI URL format")
            return None
        except Exception as e:
            print(f"Error extracting DOI from URL: {e}")
            return None

    def fetch_pdf_content(self, pdf_url):
        """Fetches the PDF content from a given URL."""
        try:
            response = self.session.get(pdf_url)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '')
            return response.content, content_type
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF content from {pdf_url}: {e}")
            return b'', ''

    def extract_text_from_pdf(self, pdf_content):
        """Extracts text from PDF content."""
        try:
            pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
            text = ''.join(page.extract_text() for page in pdf_reader.pages)
            return text
        except Exception as e:
            print(f"Error extracting text from PDF: {e}")
            return ''

    
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
            if data.get("best_oa_location") and data["best_oa_location"].get("url_for_pdf"):
                return [data["best_oa_location"]["url_for_pdf"]]
            elif data.get("first_oa_location") and data["first_oa_location"].get("url_for_pdf"):
                return [data["first_oa_location"]["url_for_pdf"]]
            elif data.get("best_oa_location") and data["best_oa_location"].get("url"):
                print(f"No open access PDF found for DOI: {doi}. Checking the HTML link...")
                return [self.fetch_redirected_url(data["best_oa_location"]["url"])]
            print("Found this ..")
            return [self.fetch_redirected_url()]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Unpaywall: {e}")
            return [self.fetch_redirected_url()]
        
    # def fetch_text_from_html(self):
    #     """Fetches and extracts plain text from the HTML of the URL."""
    #     self.session.headers.update({
    #         "Accept-Encoding": "utf-8"
    #     })
    #     try:
    #         redirected_url = self.fetch_redirected_url()
    #         if not redirected_url:
    #             print(f"Failed to fetch redirected URL for {self.url}")
    #             return ''

    #         response = self.session.get(redirected_url)
    #         response.raise_for_status()
    #         soup = BeautifulSoup(response.content, 'html.parser')
    #         return soup.get_text()
    #     except requests.exceptions.RequestException as e:
    #         print(f"Error fetching HTML content: {e}")
    #         return ''

    def fetch_pdf_urls(self):
        self.session.headers.update({
            "Accept-Encoding": "utf-8"
        })
        """
        Fetches PDF URLs based on the specified database or website.
        This method can be customized by inheriting classes to handle specific websites.
        """
        redirected_url = self.fetch_redirected_url()
        # print(self.url, "Whatsup", redirected_url)
        try:
            
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {self.url}")
                return []

            response = self.session.get(redirected_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            pdf_urls = [urljoin(redirected_url, link['href']) for link in soup.find_all('a', href=True) if link['href'].endswith('.pdf')]
            
            if len(pdf_urls) > 0:
                return pdf_urls
            return [redirected_url]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF URLs: {e}")
            return [redirected_url]
        except Exception as e:
            print(f"An error occurred: {e}")
            return [redirected_url]
        
    def fetch_text_from_html(self):
        """Fetches and extracts plain text from the HTML content of the URL."""
        redirected_url = self.fetch_redirected_url()
        self.session.headers.update({
            "Accept-Encoding": "utf-8"
        })
        print(redirected_url)
        try:
            
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {self.url}")
                return ''

            response = self.session.get(redirected_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup.get_text()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching HTML content: {e}. But we are trying a different method.")
            from src.Utils.Helpers import html_to_plain_text_selenium
            return html_to_plain_text_selenium(redirected_url, headless=True)

