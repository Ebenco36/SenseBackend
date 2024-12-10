import time
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from src.Commands.SeleniumPool import PDFDownloader
from src.Utils.Helpers import clean_special_characters
from src.Services.Factories.GeneralPDFScraper.GeneralPDFWebScraper import GeneralPDFWebScraper
import PyPDF2

class CochranePDFWebScraper(GeneralPDFWebScraper):
    """
    A class specialized for scraping PDFs and HTML content specifically from Cochrane.
    Inherits common scraping functionalities from GeneralPDFWebScraper and customizes headers and URL handling for Cochrane requirements.

    Attributes:
        url (str): The URL to scrape.
        DB_name (str): Optional database name, defaults to "Cochrane" if not provided.
        session (requests.Session): HTTP session with custom headers for Cochrane.
    """

    def __init__(self, DB_name="Cochrane", header=None):
        self.DB_name = DB_name
        self.session = requests.Session()
        self.session.headers.update(header or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
        })

        # Cochrane-specific headers and keep-alive mechanism
        if self.DB_name == "Cochrane":
            self._initialize_cochrane_session()
        
        super().__init__(self.DB_name, self.session)
        
    def set_doi_url(self, url):
        self.url = url
        super().set_doi_url(self.url)
        return self
        
    def _initialize_cochrane_session(self):
        """Sets Cochrane-specific headers and manages keep-alive requests."""
        cochrane_alive_url = "https://www.cochranelibrary.com/delegate/scolarisauthportlet/keep-alive"
        try:
            cochrane_response = self.session.get(cochrane_alive_url)
            expires_header = cochrane_response.headers.get("Expires", "Thu, 14 Nov 2024 10:28:28 GMT")
            self.session.headers.update({
                "Referer": "https://www.cochranelibrary.com",
                "If-Modified-Since": expires_header
            })
        except requests.exceptions.RequestException as e:
            print(f"Error initializing Cochrane session: {e}")

    def fetch_pdf_urls(self):
        """Returns the PDF URL directly, as Cochrane URLs are straightforward."""
        try:
            redirected_url = self.fetch_redirected_url()
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {self.url}")
                return []
            return [self.url]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF URLs: {e}")
            return []
        
    def fetch_text_from_html_for_cochrane(self):
        self.session.headers.update({
            "Accept-Encoding": "utf-8"
        })
        """Fetches and extracts HTML text specifically for Cochrane."""
        try:
            response = self.session.get(self.url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            return clean_special_characters(soup.get_text())
        except requests.exceptions.RequestException as e:
            print(f"Error fetching HTML content for Cochrane: {e}. But we are trying a different method.")
            from src.Utils.Helpers import html_to_plain_text_selenium
            return clean_special_characters(
                html_to_plain_text_selenium(
                    self.url, 
                    headless=True
                )
            )

    # def fetch_and_extract_first_valid_pdf_text(self):
    #     """
    #     Fetches the first available PDF content from Cochrane URL, extracts text, 
    #     or returns HTML text as a fallback if PDF is not available.
    #     """
    #     self.session.headers.update({
    #         "Accept-Encoding": "gzip, deflate, br"
    #     })
    #     pdf_urls = self.fetch_pdf_urls()
        
    #     pdf_content, content_type = self.fetch_pdf_content(pdf_urls[0])
    #     if pdf_content and "application/pdf" in content_type:
    #         text = self.extract_text_from_pdf(pdf_content)
    #         if text:
    #             return clean_special_characters(text)

    #     # Fallback to HTML content if no valid PDF is found
    #     if self.DB_name == "Cochrane":
    #         return self.fetch_text_from_html_for_cochrane()
    #     return self.fetch_text_from_html()
