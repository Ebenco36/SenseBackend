import random
import string
import time
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from src.Commands.SeleniumPool import PDFDownloader
from src.Utils.Helpers import clean_special_characters, get_contents
from src.Services.Factories.scrapers.GeneralPDFWebScraper import (
    GeneralPDFWebScraper,
)
from src.Services.Factories.Sections.ArticleExtractorFactory import (
    ArticleExtractorFactory,
)
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
        self.session.headers.update(
            header
            or {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

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
        cochrane_alive_url = (
            "https://www.cochranelibrary.com/delegate/scolarisauthportlet/keep-alive"
        )
        try:
            cochrane_response = self.session.get(cochrane_alive_url)
            expires_header = cochrane_response.headers.get(
                "Expires", "Thu, 14 Nov 2024 10:28:28 GMT"
            )
            self.session.headers.update(
                {
                    "Referer": "https://www.cochranelibrary.com",
                    "If-Modified-Since": expires_header,
                }
            )
        except requests.exceptions.RequestException as e:
            print(f"Error initializing Cochrane session: {e}")

    def fetch_pdf_urls(self):
        """Returns the PDF URL directly, as Cochrane URLs are straightforward."""
        try:
            redirected_url = self.fetch_redirected_url()
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {redirected_url}")
                return []
            return [redirected_url]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF URLs: {e}")
            return []
        
    def generate_random_email(self, domain="gmail.com"):
        """Generates a random email address for Unpaywall API access."""
        local_part = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        email = f"{local_part}@{domain}"
        print(f"Generated email: {email}")
        return email
    
    def convert_cochrane_pdf_to_full_text(self, pdf_url):
        """Convert a Cochrane PDF URL to its full-text HTML URL."""
        if "/pdf/" in pdf_url:
            return pdf_url.split("/pdf/")[0] + "/full"
        return pdf_url

    def fetch_text_from_html_for_cochrane(self):
        self.session.headers.update({"Accept-Encoding": "utf-8"})
        self.url = self.convert_cochrane_pdf_to_full_text(self.url)
        """Fetches and extracts HTML text specifically for Cochrane."""
        return get_contents(self.url)
        
