import random
import string
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from src.Commands.SeleniumPool import PDFDownloader
from src.Utils.Helpers import (
    extract_pdf_links, clean_special_characters, 
    fetch_all_content_for_linked
)
from src.Services.Factories.GeneralPDFScraper.GeneralPDFWebScraper import GeneralPDFWebScraper
import PyPDF2

class OVIDPDFWebScraper(GeneralPDFWebScraper):
    """
    A base class for web scraping PDF links, handling redirects, and extracting PDF or HTML content.
    
    Attributes:
        url (str): The URL to scrape.
        DB_name (str): Optional name of the database or source.
        session (requests.Session): A session for making HTTP requests with optional headers.
    """

    def __init__(self, DB_name=None, header=None):
        self.DB_name = DB_name
        self.session = requests.Session()
        self.session.headers.update(header or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
        })
        
        super().__init__(self.DB_name, self.session)
    
    def set_doi_url(self, url):
        self.url = url
        super().set_doi_url(self.url)
        return self
        
    def generate_random_email(self, domain="gmail.com"):
        """Generates a random email address for Unpaywall API access."""
        local_part = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        email = f"{local_part}@{domain}"
        print(f"Generated email: {email}")
        return email