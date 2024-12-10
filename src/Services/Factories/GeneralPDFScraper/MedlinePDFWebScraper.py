import random
import string
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from src.Commands.SeleniumPool import PDFDownloader
from charset_normalizer import detect
from src.Utils.Helpers import extract_pdf_links, clean_special_characters
from src.Services.Factories.GeneralPDFScraper.GeneralPDFWebScraper import GeneralPDFWebScraper
import PyPDF2

class MedlinePDFWebScraper(GeneralPDFWebScraper):
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
            # "Accept-Encoding": "gzip, deflate, br"
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


    # def fetch_and_extract_first_valid_pdf_text(self):
    #     """
    #     Fetches and extracts text from the first available PDF URL, or falls back to HTML content.

    #     Returns:
    #         str: Extracted text content from the first valid PDF or HTML as fallback.
    #     """
    #     pdf_urls = self.fetch_pdf_urls_2()
    #     # for pdf_url in pdf_urls:
    #     pdf_content, content_type = self.fetch_pdf_content(pdf_urls[0])
    #     if pdf_content and "application/pdf" in content_type:
    #         text = self.extract_text_from_pdf(pdf_content)
    #         if text:
    #             return clean_special_characters(text)

    #     return self.fetch_text_from_html()