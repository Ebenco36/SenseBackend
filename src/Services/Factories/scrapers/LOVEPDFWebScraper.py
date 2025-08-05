import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from src.Commands.SeleniumPool import PDFDownloader
from src.Utils.Helpers import clean_special_characters
from src.Services.Factories.scrapers.GeneralPDFWebScraper import GeneralPDFWebScraper
import PyPDF2

class LOVEPDFWebScraper(GeneralPDFWebScraper):
    """
    A specialized scraper for extracting PDFs and HTML content from LOVE Database URLs.
    Inherits common functionality from GeneralPDFWebScraper and customizes methods
    for handling LOVE-specific content.

    Attributes:
        url (str): The URL to scrape.
        DB_name (str): Optional database name, default is "LOVE" if not provided.
        session (requests.Session): HTTP session with headers for LOVE Database.
    """

    def __init__(self, DB_name="LOVE", header=None):
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
        