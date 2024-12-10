import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from src.Commands.SeleniumPool import PDFDownloader
from src.Utils.Helpers import clean_special_characters
from src.Services.Factories.GeneralPDFScraper.GeneralPDFWebScraper import GeneralPDFWebScraper
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
        
    # def fetch_and_extract_first_valid_pdf_text(self):
    #     """
    #     Fetches the first available PDF content from the LOVE Database URL, extracts text,
    #     or falls back to HTML content if no valid PDF is available.

    #     Returns:
    #         str: Extracted text content from the first valid PDF or HTML fallback.
    #     """
    #     self.session.headers.update({
    #         "Accept-Encoding": "gzip, deflate, br"
    #     })
    #     pdf_urls = self.fetch_pdf_urls()
    #     pdf_content, content_type = self.fetch_pdf_content(pdf_urls[0])
    #     if pdf_content and content_type == "application/pdf":
    #         text = self.extract_text_from_pdf(pdf_content)
    #         if text:
    #             return clean_special_characters(text)
    #     return self.fetch_text_from_html()
