import requests
import time
import random
from bs4 import BeautifulSoup
import fitz  # PyMuPDF for PDFs
import docx  # For Word documents
from urllib.parse import urlparse
import cloudscraper

from src.Services.Factories.Sections.PDFSectionParser import parse_and_print_sections
from src.Utils.Helpers import is_sciencedirect_url, is_tandfonline_url


class DocumentExtractor:
    def __init__(self):
        self.headers = {
            "User-Agent": self._get_random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )  # Bypasses Cloudflare
        self.session = requests.Session()

    def _get_random_user_agent(self):
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1"
        ]
        return random.choice(user_agents)

    def fetch_content(self, url):
        """
        Fetch content from a URL using requests.
        If a 403 error occurs, retries using CloudScraper with a delay.
        Returns a tuple: (content, content_type, status_code)
        """
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()  # Raise an error for bad status codes
            return response.content, response.headers.get("Content-Type"), response.status_code
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                print(
                    "403 Forbidden error. Retrying with CloudScraper after a delay...")

                response = self.scraper.get(
                    url, headers=self.headers, timeout=10)
                print(response.status_code)
                if not response.status_code in [200, 201]:
                    # Introduce a random delay to avoid bot detection
                    print("Retrying in 15 secs...")
                    # Random delay between 5-15 seconds
                    delay = random.uniform(5, 15)
                    time.sleep(delay)
                    response = self.scraper.get(url, headers=self.headers)

                response.raise_for_status()
                return response.content, response.headers.get("Content-Type"), response.status_code

        return None, None, None  # Return None if the request fails

    def extract_plain_text_from_html(self, html_content):
        """
        Extract plain text from HTML content using BeautifulSoup.
        Returns a BeautifulSoup object for HTML and plain text for other content.
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            # Remove script and style tags
            for script in soup(["script", "style", "head", "noscript", "footer"]):
                script.decompose()
            return soup
        except Exception as e:
            print(f"Error extracting plain text from HTML: {e}")
            return None

    def extract_plain_text_from_pdf(self, pdf_content):
        """
        Extract plain text from a PDF file using PyMuPDF.
        """
        try:
            plain_text = parse_and_print_sections(pdf_content)
            return plain_text
        except Exception as e:
            print(f"Error extracting plain text from PDF: {e}")
            return None

    def extract_plain_text_from_docx(self, docx_content):
        """
        Extract plain text from a Word document using python-docx.
        """
        try:
            doc = docx.Document(docx_content)
            plain_text = "\n".join([para.text for para in doc.paragraphs])
            return plain_text
        except Exception as e:
            print(f"Error extracting plain text from DOCX: {e}")
            return None

    def extract_content(self, source):
        """
        Extract content from a URL or local file (PDF/DOCX).
        Returns a tuple: (content, status_code)
        """
        # Check if the source is a URL
        if urlparse(source).scheme in ("http", "https"):
            content, content_type, status_code = self.fetch_content(source)

            if content is None:
                return None, status_code

            if "text/html" in content_type:
                soup, status_code = self.extract_plain_text_from_html(
                    content), status_code

                if is_sciencedirect_url(source) and soup:
                    content_tags = soup.select(
                        "div.Abstracts, div[class*='abstract'], article, section")
                    soup = BeautifulSoup("\n".join(str(tag)
                                         for tag in content_tags), "html.parser")
                elif is_tandfonline_url(source) and soup:
                    content_tags = soup.select("div.hlFld-Fulltext")
                    soup = BeautifulSoup("\n".join(str(tag)
                                         for tag in content_tags), "html.parser")
                return soup, status_code
            elif "application/pdf" in content_type:
                return self.extract_plain_text_from_pdf(content), status_code
            elif "application/vnd.openxmlformats-officedocument.wordprocessingml.document" in content_type:
                return self.extract_plain_text_from_docx(content), status_code
            else:
                print(f"Unsupported content type: {content_type}")
                return None, status_code
        # Check if the source is a local file
        elif source.endswith(".pdf"):
            with open(source, "rb") as f:
                pdf_content = f.read()
            # No status code for local files
            return self.extract_plain_text_from_pdf(pdf_content), None
        elif source.endswith(".docx"):
            # No status code for local files
            return self.extract_plain_text_from_docx(source), None
        else:
            print("Unsupported file type or URL.")
            return None, None
