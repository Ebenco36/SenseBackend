import re
import time
import random
import string
import PyPDF2
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from src.Utils.Helpers import (
    get_final_url, get_final_redirected_url, clean_special_characters, 
    convert_elsevier_to_sciencedirect, extract_pdf_links,
    fetch_all_content_for_linked
)
class GeneralPDFWebScraper:
    """
    A base class for scraping PDF documents from websites, processing redirects,
    and extracting content from HTML or PDF files.
    
    Attributes:
        url (str): The initial URL to scrape.
        DB_name (str): Optional name of the database or source (e.g., "Cochrane").
        session (requests.Session): A session for making HTTP requests.
    """

    def __init__(self, DB_name=None, session=None):
        self.DB_name = DB_name
        self.session = session or requests.Session()
    
    
    def set_doi_url(self, url):
        self.url = url
        return self
        
    def fetch_redirected_url(self, url=None):
        """Fetches the final URL after redirections."""
        set_url = url if url else self.url
        
        final_url = get_final_url(set_url)
        try:
            if "linkinghub" in final_url:
                return convert_elsevier_to_sciencedirect(final_url)
            elif final_url == "" or final_url is None:
                return get_final_redirected_url(final_url)
            else:
                return final_url
        except requests.exceptions.RequestException as e:
            print(f"Error fetching redirected URL: {e}")
            return final_url
    
    def extract_doi_from_url(self, url):
        """Extracts DOI from a given URL, if in the expected format."""
        try:
            # Parse the URL to get the path
            url = url.lower()
            parsed_url = urlparse(url)
            # Also check using regular patterns
            doi_pattern = r'10.\d{4,9}/[-._;()/:A-Z0-9]+'
            match = re.search(doi_pattern, url, re.IGNORECASE)
            # The DOI is usually the part of the path after 'dx.doi.org/'
            if 'dx.doi.org' in parsed_url.netloc:
                doi = parsed_url.path.lstrip('/')
                return doi
            elif match:
                return match.group(0)
            else:
                return self.get_doi_from_any_url_with_selenium(url)
        except Exception as e:
            print(f"Error extracting DOI from URL: {e}")
            return None

    def fetch_pdf_urls_2(self):
        """
        Fetches all PDF URLs from the redirected URL by parsing HTML content.
        Handles special cases like 'linkinghub' URLs with Unpaywall support.

        Returns:
            list: List of URLs pointing to PDF files.
        """
        pdfs = []
        try:
            redirected_url = self.fetch_redirected_url()
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {self.url}")
                return []
            
            if 'linkinghub' in redirected_url:
                doi = self.extract_doi_from_url(self.url)
                
                if doi:
                    pdf_urls = self.fetch_from_unpaywall(doi)
                    pdfs = pdf_urls if pdf_urls else [doi]
                
            elif redirected_url:
                pdf_links = extract_pdf_links(redirected_url)
                pdfs = pdf_links
                    
            else:
                doi = self.extract_doi_from_url(self.url)
                if doi:
                    pdfs = self.fetch_from_unpaywall(doi)
                else:
                    pdfs = []
            
            if len(pdfs) == 0:
                doi = self.extract_doi_from_url(self.url)
                if doi:
                    pdfs = self.fetch_from_unpaywall(doi)
                else:
                    pdfs = []
            
                 
            return pdfs
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF URLs: {e}")
            return []
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return []
        
        
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
            
            return [self.fetch_redirected_url()]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Unpaywall: {e}")
            return [self.fetch_redirected_url()]
        
    def fetch_pdf_urls(self):
        self.session.headers.update({
            "Accept-Encoding": "utf-8"
        })
        """
        Fetches PDF URLs based on the specified database or website.
        This method can be customized by inheriting classes to handle specific websites.
        """
        redirected_url = self.fetch_redirected_url()
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
    
    def get_doi_from_any_url_with_selenium(self, url):
        """
        Use Selenium to extract DOI from the webpage if it is not directly available in the URL.
        """
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')

        # Automatically manage Chromedriver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        try:
            driver.get(url)
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # Check meta tags for DOI
            meta_tags = soup.find_all('meta')
            for tag in meta_tags:
                if 'name' in tag.attrs and 'doi' in tag.attrs['name'].lower():
                    return tag.get('content', 'DOI not found in meta tag')

            # Fallback: Search for DOI in the page content
            doi_pattern = r'10.\d{4,9}/[-._;()/:A-Z0-9]+'
            match = re.search(doi_pattern, page_source, re.IGNORECASE)
            if match:
                return match.group(0)

            return "DOI not found on the page."
        finally:
            driver.quit()

    def get_doi(self, url):
        """
        Main function to get DOI: first tries to extract from URL, then uses Selenium if needed.
        """
        doi = self.extract_doi_from_url(url)
        if doi:
            return doi

        # If DOI is not in the URL, fall back to Selenium
        return self.get_doi_from_any_url_with_selenium(url)

    def fetch_text_from_html(self):
        """Fetches and extracts plain text from the HTML content of the URL."""
        redirected_url = self.fetch_redirected_url()
        print(redirected_url)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.77 Safari/537.3",
            "Accept-Encoding": "utf-8",
            # "Accept-Language": "en-US,en;q=0.5",
            # "Sec-Fetch-Dest": "document",
            # "Sec-Fetch-Mode": "navigate",
            # "Sec-Fetch-Site": "same-origin",
            # "Sec-Fetch-User": "?1"
        })
        
        try:
            
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {self.url}")
                return ''

            response = self.session.get(redirected_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            return clean_special_characters(
                soup.get_text()
            )
        except requests.exceptions.RequestException as e:
            print(f"Error fetching HTML content: {e}. But we are trying a different method.")
            from src.Utils.Helpers import html_to_plain_text_selenium
            return clean_special_characters(
                html_to_plain_text_selenium(
                    redirected_url, 
                    headless=True
                )
            )

    def fetch_and_extract_first_valid_pdf_text(self):
        """
        Fetches and extracts text from the first available PDF URL or falls back to HTML content
        for various databases and URL scenarios.

        Returns:
            str: Extracted text content from the first valid PDF or HTML as fallback.
        """
        # Update headers for better compression handling
        self.session.headers.update({
            "Accept-Encoding": "gzip, deflate, br"
        })

        # Fetch PDF URLs dynamically
        pdf_urls = self.fetch_pdf_urls() if hasattr(self, 'fetch_pdf_urls') else self.fetch_pdf_urls_2()
        if not pdf_urls:
            return "No PDF URLs found."

        # Handle ScienceDirect URLs
        if "sciencedirect" in pdf_urls[0]:
            cookies_file = "cookies.json"
            return fetch_all_content_for_linked(pdf_urls[0], cookies_file)

        # Handle ValueInHealth URLs
        if "valueinhealthjournal" in pdf_urls[0]:
            downloader = PDFDownloader()
            downloaded_file_path = downloader.download_pdf(pdf_urls[0])
            if downloaded_file_path:
                pdf_content = downloader.read_pdf_binary(downloaded_file_path)
                return clean_special_characters(pdf_content)

        # Handle Academic OUP URLs
        redirected_url = self.fetch_redirected_url() if hasattr(self, 'fetch_redirected_url') else None
        if "academic.oup.com" in redirected_url:
            # Specific processing for Academic OUP URLs
            pdf_content, content_type = "", ""

        else:
            # General PDF fetching
            pdf_content, content_type = self.fetch_pdf_content(pdf_urls[0])

        # Process PDF content if valid
        if pdf_content and "application/pdf" in content_type:
            text = self.extract_text_from_pdf(pdf_content)
            if text:
                return clean_special_characters(text)

        # Database-specific fallback for Cochrane
        if hasattr(self, "DB_name") and self.DB_name == "Cochrane":
            return self.fetch_text_from_html_for_cochrane()

        # General fallback to HTML content
        return self.fetch_text_from_html()
