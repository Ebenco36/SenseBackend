import PyPDF2
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class GeneralPDFWebScraper:
    def __init__(self, url):
        self.url = url

    def fetch_redirected_url(self):
        try:
            response = requests.head(self.url, allow_redirects=True)
            redirected_url = response.url
            return redirected_url
        except requests.exceptions.RequestException as e:
            print(f"Error fetching redirected URL: {e}")
            return None

    def fetch_pdf_urls(self):
        try:
            redirected_url = self.fetch_redirected_url()
            if not redirected_url:
                print(f"Failed to fetch redirected URL for {self.url}")
                return []

            response = requests.get(redirected_url)
            response.raise_for_status()  # Raise HTTPError for bad responses
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)
            pdf_urls = [urljoin(redirected_url, link['href']) for link in links if link['href'].endswith('.pdf')]
            return pdf_urls
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF URLs: {e}")
            return []

    def fetch_pdf_content(self, pdf_url):
        try:
            response = requests.get(pdf_url)
            response.raise_for_status()  # Raise HTTPError for bad responses
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

    def fetch_and_extract_first_valid_pdf_text(self):
        pdf_urls = self.fetch_pdf_urls()

        for pdf_url in pdf_urls:
            pdf_content = self.fetch_pdf_content(pdf_url)
            if pdf_content:
                text = self.extract_text_from_pdf(pdf_content)
                print("Pdf content...")
                if text:
                    return text

        print("No valid PDFs found on the page.")
        html_text = self.fetch_text_from_html()
        return html_text if html_text else ''
