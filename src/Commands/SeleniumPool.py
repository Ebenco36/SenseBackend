import os
import time
import shutil
import fitz  # PyMuPDF
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

class PDFDownloader:
    def __init__(self, download_dir="downloads", wait_time=10):
        self.download_dir = os.path.join(os.getcwd(), download_dir)
        os.makedirs(self.download_dir, exist_ok=True)
        self.wait_time = wait_time
        self.driver = self._init_driver()
    
    def _init_driver(self):
        chrome_options = Options()
        chrome_options.add_experimental_option('prefs', {
            "download.default_directory": self.download_dir,
            "plugins.always_open_pdf_externally": True,
            "download.prompt_for_download": False
        })
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    def _extract_key_from_url(self, url):
        """
        Extract the unique key from the URL to use as the filename.
        Example: 'S1098-3015(14)04218-1'
        """
        'https://www.valueinhealthjournal.com/article/S1098-3015(14)04218-1/pdf'
        
        key = url.split("/")
        return key[4]

    def _file_exists(self, file_name):
        """Check if a file with the given name already exists in the download directory."""
        return os.path.exists(os.path.join(self.download_dir, file_name))

    def download_pdf(self, url):
        """
        Download a PDF from the provided URL and save it with a specific name based on the URL key.
        If the file already exists, the download is skipped.
        """
        file_key = self._extract_key_from_url(url)
        target_file_name = f"{file_key}.pdf"
        target_file_path = os.path.join(self.download_dir, target_file_name)

        # Check if the file already exists
        if self._file_exists(target_file_name):
            print(f"The file '{target_file_name}' already exists. Skipping download.")
            return target_file_path

        # Open the URL
        try:
            self.driver.get(url)
            time.sleep(self.wait_time)  # Wait for download to complete

            # Find any newly downloaded PDF and rename it to the desired name
            downloaded_files = [f for f in os.listdir(self.download_dir) if f.endswith('.pdf')]
            for downloaded_file in downloaded_files:
                downloaded_file_path = os.path.join(self.download_dir, downloaded_file)
                if downloaded_file != target_file_name:
                    shutil.move(downloaded_file_path, target_file_path)
                    print(f"Downloaded and renamed file to: {target_file_name}")
                    break
            else:
                print("No new PDF files were detected for renaming.")
        finally:
            self.driver.quit()

        return target_file_path

    def read_pdf_content(self, file_path):
        """Read and return the text content from a PDF file."""
        content = ""
        try:
            with fitz.open(file_path) as pdf_file:
                for page_num in range(pdf_file.page_count):
                    page = pdf_file[page_num]
                    content += page.get_text()
            print("PDF content successfully read.")
        except Exception as e:
            print(f"Error reading PDF content: {e}")
        
        return content
    
    def read_pdf_binary(self, file_path):
        """Read the PDF content as raw binary data."""
        try:
            with open(file_path, 'rb') as pdf_file:
                binary_content = pdf_file.read()
            print("PDF content read as binary data.")
            return binary_content
        except Exception as e:
            print(f"Error reading PDF content: {e}")
            return None