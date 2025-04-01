import requests
from bs4 import BeautifulSoup
import re
from PyPDF2 import PdfReader
from io import BytesIO 

class BaseArticleExtractor:
    def __init__(self, soup=None, url=None, pdf_content=None, text_content=None):
        self.url = url
        self.soup = self.remove_heading_numbering_from_html(soup)
        
        self.pdf_content = pdf_content
        self.doc = None
        self.sections_dict = {}
        self.pdf_reader = None
        self.text_content = text_content
        self._fetch_and_parse()
        
    def remove_heading_numbering_from_html(self, soup):
        """
        Removes leading numbering (e.g., "1. ", "a)", "(2)") from HTML blocks that resemble headings or list items,
        without affecting true numerical values like "1.4", "3.5%", etc.
        """
        if soup:
            # remove superscript
            for sup in soup.find_all("sup"):
                sup.decompose()

            # Regex to catch numbering *only at the start* of a text block
            numbering_pattern = re.compile(
                r'^\s*(?:\(?[ivxIVX]{1,5}\)?|[a-zA-Z]{1,2}|\(?\d{1,2}\)?)[:\.\)]\s+(?=[A-Z])'
            )

            for element in soup.find_all(string=True):
                if element.parent.name in ['p', 'li', 'div', 'span', 'td']:
                    original = element.strip()
                    if numbering_pattern.match(original):
                        cleaned = numbering_pattern.sub('', original)
                        element.replace_with(cleaned)

            return soup

    def _fetch_and_parse(self):
        """Fetch and parse content based on whether it's a PDF or an HTML page."""
        class_name = self.__class__.__name__

        if self.pdf_content or class_name == "PDFUniversalExtractor":
            self._process_pdf()
        elif self.text_content:
            self.text_content = self.text_content # Still doing the same thing ... Think later
        else:
            self._process_html()
        self._extract_sections()

    def _process_html(self):
        """Fetch and parse HTML content."""
        if not self.soup and self.url:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                response = requests.get(self.url, headers=headers, timeout=10)
                response.raise_for_status()
                self.soup = BeautifulSoup(response.content, 'html.parser')
                
            except requests.RequestException as e:
                print(f"Error fetching HTML content: {e}")

    def _process_pdf(self):
        """Fetch and parse PDF content using PyPDF2."""
        if not self.pdf_content and self.url:
            # Check if the URL is actually a PDF
            content_type = self._get_content_type(self.url)

            if "application/pdf" in content_type:
                self.pdf_content, _ = self.fetch_pdf_content(self.url)
            else:
                print(f"Warning: URL does not point to a PDF ({self.url})")

        if self.pdf_content:
            print("PDF content detected. Decoding with PyPDF2...")
            try:
                self.pdf_reader = PdfReader(BytesIO(self.pdf_content))  # Open PDF
                print("PDF successfully loaded using PyPDF2.")
            except Exception as e:
                print(f"Error loading PDF content with PyPDF2: {e}")

    def _get_content_type(self, url):
        """Check the Content-Type of the URL before fetching full content."""
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            return response.headers.get("Content-Type", "")
        except requests.RequestException as e:
            print(f"Error checking content type: {e}")
            return "application/pdf"

    def fetch_pdf_content(self, pdf_url):
        """Fetch PDF content as bytes from a URL."""
        try:
            response = requests.get(pdf_url, timeout=15)
            response.raise_for_status()
            return response.content, response.headers.get("Content-Type", "")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PDF content from {pdf_url}: {e}")
            return b'', ''
        
    def _extract_sections(self):
        """Method to be implemented by child classes to extract sections based on site-specific structures."""
        raise NotImplementedError("Subclasses must implement _extract_sections.")

    def get_section(self, section_name):
        """Retrieve a specific section by name."""
        return self.sections_dict.get(section_name, f"Section '{section_name}' not found.")

    def get_subsections(self, section_name):
        """Retrieve all subsections for a given section name."""
        subsections = {key: value for key, value in self.sections_dict.items() if key.startswith(f"{section_name} ")}
        if not subsections:
            return f"No subsections found for section '{section_name}'."
        return subsections

    def get_available_sections(self):
        """Return a list of available sections."""
        return list(self.sections_dict.keys())

    def get_abstract(self):
        return self.get_section("Abstract")

    def get_abstract_subsections(self):
        """Retrieve subsections under the abstract, such as Introduction, Methods, etc."""
        return self.get_subsections("Abstract")

    def get_introduction(self):
        """Retrieve the Introduction section from the main body."""
        return self.get_section("Introduction")

    def get_methods(self):
        """Retrieve the Methods section from the main body."""
        return self.get_section("Methods")

    def get_results(self):
        """Retrieve the Results section from the main body."""
        return self.get_section("Results")

    def get_discussion(self):
        """Retrieve the Discussion section from the main body."""
        return self.get_section("Discussion")

    def get_conclusion(self):
        """Retrieve the Conclusion section from the main body."""
        return self.get_section("Conclusion")

    def get_references(self):
        """Retrieve the References section from the main body."""
        return self.get_section("References")
    

    def remove_section_numbering_from_html(self, html: str) -> str:
        """
        Removes section-style numbering (e.g., '3.1 Study') from the beginning of paragraph-like elements in HTML.
        Leaves real numeric values (like 1.4, 3.2%) untouched.
        """
        soup = BeautifulSoup(html, "html.parser")
        section_pattern = re.compile(r'^\s*\d+(\.\d+)*[\.\)]?\s+(?=[A-Z])')

        for element in soup.find_all(string=True):
            if element.parent.name in ['p', 'div', 'span', 'li', 'td']:
                original = element.strip()
                if section_pattern.match(original):
                    cleaned = section_pattern.sub('', original)
                    element.replace_with(cleaned)

        return str(soup)

    def remove_all_section_numbering_from_text(self, text: str) -> str:
        """
        Removes section-like numbering patterns such as '2.1 Title', '3.2.1 Heading'
        from any part of the document (even inline), as long as followed by a capital letter.
        Real numeric values (e.g., 1.4 mg) are preserved.
        """
        if not isinstance(text, str):
            raise TypeError(f"Expected string in remove_all_section_numbering_from_text, got {type(text).__name__}")
        # Match patterns like 1., 2.1, 3.4.5, 1), 2.3), (1.2), etc., followed by a capital letter
        pattern = re.compile(
            r'\b(?:\(?\d+(?:\.\d+){0,2}\)?[\.\)]?)\s+(?=[A-Z])'
        )
        return pattern.sub('', text)
    
    
