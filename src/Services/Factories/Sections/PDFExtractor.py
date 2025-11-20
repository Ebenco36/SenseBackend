import re
import pdfplumber
import pytesseract
from PIL import Image
from io import BytesIO
from pdf2image import convert_from_bytes
from src.Services.Factories.Sections.BaseArticleExtractor import BaseArticleExtractor

class PDFUniversalExtractor(BaseArticleExtractor):
    
    # Define ordered sections for structured output
    main_content_sections = ["paper_type", "title", "tables", "introduction", "methods", "results", "discussion", "conclusion", "image_text"]

    # Keywords for PRISMA flowchart images
    prisma_image_keywords = {"identification", "screening", "eligibility", "included", "excluded"}

    def _extract_sections(self):
        """Extracts structured sections, tables, and images from a PDF."""
        if not self.pdf_reader:
            print("Error: No PDF content available.")
            return
        
        self.current_section = None
        self.content_buffer = []
        self.sections_dict = {}

        # Define section patterns
        self.section_patterns = {
            "abstract": r"\babstract\b|\bsummary\b",
            "introduction": r"\bintroduction\b|\bbackground\b",
            "methods": r"\bmethods?\b|\bmaterials?\s*(and|&)\s*methods?\b|\bexperimental setup\b",
            "results": r"\bresults?\b|\bfindings\b|\banalysis\b",
            "discussion": r"\bdiscussion\b|\binterpretation\b",
            "conclusion": r"\bconclusion\b|\bsummary\b|\bclosing remarks\b",
            # "references": r"\breferences\b|\bbibliography\b"
        }

        full_text = self._extract_pdf_text()
        self._process_text(full_text)
        self._extract_tables()
        self._extract_images()

        self._finalize_section()
        self.is_open_access()
        self._extract_references()
        self._combine_main_content()

    def _extract_pdf_text(self):
        """Extracts and processes text while ensuring structured output."""
        text_content = []
        with pdfplumber.open(BytesIO(self.pdf_content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_content.append(text.strip())

        # Join and clean extracted text
        full_text = "\n".join(text_content)
        return self._clean_extracted_text(full_text)

    def _clean_extracted_text(self, text):
        """Removes unwanted figure captions, DOI numbers, and false headings."""
        text = re.sub(r'\bFIG\.\s*\d+.*', '', text)  # Removes figure captions
        text = re.sub(r'\d{2,}(\.\d+)?', '', text)   # Removes DOI-like numbers
        return text

    def _process_text(self, text):
        """Processes extracted text and assigns it to structured sections."""
        lines = text.split("\n")
        for line in lines:
            line = line.strip()
            if self._is_heading(line):
                self._finalize_section()
                self.current_section = self._match_section(line)
            elif self.current_section:
                self.content_buffer.append(line)

    def _is_heading(self, text):
        """Identifies headings while avoiding false positives."""
        common_headings = ["ABSTRACT", "INTRODUCTION", "METHODS", "RESULTS", "DISCUSSION", "CONCLUSION", "REFERENCES"]
        return len(text) < 100 and text.isupper() and any(h in text for h in common_headings)

    def _match_section(self, text):
        """Matches a given text to known section patterns."""
        for section, pattern in self.section_patterns.items():
            if re.search(pattern, text, re.IGNORECASE):
                return section
        return None

    def _finalize_section(self):
        """Saves the accumulated content for the current section."""
        if self.current_section and self.content_buffer:
            content = "\n".join(self.content_buffer)
            self.sections_dict.setdefault(self.current_section, []).append(content)
            self.content_buffer = []

    def is_open_access(self):
        """Detects Open Access mentions in the first three pages."""
        open_access_keywords = [
            r"open\s+access", r"creative\s+commons", r"CC\s+BY", 
            r"free\s+to\s+read", r"author\s+manuscript"
        ]

        text = ""
        with pdfplumber.open(BytesIO(self.pdf_content)) as pdf:
            for page in pdf.pages[:3]:  # Limit search to first 3 pages
                text += page.extract_text() or ""

        for keyword in open_access_keywords:
            if re.search(keyword, text, re.IGNORECASE):
                self.sections_dict["paper_type"] = f"===== PaperType =====\n{keyword}"
                return True
        return False

    def _extract_tables(self):
        """Extracts tables using pdfplumber and stores them as structured text."""
        tables = []
        with pdfplumber.open(BytesIO(self.pdf_content)) as pdf:
            for page in pdf.pages:
                extracted_tables = page.extract_tables()
                for table in extracted_tables:
                    table_text = "\n".join([" | ".join(str(cell) if cell else "" for cell in row) for row in table if any(row)])
                    tables.append(table_text)

        if tables:
            self.sections_dict["tables"] = f"===== Tables =====\n" + "\n\n".join(tables)

    def _extract_images(self):
        """Extracts text from images using OCR (Tesseract) but only includes PRISMA-relevant images."""
        image_texts = []
        images = convert_from_bytes(self.pdf_content)  # Convert PDF pages to images

        for i, image in enumerate(images):
            text = pytesseract.image_to_string(image)
            if any(keyword in text.lower() for keyword in self.prisma_image_keywords):
                image_texts.append(f"***** ImageText (Page {i + 1}) *****\n{text.strip()}")

        if image_texts:
            self.sections_dict["image_text"] = "\n\n".join(image_texts)

    def _extract_references(self):
        """Processes and formats the references section."""
        if "References" in self.sections_dict:
            ref_content = "\n".join(self.sections_dict["References"])
            self.sections_dict["references"] = f"===== References =====\n{ref_content}"

    def _combine_main_content(self):
        """Combines only the required sections into the MainContent."""
        main_content = []
        for section in self.main_content_sections:
            if section in self.sections_dict:
                content = "\n".join(self.sections_dict[section])
                main_content.append(f"===== {section} =====\n{content}")

        if main_content:
            self.sections_dict["main_content"] = "\n\n".join(main_content)

    def get_extracted_text(self):
        """Returns extracted text sections in a structured format."""
        return self.sections_dict
