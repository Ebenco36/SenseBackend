import re
from src.Services.Factories.Sections.BaseArticleExtractor import BaseArticleExtractor

class TXTUniversalExtractor(BaseArticleExtractor):
    
    # Define ordered sections for structured output
    main_content_sections = ["paper_type", "title", "tables", "introduction", "methods", "results", "discussion", "conclusion", "image_text"]

    def _extract_sections(self):
        """Extracts structured sections from a TXT file."""
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
            # "references": r"\breferences\b|\bbibliography\b",
            "tables": r"\btable\b",
            "figures": r"\bfigure\b",
        }

        full_text = self._load_text()
        self._process_text(full_text)
        self._finalize_section()

    def _load_text(self):
        """Extracts and processes text while ensuring structured output."""
        self.text_content = self._clean_extracted_text(self.text_content)
        return self.text_content

    def _clean_extracted_text(self, text):
        if not isinstance(text, str):
            print("Unexpected type:", type(text))
            return ""
        return re.sub(r'\u00A0', ' ', text)

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
        return len(text) < 100 and text.isupper()

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

    def get_extracted_text(self):
        """Returns extracted text sections in a structured format."""
        return self.sections_dict

    def save_extracted_sections(self, output_path):
        """Saves the extracted sections into a structured text file."""
        with open(output_path, "w", encoding="utf-8") as f:
            for section, content in self.sections_dict.items():
                f.write(f"===== {section} =====\n")
                f.write("\n\n".join(content))
                f.write("\n\n")
