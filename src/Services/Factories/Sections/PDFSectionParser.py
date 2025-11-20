import fitz  # PyMuPDF
import re
from collections import Counter
from typing import Union

# The PDFSectionParser class already handles both path and content.
class PDFSectionParser:
    """
    Parses a PDF from a file path or an in-memory byte stream
    into standardized, structured sections.
    """
    def __init__(self, pdf_source: Union[str, bytes]):
        self.pdf_source = pdf_source
        try:
            if isinstance(pdf_source, str):
                self.doc = fitz.open(pdf_source)
            elif isinstance(pdf_source, bytes):
                self.doc = fitz.open(stream=pdf_source, filetype="pdf")
            else:
                raise TypeError("PDF source must be a file path (str) or a byte stream (bytes).")
        except Exception as e:
            raise IOError(f"Failed to open PDF source: {e}")
        self.sections = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.doc.close()

    def __str__(self):
        source_type = "path" if isinstance(self.pdf_source, str) else "stream"
        titles = ", ".join(self.sections.keys()) if self.sections else "unparsed"
        return f"PDFSectionParser(source_type='{source_type}', sections=[{titles}])"

    def _get_most_common_font_size(self) -> float:
        font_counts = Counter()
        for page in self.doc:
            for block in page.get_text("dict").get("blocks", []):
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        font_counts[round(span["size"], 2)] += 1
        if not font_counts: return 0.0
        return font_counts.most_common(1)[0][0]

    def parse(self) -> dict:
        section_map = {
            "Abstract": ["abstract"], "Introduction": ["introduction", "background"],
            "Methods": ["methods", "method", "methodology", "materials and methods"],
            "Results": ["results", "findings"], "Discussion": ["discussion"],
            "Conclusion": ["conclusions", "conclusion"], 
            # "References": ["references", "bibliography"],
        }
        reverse_map = {kw: key for key, kws in section_map.items() for kw in kws}
        pattern = re.compile(r"^(?:[A-Z0-9]+\.?\s*)?(" + "|".join(reverse_map.keys()) + r")\s*$", re.IGNORECASE)
        body_font_size = self._get_most_common_font_size()
        parsed_sections, current_section = {"Header": []}, "Header"
        for page in self.doc:
            for block in page.get_text("dict").get("blocks", []):
                for line in block.get("lines", []):
                    if not line.get("spans"): continue
                    text = " ".join([s["text"] for s in line["spans"]]).strip()
                    span = line["spans"][0]
                    match = pattern.match(text)
                    if match and span["size"] > body_font_size:
                        current_section = reverse_map.get(match.group(1).lower(), "Unknown")
                        parsed_sections.setdefault(current_section, [])
                    else:
                        parsed_sections[current_section].append(text)
        self.sections = {title: "\n".join(lines) for title, lines in parsed_sections.items() if lines}
        if "References" in self.sections:
            del self.sections["References"]
        return self.sections

def parse_and_print_sections(pdf_source: Union[str, bytes]):
    """
    Parses a PDF from a file path or byte stream, extracts structured
    sections, and prints the results.

    Args:
        pdf_source (Union[str, bytes]): The path to the PDF file or its raw byte content.
    """
    formatted_text = ""
    try:
        with PDFSectionParser(pdf_source) as parser:
            if isinstance(pdf_source, str):
                print(f"====== Parsing Document from path: {parser.pdf_source} ======\n")
            else:
                print("====== Parsing Document from in-memory content ======\n")

            extracted_data = parser.parse()
            for section_title, content in extracted_data.items():
                formatted_text += f"====== {section_title.replace('_', ' ')} ======\n"
                formatted_text += f"{content}\n\n"
            
        return formatted_text
    except (IOError, FileNotFoundError) as e:
        print(f"Error processing PDF source: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")