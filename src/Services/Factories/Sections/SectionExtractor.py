import re

class SectionExtractor:
    """
    Extracts and stores sections from a document based on predefined headers.
    Allows retrieval using .get("Section Name"), case-insensitive.
    """

    def __init__(self, text):
        """
        Initializes the extractor and processes the text.
        
        Args:
            text (str): The full document as a string.
        """
        self.original_text = text.strip()
        self.sections = self._extract_sections(text)

    def _extract_sections(self, text):
        """
        Extracts sections from the document.

        Args:
            text (str): The full document as a string.

        Returns:
            dict: A dictionary with lowercase section names as keys and their respective content as values.
        """
        section_pattern = re.compile(r"===== (.*?) =====")  # Match section headers
        matches = list(section_pattern.finditer(text))

        sections = {}

        for i, match in enumerate(matches):
            section_name = match.group(1).strip().lower()  # Store section names in lowercase
            start_pos = match.end()  # Start after the header

            if i + 1 < len(matches):
                end_pos = matches[i + 1].start()  # End before the next header
            else:
                end_pos = len(text)  # Last section goes till the end

            section_content = text[start_pos:end_pos].strip()
            sections[section_name] = section_content

        return sections

    def get(self, section_name, default="Section not found"):
        """
        Retrieves the content of a given section (case-insensitive).

        Args:
            section_name (str): The name of the section to retrieve.
            default (str, optional): Default value if section not found.

        Returns:
            str: The content of the section or default value.
        """
        if section_name.lower() == "main_content":
            return self.original_text  # Return the full document if "MainContent" is requested
        return self.sections.get(section_name.lower(), default)

    def available_sections(self):
        """
        Returns a list of available sections.

        Returns:
            list: List of section names.
        """
        return list(self.sections.keys())