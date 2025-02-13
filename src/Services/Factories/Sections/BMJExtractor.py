from src.Services.Factories.Sections.BaseArticleExtractor import BaseArticleExtractor
from src.Services.Factories.Sections.PrismaImageScraper import PrismaImageScraper
import re

class BMJExtractor(BaseArticleExtractor):
    def _extract_sections(self):
        """Extract sections for articles hosted on BMJ."""
        # Extract the title and ensure it's added to sections_dict
        self._extract_title()

        # Dynamically identify the main_content section
        content_div = (
            self.soup.find('div', {'class': 'article fulltext-view'}) or
            self.soup.find('article')
        )
        if not content_div:
            print("Could not find the main_content section.")
            content_div = self.soup.find('body')

        self.is_open_access()
        self._extract_image_content()
        self._extract_tables()
        self._extract_references()
        self.__extract_sections()
    
    def _extract_image_content(self):
        from src.Utils.Helpers import process_prisma_images
        extracted_content = process_prisma_images(self.soup, self.url)
        self.image_text = ""
        self.image_text = extracted_content
        self.sections_dict["ImageText"] = f"===== ImageText =====\n" + (self.image_text if self.image_text else "")
        return self.image_text
    
    def _extract_title(self):
        """Extract the title of the paper."""
        title_tag = self.soup.find('h1', {'class': 'title'}) or self.soup.find('title')
        if title_tag:
            self.sections_dict["Title"] = f"===== Title =====\n" + title_tag.get_text(strip=True)

    def is_open_access(self):
        # Look for Open Access metadata in meta tags
        open_access_meta = self.soup.find("meta", {"name": "citation_open_access", "content": "true"})
        if open_access_meta:
            self.sections_dict["paper_type"] = f"===== Paper_Type =====\n" + open_access_meta
            return open_access_meta

        # Define Open Access keywords
        open_access_keywords = [
            "open access", "free access", "gold open access", "creative commons",
            "open research", "fully open", "free full text", "oa"
        ]

        # Find the abstract section to limit search
        abstract_section = self.soup.find(["h2", "h3", "strong", "p"], string=lambda text: text and "abstract" in text.lower())

        # Get text before the abstract
        page_text = ""
        for element in self.soup.find_all(["p", "div", "span", "a"]):  # Limit to paragraph-like elements
            if abstract_section and element == abstract_section:
                break  # Stop processing when reaching the abstract
            page_text += element.get_text(separator=" ", strip=True).lower() + " "

        # Search for Open Access keywords in the extracted text
        if any(keyword in page_text for keyword in open_access_keywords):
            matched_keyword = next(keyword for keyword in open_access_keywords if keyword in page_text)
            self.sections_dict["paper_type"] = f"===== Paper_Type =====\n{matched_keyword}"
            return matched_keyword

        return False
    
    def __extract_sections(self):
        """
        Extract all sections by targeting H2 and H3 tags.
        Sections are identified by standard scientific section names such as Methods, Results, Discussion, and Conclusion.
        """
        # Define common section aliases
        section_aliases = {
            "abstract": [r"abstract"],
            "introduction": [r"introduction", r"background"],
            "methods": [r"methods?", r"materials?\s*(and|&)\s*methods?", r"study\s*design"],
            "search_strategy": [
                r"search\s*strategy",
                r"literature\s*search",
                r"data\s*sources?",
                r"search\s*terms?",
                r"database\s*search",
                r"electronic\s*databases?",
                r"search\s*methodology"
            ],
            "results": [r"results?", r"findings?", r"analysis-results?"],
            "discussion": [r"discussion", r"interpretation", r"analysis-discussion"],
            "conclusion": [r"conclusion", r"summary", r"key\s*findings?"],
            "acknowledgments": [r"acknowledgment", r"acknowledgements?", r"funding"],
            "references": [r"references?", r"bibliography"],
        }

        current_section = None
        content_buffer = {}
        abstract_subsections = {}

        for tag in self.soup.find_all(['h2', 'h3', 'h4']):
            section_name = tag.get_text(strip=True)

            # Check if this heading matches a known section
            matched_section = None
            for section, patterns in section_aliases.items():
                if any(re.search(pattern, section_name, re.IGNORECASE) for pattern in patterns):
                    matched_section = section
                    break

            # If this is a recognized section, store the previous one and start a new section
            if matched_section:
                if current_section and current_section in content_buffer:
                    self.sections_dict[current_section] = f"===== {current_section} =====\n" + "\n".join(content_buffer[current_section])

                current_section = matched_section
                content_buffer[current_section] = []
                
                # If it's the Abstract section, check for subsections inside it
                if current_section == "abstract":
                    abstract_subsections = self._extract_abstract_subsections(tag)

            # Extract all content following this heading until the next section
            next_element = tag.find_next_sibling()
            while next_element and next_element.name not in ['h2', 'h3', 'h4']:
                if next_element.name in ['p', 'div', 'section', 'ul', 'ol', 'table']:
                    if current_section:  # Ensure we have a valid section before appending
                        content_buffer[current_section].append(next_element.get_text(separator=' ', strip=True))
                next_element = next_element.find_next_sibling()

        # Save the last section with its heading
        if current_section and current_section in content_buffer:
            self.sections_dict[current_section] = f"===== {current_section} =====\n" + "\n".join(content_buffer[current_section])

        # Save abstract subsections with their own headers
        for subsection, content in abstract_subsections.items():
            self.sections_dict[subsection] = f"===== {subsection} =====\n{content}"
            
        self._combine_main_content()

    def _extract_abstract_subsections(self, abstract_tag):
        """Extract subsections inside the Abstract section."""
        abstract_subsections = {}
        current_subsection = None
        content_buffer = {}

        next_element = abstract_tag.find_next_sibling()
        while next_element and next_element.name not in ['h2']:
            if next_element.name in ['h3', 'h4', 'strong']:
                current_subsection = next_element.get_text(strip=True)
                content_buffer[current_subsection] = []
            elif next_element.name in ['p', 'div', 'section', 'ul', 'ol']:
                if current_subsection:
                    content_buffer[current_subsection].append(next_element.get_text(separator=' ', strip=True))
            next_element = next_element.find_next_sibling()

        for subsection, content in content_buffer.items():
            abstract_subsections[f"Abstract {subsection}"] = f"===== Abstract {subsection} =====\n" + "\n".join(content)

        return abstract_subsections

    def _combine_main_content(self):
        """Combine key sections into a single 'Main_Content' section."""
        main_content_sections = ["title", "paper_type", "publication_date", "abstract", "tables", "introduction", "methods", "search_strategy", "results", "discussion", "conclusion", "image_text"]
        main_content = []

        for section in main_content_sections:
            if section in self.sections_dict:
                main_content.append(self.sections_dict[section])

        if main_content:
            self.sections_dict["main_content"] = "===== Main_Content =====\n" + "\n\n".join(main_content)

    def _extract_tables(self):
        """Extract all tables from the document and store them separately."""
        tables = self.soup.find_all("table")
        table_content = []
        
        for i, table in enumerate(tables, start=1):
            rows = []
            for row in table.find_all("tr"):
                cells = [cell.get_text(strip=True) for cell in row.find_all(["th", "td"])]
                rows.append("\t".join(cells))  # Use tabs for clear column separation
            
            if rows:
                table_content.append(f"===== Table {i} =====\n" + "\n".join(rows))
        
        if table_content:
            self.sections_dict["tables"] = "\n\n".join(table_content)

    def _extract_references(self):
        """Extract references."""
        references_div = None

        for header_tag in self.soup.find_all(['h2', 'h3']):
            if 'reference' in header_tag.get_text(strip=True).lower():
                references_div = header_tag.find_next()
                break

        if references_div:
            references_text = references_div.get_text(separator='\n', strip=True)
            self.sections_dict["references"] = references_text

    def _gather_sibling_texts(self, start_tag, stop_tags):
        """Helper to gather text from siblings until a stopping tag is reached."""
        content = []
        for sibling in start_tag.find_next_siblings():
            if sibling.name in stop_tags:
                break
            if sibling.name == 'p':
                content.append(sibling.get_text(strip=True))
            elif sibling.name in ['ul', 'ol']:
                list_items = [li.get_text(strip=True) for li in sibling.find_all('li')]
                content.extend(list_items)
        return ' '.join(content)
