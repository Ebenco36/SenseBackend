import re
from bs4 import BeautifulSoup
from src.Services.Factories.Sections.BaseArticleExtractor import BaseArticleExtractor
from src.Services.Factories.Sections.PrismaImageScraper import PrismaImageScraper
from tqdm import tqdm

class UniversalExtractor(BaseArticleExtractor):
    def _extract_sections(self):
        self._extract_title()
        self.is_open_access()
        self._extract_references()
        self._extract_image_content()
        self.__extract_sections()

    def _extract_image_content(self):
        from src.Utils.Helpers import process_prisma_images
        if self.soup:
            extracted_content = process_prisma_images(self.soup, self.url)
            self.image_text = ""
            if extracted_content:
                self.image_text = extracted_content
            else:
                svg_texts = []
                for svg in self.soup.find_all("svg"):
                    texts = [text.get_text(separator=" ", strip=True) for text in svg.find_all("text")]
                    if texts:
                        svg_texts.append("\n".join(texts))
                self.image_text = "\n\n".join(svg_texts) if svg_texts else ""

            self.sections_dict["image_text"] = f"===== ImageText =====\n{self.image_text}"
            return self.image_text
        return ""

    def _extract_title(self):
        if self.soup:
            title_tag = (
                self.soup.find('h1', {'class': 'title'}) or
                self.soup.find('title') or
                self.soup.find('h1', {'property': 'name'}) or
                self.soup.find("h1", {'class': 'citation__title'})
            )
            if title_tag:
                self.sections_dict["title"] = f"===== Title =====\n" + title_tag.get_text(strip=True)
            else:
                title_tag = self.soup.find('meta', {'name': 'citation_title'}) or self.soup.find('title')
                if title_tag:
                    self.sections_dict["title"] = f"===== Title =====\n" + title_tag.get("content", "").strip()

    def is_open_access(self):
        if self.soup:
            open_access_meta = self.soup.find("meta", {"name": "citation_open_access", "content": "true"})
            if open_access_meta:
                self.sections_dict["paper_type"] = f"===== Paper_Type =====\n" + str(open_access_meta)
                return open_access_meta

            open_access_keywords = [
                "open access", "free access", "gold open access", "creative commons",
                "open research", "fully open", "free full text", "oa"
            ]

            abstract_section = self.soup.find(["h2", "h3", "strong", "p"], string=lambda text: text and "abstract" in text.lower())
            page_text = ""
            for element in self.soup.find_all(["p", "div", "span", "a"]):
                if abstract_section and element == abstract_section:
                    break
                page_text += element.get_text(separator=" ", strip=True).lower() + " "

            if any(keyword in page_text for keyword in open_access_keywords):
                matched_keyword = next(k for k in open_access_keywords if k in page_text)
                self.sections_dict["paper_type"] = f"===== Paper_Type =====\n{matched_keyword}"
                return matched_keyword
        return False

    def _remove_section_numbering(self, heading: str) -> str:
        return re.sub(r"^\s*\(?[0-9IVXivxA-Za-z]+(\.[0-9A-Za-z]+)*[\)\.]?\s*[-–—]?\s*", "", heading).strip()

    def _extract_tables(self):
        tables = self.soup.find_all("table")
        table_content = []
        for i, table in enumerate(tables, start=1):
            rows = []
            for row in table.find_all("tr"):
                cells = [cell.get_text(strip=True) for cell in row.find_all(["th", "td"])]
                rows.append("\t".join(cells))
            if rows:
                table_content.append(f"===== Table {i} =====\n" + "\n".join(rows))
        if table_content:
            self.sections_dict["tables"] = "\n\n".join(table_content)

    def __extract_sections(self):
        # 🆕 Remove inline section numbering like "3.1 Study..."
        self.soup = BeautifulSoup(self.remove_section_numbering_from_html(str(self.soup)), "html.parser")

        section_aliases = {
            "abstract": [r"abstract"],
            "introduction": [r"introduction", r"background"],
            "methods": [r"methods?", r"materials?\s*(and|&)\s*methods?", r"study\s*design"],
            "search_strategy": [
                r"search\s*methods", r"study\s*selection", r"search\s*strategy",
                r"literature\s*search", r"retrieval*strategy\s*and\s*selection\s*criteria",
                r"literature*search*strategy\s*and\s*study*selection", r"data\s*sources?",
                r"search\s*terms?", r"database\s*search", r"electronic\s*databases?",
                r"search\s*methodology", r"search\s*and\s*selection\s*process",
                r"search\s*and\s*study\s*selection"
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

        if self.soup:
            for tag in self.soup.find_all(['h2', 'h3', 'h4']):
                raw_section_name = tag.get_text(strip=True)
                section_name = self._remove_section_numbering(raw_section_name)

                matched_section = None
                for section, patterns in section_aliases.items():
                    if any(re.search(pattern, section_name, re.IGNORECASE) for pattern in patterns):
                        matched_section = section
                        break

                if matched_section:
                    if current_section and current_section in content_buffer:
                        self.sections_dict[current_section] = f"===== {current_section} =====\n" + "\n".join(content_buffer[current_section])
                    current_section = matched_section
                    content_buffer[current_section] = []

                    if current_section == "abstract":
                        abstract_subsections = self._extract_abstract_subsections(tag)

                next_element = tag.find_next_sibling()
                while next_element and next_element.name not in ['h2', 'h3', 'h4']:
                    if next_element.name in ['p', 'div', 'section', 'ul', 'ol', 'table']:
                        if current_section:
                            content_buffer[current_section].append(next_element.get_text(separator=' ', strip=True))
                    next_element = next_element.find_next_sibling()

            if current_section and current_section in content_buffer:
                self.sections_dict[current_section] = f"===== {current_section} =====\n" + "\n".join(content_buffer[current_section])

            for subsection, content in abstract_subsections.items():
                self.sections_dict[subsection] = f"===== {subsection} =====\n{content}"

            self._combine_main_content()

    def _extract_abstract_subsections(self, abstract_tag):
        abstract_subsections = {}
        current_subsection = None
        content_buffer = {}
        next_element = abstract_tag.find_next_sibling()
        while next_element and next_element.name not in ['h2']:
            if next_element.name in ['h3', 'h4', 'strong']:
                raw_subsection = next_element.get_text(strip=True)
                current_subsection = self._remove_section_numbering(raw_subsection)
                content_buffer[current_subsection] = []
            elif next_element.name in ['p', 'div', 'section', 'ul', 'ol']:
                if current_subsection:
                    content_buffer[current_subsection].append(next_element.get_text(separator=' ', strip=True))
            next_element = next_element.find_next_sibling()

        for subsection, content in content_buffer.items():
            abstract_subsections[f"Abstract {subsection}"] = f"===== Abstract {subsection} =====\n" + "\n".join(content)

        return abstract_subsections

    def _combine_main_content(self):
        main_sections = ["paper_type", "title", "abstract", "tables", "introduction", "methods", "search_strategy", "results", "discussion", "conclusion", "image_text"]
        combined = [self.sections_dict[sec] for sec in main_sections if sec in self.sections_dict]
        if combined:
            full_text = "===== Main_Content =====\n" + "\n\n".join(combined)
            self.sections_dict["main_content"] = full_text
     
    def _extract_references(self):
        """Extract references."""
        references_div = None
        if self.soup:
            for header_tag in self.soup.find_all(['h2', 'h3']):
                if 'reference' in header_tag.get_text(strip=True).lower():
                    references_div = header_tag.find_next()
                    break
            if references_div:
                references_text = references_div.get_text(separator='\n', strip=True)
                self.sections_dict["references"] = references_text
