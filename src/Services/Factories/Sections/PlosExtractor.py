from bs4 import BeautifulSoup
from src.Services.Factories.Sections.BaseArticleExtractor import BaseArticleExtractor
import re

class PlosExtractor(BaseArticleExtractor):
    def _extract_sections(self):
        self._extract_title()
        # self._extract_abstract()
        self._extract_image_content()
        self._extract_body_sections()
        # self._extract_references()
        self._combine_main_content()

    def _extract_title(self):
        title_tag = self.soup.find("h1", id="artTitle")
        if title_tag:
            title = title_tag.get_text(strip=True)
            self.sections_dict["title"] = f"===== Title =====\n{title}"

    def _extract_abstract(self):
        if not self.soup:
            return

        abstract_text = []
        intro_header = self.soup.find(lambda tag: tag.name in ["h2", "h3"] and "introduction" in tag.get_text(strip=True).lower())
        if intro_header:
            for elem in reversed(list(intro_header.find_all_previous())):
                if elem.name == "div" and "articleinfo" in elem.get("class", []):
                    continue  # Skip metadata block
                if elem.name == "p":
                    abstract_text.insert(0, elem.get_text(strip=True))
                elif elem.name in ["h2", "h3"]:
                    break
        if abstract_text:
            self.sections_dict["abstract"] = f"===== Abstract =====\n" + "\n".join(abstract_text)

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

    def _extract_body_sections(self):
        section_patterns = {
            "abstract": ["abstract"],
            "introduction": ["introduction"],
            "methods": ["^methods?$", "materials? and methods?", "study design"],
            "search_strategy": [
                "search strategy", "search methods", "study selection",
                "search and selection", "search strategy, selection, assessment"
            ],
            "results": ["^results?$", "findings?"],
            "discussion": ["^discussion$"],
            "conclusion": ["^conclusion$", "summary"],
        }
        # {
        #     "abstract": ["abstract"],
        #     "introduction": ["introduction"],
        #     "methods": ["methods?", "materials? and methods?", "study design"],
        #     "search_strategy": ["search strategy?", "search methods", "study selection"],
        #     "results": ["results?", "findings?"],
        #     "discussion": ["discussion"],
        #     "conclusion": ["conclusion", "summary"],
        # }

        headers = self.soup.find_all(["h2", "h3"])
        current_section = None
        content_buffer = {}

        for i, header in enumerate(headers):
            section_text = header.get_text(strip=True).lower()
            matched_key = None
            for key, patterns in section_patterns.items():
                if any(re.search(pat, section_text, re.IGNORECASE) for pat in patterns):
                    matched_key = key
                    break
            if not matched_key:
                continue

            current_section = matched_key
            content_buffer[current_section] = []

            sibling = header.find_next_sibling()
            while sibling and sibling.name not in ["h2", "h3"]:
                if sibling.name in ["p", "div"]:
                    content_buffer[current_section].append(sibling.get_text(strip=True))
                sibling = sibling.find_next_sibling()

        for section, contents in content_buffer.items():
            if contents:
                self.sections_dict[section] = f"===== {section.capitalize()} =====\n" + "\n".join(contents)

    def _extract_references(self):
        references = self.soup.find_all("li", id=re.compile("^ref"))
        ref_texts = [li.get_text(strip=True) for li in references]
        if ref_texts:
            self.sections_dict["references"] = f"===== References =====\n" + "\n".join(ref_texts)

    def _combine_main_content(self):
        keys = [
            "title", "abstract", "introduction", "methods", "search_strategy",
            "results", "image_text", "discussion", "conclusion"
        ]
        combined = [self.sections_dict[k] for k in keys if k in self.sections_dict]
        if combined:
            self.sections_dict["main_content"] = "===== Main_Content =====\n" + "\n\n".join(combined)