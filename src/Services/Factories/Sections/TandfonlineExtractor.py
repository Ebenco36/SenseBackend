from bs4 import BeautifulSoup
import re
import requests
from PIL import Image
from io import BytesIO
import pytesseract
from urllib.parse import urljoin
import cloudscraper
from src.Services.Factories.Sections.BaseArticleExtractor import BaseArticleExtractor

class TandfonlineExtractor(BaseArticleExtractor):
    def _extract_sections(self):
        self.section_order = []  # Track order of canonical keys
        self._extract_title()
        self._extract_abstract()
        self._extract_all_sections()
        self._extract_keywords()
        self._extract_prisma_images()
        self._extract_fallback_sections()
        self._combine_main_content()

    def _extract_title(self):
        title_tag = self.soup.find('h1')
        if title_tag:
            text = title_tag.get_text(strip=True)
            self.sections_dict["title"] = f"===== Title =====\n{text}"
            self.sections_dict["paper_title"] = f"===== Paper Title =====\n{text}"
            self.section_order.insert(0, "paper_title")  # Ensure it's the first

    def _extract_abstract(self):
        abstract_div = self.soup.find('div', class_='hlFld-Abstract')
        if abstract_div:
            paragraphs = abstract_div.find_all('p')
            if paragraphs:
                text = "\n".join(p.get_text(strip=True) for p in paragraphs)
                self.sections_dict["abstract"] = f"===== Abstract =====\n{text}"
                self.section_order.append("abstract")

    def _extract_keywords(self):
        keyword_ul = self.soup.select_one('div.abstractKeywords ul')
        if keyword_ul:
            keywords = [li.get_text(strip=True) for li in keyword_ul.find_all('li')]
            if keywords:
                self.sections_dict["keywords"] = f"===== Keywords =====\n{', '.join(keywords)}"
                self.section_order.append("keywords")

    def _extract_all_sections(self):
        sections = self.soup.find_all('div', class_='NLM_sec')
        known_labels = [
            "introduction", "background", "methods", "materials_and_methods",
            "results", "discussion", "conclusion", "conclusions",
            "conflict_of_interest", "acknowledgements", "image_text"
        ]

        for sec in sections:
            heading = sec.find(['h2', 'h3'], class_=re.compile(r'section-heading'))
            if heading:
                heading_text = heading.get_text(strip=True)
                key = heading_text.lower().strip()
                key_norm = re.sub(r"[^a-z0-9]+", "_", key)
                canonical_key = next((label for label in known_labels if label in key_norm), key_norm)

                paragraphs = sec.find_all('p')
                content = "\n".join(p.get_text(separator=" ", strip=True) for p in paragraphs)

                if content:
                    if canonical_key not in self.sections_dict:
                        self.sections_dict[canonical_key] = f"===== {heading_text} =====\n{content}"
                        self.section_order.append(canonical_key)
                    else:
                        # Avoid repeated section headers
                        self.sections_dict[canonical_key] += f"\n\n{content}"

    def _extract_fallback_sections(self):
        known_labels = [
            "abstract", "introduction", "background", "methods", "materials_and_methods",
            "results", "discussion", "conclusion", "conclusions", "conflict_of_interest",
            "acknowledgements", "funding", "ethics", "ethical_approval", "author_contributions",
            "supplementary", "image_text"
        ]

        intro_seen = 'introduction' in self.section_order
        results_seen = 'results' in self.section_order
        method_seen = any(k in self.sections_dict for k in ['methods', 'materials_and_methods'])

        for tag in self.soup.find_all(['h2', 'h3', 'strong']):
            heading_text = tag.get_text(strip=True)
            if not heading_text or len(heading_text) > 100:
                continue

            sibling = tag.find_next_sibling()
            contents = []
            while sibling and sibling.name not in ['h2', 'h3', 'strong']:
                if sibling.name in ['p', 'div']:
                    text = sibling.get_text(" ", strip=True)
                    if text:
                        contents.append(text)
                sibling = sibling.find_next_sibling()

            if contents:
                key_norm = re.sub(r"[^a-z0-9]+", "_", heading_text.lower())
                canonical_key = next((label for label in known_labels if label in key_norm), None)
                section_text = f"===== {heading_text} =====\n" + "\n".join(contents)

                if canonical_key and canonical_key not in self.sections_dict:
                    self.sections_dict[canonical_key] = section_text
                    self.section_order.append(canonical_key)
                else:
                    destination = "methods" if intro_seen and not method_seen else key_norm
                    if destination not in self.sections_dict:
                        self.sections_dict[destination] = section_text
                        self.section_order.append(destination)
                    else:
                        self.sections_dict[destination] += "\n\n" + section_text

    def _extract_prisma_images(self):
        figures = self.soup.find_all('div', class_='figureView')
        matched_images = []

        # Use CloudScraper instead of requests
        scraper = cloudscraper.create_scraper()

        for idx, fig in enumerate(figures):
            caption = fig.find('p', class_='captionText') or fig.find('p')
            caption_text = caption.get_text(strip=True) if caption else ''

            if 'prisma' in caption_text.lower() or 'flow chart' in caption_text.lower():
                img = fig.find('img')
                if img and img.get('src'):
                    img_url = urljoin(self.url, img['src'])

                    try:
                        response = scraper.get(img_url, timeout=10)
                        response.raise_for_status()

                        image = Image.open(BytesIO(response.content)).convert("RGB")
                        ocr_text = pytesseract.image_to_string(image)

                    except Exception as e:
                        ocr_text = f"Error extracting OCR text: {str(e)}"

                    matched_images.append(
                        f"***** PRISMA Image {idx+1} *****\nCaption: {caption_text}\nURL: {img_url}\nOCR Text:\n{ocr_text}\n"
                    )

        if matched_images:
            self.sections_dict["image_text"] = f"===== ImageText =====\n" + "\n\n".join(matched_images)
            self.section_order.append("image_text")

    def _combine_main_content(self):
        priority_order = [
            "paper_title", "title", "abstract", "introduction", "background", "methods",
            "materials_and_methods", "results", "discussion", "conclusion",
            "conclusions", "conflict_of_interest", "acknowledgements",
            "keywords", "image_text"
        ]
        combined = [self.sections_dict[k] for k in priority_order if k in self.sections_dict]
        if combined:
            full_text = "\n\n".join(combined)
            self.sections_dict["main_content"] = "===== Main_Content =====\n" + full_text
