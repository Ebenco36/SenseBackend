from bs4 import BeautifulSoup
import re
import requests
from PIL import Image
from io import BytesIO
import pytesseract
from src.Services.Factories.Sections.BaseArticleExtractor import BaseArticleExtractor

class ScienceDirectExtractor(BaseArticleExtractor):
    def _extract_sections(self):
        self._extract_title()
        self._extract_abstract()
        self._extract_all_sections()
        # self._extract_references()
        self._extract_keywords()
        self._extract_prisma_images()
        self._combine_main_content()

    def _extract_title(self):
        if self.soup:
            title_tag = self.soup.find('h1', {'id': 'screen-reader-main-title'})
            if title_tag:
                title_text = title_tag.find('span', class_='title-text')
                if title_text:
                    self.sections_dict["title"] = f"===== Title =====\n{title_text.get_text(strip=True)}"

    def _extract_abstract(self):
        abstract_section = self.soup.find('div', id='abstracts')
        if abstract_section:
            all_abstract_parts = []
            for div in abstract_section.find_all(['div', 'section'], class_='u-margin-s-bottom'):
                all_abstract_parts.append(div.get_text(separator=' ', strip=True))
            if all_abstract_parts:
                self.sections_dict["abstract"] = f"===== Abstract =====\n" + "\n".join(all_abstract_parts)

    def _extract_all_sections(self):
        sections = self.soup.find_all('section')
        known_labels = [
            "introduction", "background", "methods", "materials_and_methods",
            "results", "discussion", "conclusion", "conclusions",
            "conflict_of_interest", "acknowledgements", "image_text"
        ]

        for sec in sections:
            heading = sec.find(['h2', 'h3'])
            if heading:
                heading_text = heading.get_text(strip=True)
                key = heading_text.lower().strip()
                key_norm = re.sub(r"[^a-z0-9]+", "_", key)

                canonical_key = next((label for label in known_labels if label in key_norm), key_norm)

                paragraphs = sec.find_all(['div', 'p'], class_='u-margin-s-bottom')
                if not paragraphs:
                    paragraphs = sec.find_all('p')

                content = "\n".join(p.get_text(separator=" ", strip=True) for p in paragraphs)
                content = re.sub(r"\s*\[(?:\d+,?\s*)+\]", "", content)

                if content:
                    self.sections_dict[canonical_key] = f"===== {heading_text} =====\n{content}"

    def _extract_references(self):
        references_section = self.soup.find('div', id='preview-section-references')
        if references_section:
            references = []
            for item in references_section.find_all('li', class_='bib-reference'):
                title_tag = item.find('h3')
                title = title_tag.get_text(strip=True) if title_tag else ''
                refs = item.get_text(separator=' ', strip=True)
                references.append(f"{title}\n{refs}")
            if references:
                self.sections_dict["references"] = f"===== References =====\n" + "\n\n".join(references)

    def _extract_keywords(self):
        keyword_sections = self.soup.find_all('div', class_='keywords-section')
        all_keywords = []
        for sec in keyword_sections:
            for div in sec.find_all('div', class_='keyword'):
                kw = div.get_text(strip=True)
                if kw:
                    all_keywords.append(kw)
        if all_keywords:
            self.sections_dict["keywords"] = f"===== Keywords =====\n" + ", ".join(all_keywords)

    def _extract_prisma_images(self):
        figures = self.soup.find_all('figure')
        matched_images = []

        for idx, fig in enumerate(figures):
            caption_tag = fig.find(['figcaption', 'div'], class_=re.compile(r'(caption|label)', re.IGNORECASE))
            caption_text = caption_tag.get_text(strip=True) if caption_tag else ''

            if 'prisma' in caption_text.lower() or 'flow diagram' in caption_text.lower():
                img = fig.find('img')
                if img and img.get('src'):
                    img_url = img['src']
                    try:
                        response = requests.get(img_url)
                        image = Image.open(BytesIO(response.content))
                        ocr_text = pytesseract.image_to_string(image)
                    except Exception as e:
                        ocr_text = f"Error extracting OCR text: {str(e)}"

                    matched_images.append(
                        f"***** PRISMA Image {idx+1} *****\nCaption: {caption_text}\nURL: {img_url}\nOCR Text:\n{ocr_text}\n"
                    )

        if matched_images:
            self.sections_dict["image_text"] = f"===== ImageText =====\n" + "\n\n".join(matched_images)

    def _combine_main_content(self):
        priority_order = [
            "title", "abstract", "introduction", "background", "methods",
            "materials_and_methods", "results", "discussion", "conclusion",
            "conclusions", "conflict_of_interest", "acknowledgements",
            "keywords", "image_text", "references"
        ]
        combined = [self.sections_dict[k] for k in priority_order if k in self.sections_dict]
        if combined:
            full_text = "\n\n".join(combined)
            self.sections_dict["main_content"] = "===== Main_Content =====\n" + full_text
