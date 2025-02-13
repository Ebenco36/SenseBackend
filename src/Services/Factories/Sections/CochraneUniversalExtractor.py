from src.Services.Factories.Sections.BaseArticleExtractor import BaseArticleExtractor
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm  # Import tqdm for a progress bar



class CochraneUniversalExtractor(BaseArticleExtractor):

    def _extract_sections(self):
        """Extract all key sections from Cochrane articles."""
        self._extract_title()
        self._extract_publication_date()
        self._extract_abstract()
        self._extract_section(
            "introduction", ["pico-section", "pls", "vls", "background", "objectives"]
        )
        self._extract_section("methods", ["methods"])
        self._extract_section("paper_type", [
            "open access", "free access", "free", 
            "gold open access", "creative commons",
            "open research", "fully open", "free full text", "oa"
        ])
        self._extract_section("search_strategy", ["Search methods"])
        self._extract_section("results", ["results", "summaryOfFindings"])
        self._extract_section("discussion", ["discussion"])
        self._extract_section("conclusion", ["conclusions"])
        self._extract_images()
        self._extract_tables()
        self._combine_main_content()

    def _extract_title(self):
        """Extracts the title from the HTML."""
        title_tag = self.soup.find("h1", class_="publication-title")
        if title_tag:
            self.sections_dict["title"] = "===== Title =====\n" + title_tag.get_text(
                strip=True
            )
    
    def _extract_publication_date(self):
        """Extracts and formats the publication date."""
        date_tag = self.soup.find("span", class_="publish-date")
        if date_tag:
            raw_date = date_tag.get_text(strip=True)
            extracted_date = re.search(r"\d{1,2} \w+ \d{4}", raw_date)
            if extracted_date:
                raw_date = extracted_date.group()
            try:
                formatted_date = datetime.strptime(raw_date, "%d %B %Y").strftime(
                    "%Y-%m-%d"
                )
            except ValueError:
                formatted_date = raw_date  # Keep original if formatting fails
            self.sections_dict["publication_date"] = (
                "===== PublicationDate =====\n" + formatted_date
            )

    def _extract_abstract(self):
        """Extracts the abstract section."""
        abstract_tag = self.soup.find("section", class_="abstract")
        if abstract_tag:
            self.sections_dict["abstract"] = (
                "===== Abstract =====\n"
                + abstract_tag.get_text(separator=" ", strip=True)
            )

    def _extract_section(self, section_name, class_list):
        """Extracts a section based on provided class names."""
        content = []
        for class_name in class_list:
            section = self.soup.find("section", class_=class_name) or self.soup.find(
                "div", class_=class_name
            )
            if section:
                content.append(section.get_text(separator=" ", strip=True))
        if content:
            self.sections_dict[section_name] = (
                f"===== {section_name} =====\n" + "\n\n".join(content)
            )

    def _is_valid_image_extension(self, image_url):
        """Checks if the image URL has a valid image extension."""
        valid_extensions = {".png", ".jpg", ".jpeg", ".gif"}
        return any(image_url.endswith(ext) for ext in valid_extensions)

    def _extract_images(self):
        """Extracts image URLs, SVG text content, and alternative text as titles."""
        from src.Utils.Helpers import process_prisma_images

        figures = self.soup.find("div", class_="figures-list")
        images = []
        self.base_url = "https://www.cochranelibrary.com/"

        if figures:
            img_elements = figures.find_all("img")  # Get all image elements
            total_images = len(img_elements)

            print(f"Extracting {total_images} images...")  # Loader message

            for img in tqdm(img_elements, desc="Processing Images", unit="img"):  # Progress bar
                img_url = img.get("src")
                img_title = img.get("alt", "Untitled Image")
                if img_url:
                    img_full_url = f"{self.base_url}{img_url}"
                    if self._is_valid_image_extension(img_url):
                        extracted_content = process_prisma_images(self.soup, img_full_url)
                        images.append(
                            f"***** {img_title} *****\nImage URL: {img_full_url}\nExtracted Content: {extracted_content}"
                        )
                    else:
                        extracted_text = (
                            self._extract_svg_text(img_full_url) if img_full_url else ""
                        )
                        if extracted_text:
                            images.append(
                                f"***** {img_title} *****\nSVG Image Text: {extracted_text}"
                            )

        if images:
            self.sections_dict["image_text"] = "===== ImageText =====\n" + "\n\n".join(images)

        print("Image extraction completed!")  # Completion message

    def _extract_svg_text(self, svg_url):
        """Fetches and extracts text from an SVG image."""
        try:
            response = requests.get(svg_url)
            if response.status_code == 200:
                svg_content = response.text
                soup = BeautifulSoup(svg_content, "xml")
                svg_texts = [
                    text.get_text(separator=" ", strip=True)
                    for text in soup.find_all("text")
                ]
                return "\n".join(svg_texts) if svg_texts else ""
        except Exception as e:
            print(f"Failed to fetch or parse SVG: {e}")
        return ""

    def _extract_tables(self):
        """Extracts tables along with their titles."""
        tables_div = self.soup.find("div", class_="tables-list")
        tables = []
        if tables_div:
            for table in tables_div.find_all("table"):
                # Try to get the title from the table-heading div
                title_tag = table.find_previous("h2") or table.find_previous("h3")
                table_heading = table.find("div", class_="table-heading")
                table_label = (
                    table_heading.find("span", class_="table-label")
                    if table_heading
                    else None
                )
                table_title_span = (
                    table_heading.find("span", class_="table-title")
                    if table_heading
                    else None
                )
                table_title = (
                    table_title_span.get_text(strip=True)
                    if table_title_span
                    else (
                        table_label.get_text(strip=True)
                        if table_label
                        else (
                            title_tag.get_text(strip=True)
                            if title_tag
                            else "Untitled Table"
                        )
                    )
                )

                # Extract rows from the table
                rows = [
                    "\t".join(
                        cell.get_text(strip=True) for cell in row.find_all(["th", "td"])
                    )
                    for row in table.find_all("tr")
                ]

                if rows:
                    tables.append(f"***** {table_title} *****\n" + "\n".join(rows))

        if tables:
            self.sections_dict["tables"] = "===== Tables =====\n" + "\n\n".join(tables)

    def _combine_main_content(self):
        """Combine key sections into a single 'Main_Content' section."""
        main_content_sections = [
            "title",
            "publication_date",
            "paper_type",
            "image_text",
            "tables",
            "abstract",
            "introduction",
            "methods",
            "search_strategy",
            "results",
            "conclusion",
            "discussion",
        ]
        main_content = []

        for section in main_content_sections:
            if section in self.sections_dict:
                main_content.append(self.sections_dict[section])

        if main_content:
            self.sections_dict["main_content"] = (
                "===== Main_Content =====\n" + "\n\n".join(main_content)
            )

    def get_extracted_text(self):
        """Returns extracted text sections in a structured format."""
        return self.sections_dict
