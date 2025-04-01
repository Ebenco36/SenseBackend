from bs4 import BeautifulSoup
from src.Services.Factories.Sections.BaseArticleExtractor import BaseArticleExtractor
import re
import pytesseract
import requests
from PIL import Image
from io import BytesIO

class SagePubExtractor(BaseArticleExtractor):
    def _extract_sections(self):
        """Extract sections for SagePub articles."""
        # 🆕 Remove inline section numbering like "3.1 Study..."
        self.soup = BeautifulSoup(self.remove_section_numbering_from_html(str(self.soup)), "html.parser")
        self._extract_title()
        self._extract_abstract()
        self._extract_introduction()
        self._extract_methods()
        self._extract_results()
        self._extract_discussion()
        self._extract_conclusions()
        self.extract_image_text()
        self.extract_tables_with_titles()
        self._extract_references()
        self._extract_search_strategy()
        self._combine_main_content()

    def _extract_title(self):
        """Extracts the article title."""
        title_tag = self.soup.find('h1', {'class': 'title'}) or \
                    self.soup.find('meta', {'name': 'dc.Title'}) or \
                    self.soup.find('h1', {"property": "name"}) or \
                    self.soup.find('title')
        
        if title_tag:
            title_text = title_tag.get_text(strip=True) if title_tag.name != 'meta' else title_tag.get('content', '').strip()
            self.sections_dict["title"] = f"===== Title =====\n" + title_text
        else:
            print("⚠️ Warning: Title not found.")

    def _extract_search_strategy(self):
        """Extracts the Search Strategy section from the article."""
        search_patterns = [
            r"search\s*strategy",
            r"literature\s*search",
            r"retrieval*strategy\s*and\s*selection\s*criteria",
            r"search\s*methods?",
            r"search\s*terms?",
            r"database\s*search",
            r"electronic\s*databases?",
            r"search\s*methodology",
            r"search\s*and\s*selection\s*process"
        ]

        for section in self.soup.find_all("section"):
            heading = section.find(["h2", "h3", "h4", "h5", "h6"])
            if heading and any(re.search(pattern, heading.text, re.IGNORECASE) for pattern in search_patterns):
                self.sections_dict["search_strategy"] = f"===== Search Strategy =====\n" + section.get_text(separator=" ", strip=True)
                return

    def _extract_abstract(self):
        """Extracts the abstract from content divs."""
        # First, try to find the abstract within a 'section' or 'div' with a specific 'role' attribute
        abstract_tag = self.soup.find('section', {'id': 'abstract'}) or \
                    self.soup.find('div', {'role': 'paragraph'}) or \
                    self.soup.find('div', {'property': 'abstract'})

        if abstract_tag:
            # Try to get the abstract text, checking if the tag has text or 'content' attribute
            abstract_text = abstract_tag.get_text(strip=True) if abstract_tag.name != 'meta' else abstract_tag.get('content', '').strip()

            # Check if abstract text is not empty, and then store it
            if abstract_text:
                self.sections_dict["abstract"] = f"===== Abstract =====\n" + abstract_text
            else:
                print("⚠️ Warning: Abstract content found but is empty.")
        else:
            print("⚠️ Warning: Abstract not found.")
            
    def _extract_introduction(self):
        """Extracts the full introduction (Background) section content from the article."""
        # Find the section with the specific ID that contains the introduction (Background)
        intro_section = self.soup.find('section', {'id': 'sec-1'})
        
        if intro_section:
            # Extract all paragraph content within the section
            paragraphs = intro_section.find_all('div', {'role': 'paragraph'})
            
            # Combine all the paragraph text into one large introduction text
            introduction_text = "\n".join([para.get_text(strip=True) for para in paragraphs])
            
            # Store the introduction content if any is found
            if introduction_text:
                self.sections_dict["introduction"] = f"===== Introduction =====\n" + introduction_text
            else:
                print("⚠️ Warning: Introduction content found but is empty.")
        else:
            print("⚠️ Warning: Introduction (Background) section not found.")

    def _extract_methods(self):
        """Extracts the full Methods section, including all sub-sections, from the article."""
        # Find the section with the specific ID that contains the methods
        methods_section = self.soup.find('section', {'id': 'sec-2'})
        
        if methods_section:
            # Initialize the methods content
            methods_content = ""
            
            # Extract the main methods paragraph
            main_paragraphs = methods_section.find_all('div', {'role': 'paragraph'})
            methods_content += "\n".join([para.get_text(strip=True) for para in main_paragraphs])
            
            # Now extract the sub-sections like "Search Strategy", "Inclusion and Exclusion Criteria", etc.
            sub_sections = methods_section.find_all('section', {'id': lambda x: x and x.startswith('sec-2-')})
            
            for sub_section in sub_sections:
                sub_section_title = sub_section.find('h3')  # Get the sub-section title (like "Search Strategy")
                sub_section_paragraphs = sub_section.find_all('div', {'role': 'paragraph'})  # Get the paragraph content
                
                if sub_section_title and sub_section_paragraphs:
                    # Add the sub-section title and content to the methods content
                    methods_content += f"\n\n{str(sub_section_title)}"  # Adding the title (e.g., Search Strategy)
                    methods_content += "\n".join([para.get_text(strip=True) for para in sub_section_paragraphs])
            
            # Store the methods content if any is found
            if methods_content:
                self.sections_dict["methods"] = f"===== Methods =====\n" + methods_content
            else:
                print("⚠️ Warning: Methods content found but is empty.")
        else:
            print("⚠️ Warning: Methods section not found.")

    def _extract_results(self):
        """Extracts the full Results section from the article."""
        # Find the section with the specific ID that contains the results
        results_section = self.soup.find('section', {'id': 'sec-3'})
        
        if results_section:
            # Initialize the results content
            results_content = ""
            
            # Extract the main results paragraph
            main_paragraphs = results_section.find_all('div', {'role': 'paragraph'})
            results_content += "\n".join([para.get_text(strip=True) for para in main_paragraphs])
            
            # Now extract any sub-sections under results if applicable
            sub_sections = results_section.find_all('section', {'id': lambda x: x and x.startswith('sec-3-')})
            
            for sub_section in sub_sections:
                sub_section_title = sub_section.find('h3')  # Get the sub-section title (if any)
                sub_section_paragraphs = sub_section.find_all('div', {'role': 'paragraph'})  # Get the paragraph content
                
                if sub_section_title and sub_section_paragraphs:
                    # Add the sub-section title and content to the results content
                    results_content += f"\n\n{str(sub_section_title)}"  # Adding the title (e.g., Specific Result)
                    results_content += "\n".join([para.get_text(strip=True) for para in sub_section_paragraphs])
            
            # Store the results content if any is found
            if results_content:
                self.sections_dict["results"] = f"===== Results =====\n" + results_content
            else:
                print("⚠️ Warning: Results content found but is empty.")
        else:
            print("⚠️ Warning: Results section not found.")

    def _extract_discussion(self):
        """Extracts the full Discussion section from the article."""
        # Find the section with the specific ID that contains the discussion
        discussion_section = self.soup.find('section', {'id': 'sec-4'})
        
        if discussion_section:
            # Initialize the discussion content
            discussion_content = ""
            
            # Extract the main discussion paragraphs
            main_paragraphs = discussion_section.find_all('div', {'role': 'paragraph'})
            discussion_content += "\n".join([para.get_text(strip=True) for para in main_paragraphs])
            
            # Store the discussion content if any is found
            if discussion_content:
                self.sections_dict["discussion"] = f"===== Discussion =====\n" + discussion_content
            else:
                print("⚠️ Warning: Discussion content found but is empty.")
        else:
            print("⚠️ Warning: Discussion section not found.")

    def _extract_conclusions(self):
        """Extracts the full Conclusions section from the article."""
        # Find the section with the specific ID that contains the conclusions
        conclusions_section = self.soup.find('section', {'id': 'sec-5'})
        
        if conclusions_section:
            # Initialize the conclusions content
            conclusions_content = ""
            
            # Extract the main conclusions paragraph
            main_paragraph = conclusions_section.find('div', {'role': 'paragraph'})
            if main_paragraph:
                conclusions_content = main_paragraph.get_text(strip=True)
            
            # Store the conclusions content if found
            if conclusions_content:
                self.sections_dict["conclusions"] = f"===== Conclusions =====\n" + conclusions_content
            else:
                print("⚠️ Warning: Conclusions content found but is empty.")
        else:
            print("⚠️ Warning: Conclusions section not found.")

    def _extract_references(self):
        """Extracts the full References section from the article."""
        # Find the section with the specific ID that contains the references
        references_section = self.soup.find('section', {'id': 'backmatter'})
        
        if references_section:
            # Initialize the references content
            references_content = ""
            
            # Extract the entire content of the references section
            references_content = references_section.get_text(separator="\n", strip=True)
            
            # Store the references content if found
            if references_content:
                self.sections_dict["references"] = f"===== References =====\n" + references_content
            else:
                print("⚠️ Warning: References content found but is empty.")
        else:
            print("⚠️ Warning: References section not found.")

    def extract_image_text(self):
        """Extracts the alt text, title, URL, and OCR text from all images in the article body section."""
        # Find the section with the article body
        body_section = self.soup.find('section', {'id': 'bodymatter'})
        
        images_content = []
        
        if body_section:
            # Find all figures (to include both img and figcaption)
            figures = body_section.find_all('figure')
            
            for img_idx, figure in enumerate(figures):
                # Find the image in the figure
                img = figure.find('img')
                if img:
                    # Get the alt text (or a default message if not available)
                    alt_text = img.get('alt', 'No Alt Text Available')

                    # Get the image URL (src attribute)
                    img_url = img.get('src', 'No URL Available')

                    # Get the image title from the figcaption if available
                    figcaption = figure.find('figcaption')
                    title_text = figcaption.get_text() if figcaption else 'No Title Available'

                    # Try to download the image and extract text using OCR
                    try:
                        # Construct the full URL (if necessary) and download the image
                        image_response = requests.get(img_url)
                        image = Image.open(BytesIO(image_response.content))

                        # Run OCR on the image to extract text
                        ocr_text = pytesseract.image_to_string(image)
                    except Exception as e:
                        ocr_text = f"Error extracting text from image: {str(e)}"

                    # Store the alt text, title, URL, and OCR text of the image
                    images_content.append(f"***** Image {img_idx + 1} ******\nAlt Text: {alt_text}\nTitle: {title_text}\nURL: {img_url}\nOCR Text: {ocr_text}\n")
                    
            self.sections_dict["image_text"] = f"====== ImageText ======\n" + "\n\n".join(images_content)
        else:
            print("⚠️ Warning: Body section not found.")
        
        return images_content


    def extract_tables_with_titles(self):
        """Extracts all tables with their titles from the article body section."""
        # Find the section with the article body
        body_section = self.soup.find('section', {'id': 'bodymatter'})
        
        tables_content = []
        
        if body_section:
            # Find all tables in the body section
            tables = body_section.find_all('table')
            
            for idx, table in enumerate(tables):
                table_title = ""
                # Check if the table has a caption (title)
                caption = table.find('caption')
                if caption:
                    table_title = caption.get_text(separator=" ", strip=True)
                
                # Extract the text of the table
                table_text = table.get_text(separator="\n", strip=True)
                
                # Store the table title and text
                tables_content.append(f"***** Table {idx+1} Title: {table_title} ******\n{table_text}\n")
            self.sections_dict["tables"] = f"====== Tables ======\n" + "\n\n".join(tables_content)
        else:
            print("⚠️ Warning: Body section not found.")
        
        return tables_content
    
    def extract_open_access(self):
        """Extracts Open Access status from the article."""
        # Find the icon element that represents Open Access
        open_access_icon = self.soup.find('i', {'class': 'icon-open_access'})
        
        if open_access_icon:
            self.sections_dict["paper_type"] = f"===== Paper_Type =====\nOpen aceess"
            return "This article is Open Access."
        else:
            return "No Open Access information found."
        
    def _combine_main_content(self):
        """Combine key sections into a single 'Main Content' section."""
        main_content_sections = ["paper_type", "title", "abstract", "tables", "introduction", "methods", "search_strategy", "results", "discussion", "conclusion", "image_text"]
        main_content = []

        for section in main_content_sections:
            if section in self.sections_dict:
                main_content.append(self.sections_dict[section])

        if main_content:
            cleaned = "\n\n".join(main_content)
            self.sections_dict["main_content"] = "===== Main_Content =====\n" + cleaned
            