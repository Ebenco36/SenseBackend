from PIL import Image, ImageOps, ImageFilter, ImageEnhance
import pytesseract
import requests
from io import BytesIO
from bs4 import BeautifulSoup, Tag
import re
import urllib.parse
import numpy as np

class PrismaImageScraper:
    """
    A robust PRISMA image scraper that works across multiple sources (MDPI, PMC, Elsevier, etc.).
    """

    def __init__(self, soup, page_url, max_parent_depth=5):
        """
        Initialize the scraper with a BeautifulSoup object and the URL of the page.

        Args:
            soup (BeautifulSoup): Parsed HTML document.
            page_url (str): The original webpage URL (used to resolve relative image paths).
            max_parent_depth (int): Depth to search parent elements for images.
        """
        self.soup = soup
        self.base_url = self._extract_base_url(page_url)  # Extract base URL from the provided URL
        self.max_parent_depth = max_parent_depth
        self.prisma_keywords = ["PRISMA", "flowchart", "flow diagram", "Flow diagram", "Systematic review"]
        
    def fetch_prisma_image_urls(self):
        """
        Fetch PRISMA image URLs from the BeautifulSoup object.

        Returns:
            list: List of URLs of PRISMA images, or an empty list if none are found.
        """
        found_images = set()

        # Search for PRISMA-related images
        for keyword in self.prisma_keywords:
            matching_elements = self.soup.find_all(string=re.compile(keyword, re.IGNORECASE))
            for element in matching_elements:
                found_images.update(self._search_nearby_images(element))

        # If no PRISMA-related images found, try extracting all images
        if not found_images:
            found_images = self._extract_all_images()

        return list(found_images)

    def _search_nearby_images(self, element):
        """
        Search for PRISMA images near a given element.

        Args:
            element (Tag or NavigableString): The element containing a PRISMA-related keyword.

        Returns:
            set: Set of absolute image URLs found near the element.
        """
        images = set()

        # Ensure we are working with an HTML tag
        parent = element if isinstance(element, Tag) else element.parent
        depth = 0
        
        # 🔹 STEP 1: Search within `alt`, `title`, `figcaption`, `div.caption`
        for img in self.soup.find_all("img"):
            alt_text = img.get("alt", "").lower()
            title_text = img.get("title", "").lower()
            if any(keyword.lower() in alt_text or keyword.lower() in title_text for keyword in self.prisma_keywords):
                image_src = self._extract_image_url(img)
                if image_src:
                    images.add(image_src)
        
        # 🔹 STEP 2: Search nearby siblings (previous and next)
        for sibling in element.find_previous_siblings():
            if sibling.name == "img":
                image_src = self._extract_image_url(sibling)
                if image_src:
                    images.add(image_src)

        for sibling in element.find_next_siblings():
            if sibling.name == "img":
                image_src = self._extract_image_url(sibling)
                if image_src:
                    images.add(image_src)
        # 🔹 STEP 3: Search parent elements (Climb up the DOM tree)
        while parent and depth < self.max_parent_depth:
            if isinstance(parent, Tag):
                for img in parent.find_all("img"):
                    image_src = self._extract_image_url(img)
                    if image_src:
                        images.add(image_src)
            parent = parent.parent
            depth += 1
        
        # 🔹 STEP 4: Search `<figure>` and `<figcaption>` (PMC structure)
        for figure in self.soup.find_all("figure"):
            fig_caption = figure.find("figcaption")
            if fig_caption and any(keyword.lower() in fig_caption.get_text().lower() for keyword in self.prisma_keywords):
                for img in figure.find_all("img"):
                    image_src = self._extract_image_url(img)
                    if image_src:
                        images.add(image_src)
        
        # 🔹 STEP 5: Search `<a>` tags that wrap `<img>` (PMC zoom-in images)
        for a_tag in self.soup.find_all("a", class_="tileshop"):
            img_tag = a_tag.find("img")
            if img_tag:
                image_src = self._extract_image_url(img_tag) or a_tag.get("href")
                if image_src:
                    images.add(image_src)
        
        # 🔹 STEP 6: Search all images if PRISMA keywords appear in captions
        for caption in self.soup.find_all("figcaption"):
            caption_text = caption.get_text().lower()
            if any(keyword.lower() in caption_text for keyword in self.prisma_keywords):
                for img in caption.find_parent("figure").find_all("img"):
                    image_src = self._extract_image_url(img)
                    if image_src:
                        images.add(image_src)
        
        return images

    def _extract_all_images(self):
        """
        Extracts all images if no PRISMA-specific images are found.

        Returns:
            set: Set of absolute image URLs found in the document.
        """
        images = set()
        for img in self.soup.find_all("img"):
            image_src = self._extract_image_url(img)
            if image_src:
                images.add(image_src)
        return images

    def _extract_image_url(self, img_tag):
        """
        Extracts and resolves the absolute URL of an image.

        Args:
            img_tag (Tag): The <img> tag element.

        Returns:
            str or None: Absolute URL of the image if available.
        """
        image_src = (
            img_tag.get("src") or
            img_tag.get("data-src") or
            img_tag.get("data-large") or
            img_tag.get("data-original")
        )

        if image_src:
            # Ensure the URL is absolute; append base_url if necessary
            if not image_src.startswith("http"):
                return urllib.parse.urljoin(self.base_url, image_src)
            return image_src
        return None

    def _extract_base_url(self, page_url):
        """
        Extracts the base URL from a full page URL.

        Args:
            page_url (str): The full URL of the webpage.

        Returns:
            str: Base URL for resolving relative paths.
        """
        parsed_url = urllib.parse.urlparse(page_url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    @staticmethod
    def extract_prisma_text_pillow(image_url, lang='eng'):
        """
        Extracts text from an image URL using optimized OCR preprocessing.

        Args:
            image_url (str): URL of the image.
            lang (str): Language for OCR processing (default: 'eng').

        Returns:
            str: Extracted text content from the image.
        """
        try:
            # Download the image from the URL
            response = requests.get(image_url, stream=True)
            response.raise_for_status()  # Raise an error for HTTP issues
            img = Image.open(BytesIO(response.content))

            # Convert to grayscale
            gray_img = ImageOps.grayscale(img)

            # Increase contrast
            enhancer = ImageEnhance.Contrast(gray_img)
            contrast_img = enhancer.enhance(2.0)

            # Apply binarization using thresholding
            binarized_img = contrast_img.point(lambda x: 0 if x < 150 else 255, '1')

            # Apply sharpening filter
            sharpened_img = binarized_img.filter(ImageFilter.SHARPEN)

            # Convert to numpy array for better processing
            img_array = np.array(sharpened_img)

            # Perform OCR with structured text mode
            extracted_text = pytesseract.image_to_string(img_array, lang=lang, config="--psm 6")
            # Define PRISMA-related keywords
            prisma_image_keywords = {"identification", "screening", "eligibility", "included"}

            if any(keyword in extracted_text.lower() for keyword in prisma_image_keywords):
                return extracted_text.strip()
            else:
                return "No Image found"

        except Exception as e:
            return f"Error during OCR extraction: {str(e)}"

    @staticmethod
    def prioritize_images(image_urls):
        """
        Prioritizes images based on format (preferring jpg/png over others).

        Args:
            image_urls (list): List of image URLs.

        Returns:
            str: The most prioritized image URL, or None if the list is empty.
        """
        priority_order = ['jpg', 'jpeg', 'png', 'gif']
        sorted_images = sorted(
            image_urls,
            key=lambda url: next((i for i, ext in enumerate(priority_order) if url.lower().endswith(ext)), len(priority_order))
        )
        return sorted_images