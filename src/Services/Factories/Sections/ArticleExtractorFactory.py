from src.Services.Factories.Sections.PlosExtractor import PlosExtractor
from src.Services.Factories.Sections.TandfonlineExtractor import TandfonlineExtractor
from src.Services.Factories.Sections.ScienceDirectExtractor import ScienceDirectExtractor
from src.Services.Factories.Sections.NCBIExtractor import NCBIExtractor
from src.Services.Factories.Sections.BMJExtractor import BMJExtractor
from src.Services.Factories.Sections.UniversalExtractor import UniversalExtractor
from src.Services.Factories.Sections.PDFExtractor import PDFUniversalExtractor
from src.Services.Factories.Sections.TXTUniversalExtractor import TXTUniversalExtractor
from src.Services.Factories.Sections.CochraneUniversalExtractor import CochraneUniversalExtractor
from src.Services.Factories.Sections.SagePubExtractor import SagePubExtractor
import requests

class ArticleExtractorFactory:
    """Factory class to select the appropriate extractor based on the URL."""
    @staticmethod
    def get_extractor(soup=None, url=None, pdf_content=None, text_content=None):
        if (url and "ncbi.nlm.nih.gov" in url):
            return NCBIExtractor(soup=soup, url=url)
        elif (url and "bmj.com" in url):
            return BMJExtractor(soup=soup, url=url)
        elif (url and "cochranelibrary.com" in url):
            return CochraneUniversalExtractor(soup=soup, url=url)
        elif (url and "journals.sagepub.com" in url):
            return SagePubExtractor(soup=soup, url=url)
        elif (url and "sciencedirect.com" in url):
            return ScienceDirectExtractor(soup=soup, url=url)
        elif (url and "www.tandfonline.com" in url):
            return TandfonlineExtractor(soup=soup, url=url)
        elif (url and "journals.plos.org" in url):
            print(url)
            return PlosExtractor(soup=soup, url=url)
        elif pdf_content or (url and ArticleExtractorFactory.is_pdf_url(url)):
            return PDFUniversalExtractor(pdf_content=pdf_content, url=url)
        elif text_content:
            return TXTUniversalExtractor(text_content=text_content)
        else:
            return UniversalExtractor(soup=soup, url=url)
        
        
    @staticmethod
    def is_pdf_url(url):
        """
        Checks if a URL contains a PDF file.

        Parameters:
        - url (str): The URL to check.

        Returns:
        - bool: True if the URL is a PDF, False otherwise.
        """
        # Check if the URL ends with .pdf
        if url.lower().endswith(".pdf"):
            return True

        try:
            # Send a HEAD request to check the Content-Type
            response = requests.head(url, allow_redirects=True, timeout=5)
            content_type = response.headers.get("Content-Type", "").lower()

            # Check if the Content-Type indicates a PDF
            if "application/pdf" in content_type:
                return True
        except requests.RequestException:
            pass  # Handle errors (e.g., network issues, invalid URL)

        return False