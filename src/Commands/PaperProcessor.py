import sys
import os
import re
import fitz
import ast
import pandas as pd
import requests
from itertools import chain
sys.path.append(os.getcwd())
from bs4 import BeautifulSoup
from flask import request, jsonify
from src.Commands.regexp import searchRegEx
from src.Commands.TaggingSystem import Tagging
from src.Utils.Helpers import contains_http_or_https
from src.Services.Factories.GeneralPDFScraper.CochranePDFWebScraper import CochranePDFWebScraper
from src.Services.Factories.GeneralPDFScraper.LOVEPDFWebScraper import LOVEPDFWebScraper
from src.Services.Factories.GeneralPDFScraper.GeneralPDFWebScraper import GeneralPDFWebScraper
from src.Services.Factories.GeneralPDFScraper.MedlinePDFWebScraper import MedlinePDFWebScraper
from src.Services.Factories.GeneralPDFScraper.OVIDPDFWebScraper import OVIDPDFWebScraper
# from src.Commands.TaggingSystemFunctionBased import TaggingSystemFunctionBased

class PaperProcessor:
    DOI_PREFIX = "https://dx.doi.org/"

    def __init__(self, db_handler, csv_file_path, server_headers=None):
        self.db_handler = db_handler
        self.tag_columns = set()
        self.csv_file_path = csv_file_path
        self.server_headers = server_headers
        self.scrapers = {}
        self.data = []

    def process_papers(self, db_name=None):
        """Processes all papers and saves the extracted data to CSV files."""
        papers = self.db_handler.fetch_papers_with_column_names()
        self.tag_columns.update(["Id", "doi", "doi_url"])
        for paper in papers:
            db_name = db_name if (db_name and db_name != "all") else paper.get("Source", "Cochrane")
            text, doi_url, paper_id, doi = self._process_single_paper(paper, db_name)
            if text:
                tags = self._apply_tagging(text, doi_url, paper_id, doi)
                
                self.data.append(tags)
        self._save_data_to_csv()
        
        return pd.DataFrame(self.data)

    @staticmethod
    def extract_dois(doi_string):
        if doi_string:
            try:
                # Check if the string is a list or a single DOI
                if doi_string.startswith("[") and doi_string.endswith("]"):
                    # Parse the string as a list
                    doi_list = ast.literal_eval(doi_string)
                    return [doi.split()[0] for doi in doi_list if "[doi]" in doi]
                else:
                    # Treat the string as a single DOI
                    return [doi_string]
            except (SyntaxError, ValueError, TypeError) as e:
                print(f"Error extracting DOIs: {e}")
                return []
        else:
            print("DOI not found!!!")
            return []

    def _process_single_paper(self, paper, db_name):
        """Processes a single paper by scraping and tagging its content."""
        
        paper_id = paper.get("primary_id", None)
        doi = paper.get("DOI", None)
        doi_link = paper.get("doi_url", None)
        source = paper.get("Source", None)
        doi_url = self._construct_doi_url(doi, doi_link, db_name)
        scraper = self._select_scraper(doi_url, db_name)
        
        if doi_url:
            try:
                text = scraper.fetch_and_extract_first_valid_pdf_text()
            except Exception as e:
                print(f"EOFError encountered while processing PDF content for DOI: {doi_url} - {e}")
                text = scraper.fetch_text_from_html()
            return text, doi_url, paper_id, doi
        else:
            return None, None, None, None

    def _select_scraper(self, doi_url, db_name):
        """Selects the appropriate scraper based on the database name."""
        if db_name not in self.scrapers:
            # Instantiate and cache the scraper for the given database
            if db_name == "Cochrane":
                self.scrapers[db_name] = CochranePDFWebScraper(db_name, self.server_headers)
            elif db_name == "LOVE":
                self.scrapers[db_name] = LOVEPDFWebScraper(db_name, self.server_headers)
            elif db_name == "Medline":
                self.scrapers[db_name] = MedlinePDFWebScraper(db_name, self.server_headers)
            elif db_name == "OVID":
                self.scrapers[db_name] = OVIDPDFWebScraper(db_name, self.server_headers)
            else:
                self.scrapers[db_name] = LOVEPDFWebScraper(db_name, self.server_headers)

        # Reuse the cached instance and set the DOI URL
        return self.scrapers[db_name].set_doi_url(doi_url)

    def _construct_doi_url(self, doi, doi_link, db_name):
        """Constructs the DOI URL based on the database name and DOI."""
        if db_name == "Cochrane":
            doi_link = doi
            return "https://www.cochranelibrary.com" + self._cochrane_doi_path(doi_link)
        elif db_name == "Medline":
            doi_list = self.extract_dois(doi)
            if doi_list:
                return self.DOI_PREFIX + doi_list[0]
            else:
                print("No valid DOI found.")
                return ""
        return self.format_doi(doi)


    def format_doi(self, doi):
        if (self.DOI_PREFIX in doi.lower() or contains_http_or_https(doi)):
            return doi
        else:
            return self.DOI_PREFIX + doi
    
    def construct_pdf_url(self, full_url):
        """Constructs the PDF URL path based on the DOI prefix and article code."""
        if "/full" in full_url and "/doi/" in full_url:
            doi_prefix = full_url.split('/')[-2]
            article_code = doi_prefix.split('.')[1]  # Assumes format with article code
            pdf_url = f"/cdsr/doi/10.1002/{doi_prefix}/pdf/CDSR/{article_code}/{article_code}.pdf"
            return pdf_url
        else:
            print("Invalid URL format for Cochrane PDF.")
            return ""

    def _cochrane_doi_path(self, doi):
        """Constructs the path for Cochrane's DOI format."""
        return self.construct_pdf_url(doi)

    def _apply_tagging(self, text, doi_url, paper_id, doi):
        """Applies tagging to the text content and structures the results."""
        tagger = Tagging(text)
        tags = tagger.create_columns_from_text(searchRegEx)
        tags["Id"] = paper_id
        tags["doi"] = doi
        tags["doi_url"] = doi_url
        # Flatten complex data types
        response = self.flatten_tags(tags)
        
        return response
    
    def flatten_tags(self, tags):
        """Convert nested lists or other complex data types to flattened strings."""
        for key, value in tags.items():
            if isinstance(value, list):
                # Select only the last list if multiple lists exist
                if isinstance(value[-1], list):
                    value = value[-1]  # Take the last list for processing
                
                # Convert the selected list to a comma-separated string
                tags[key] = ", ".join(map(str, value)) if isinstance(value, list) else str(value)

            elif isinstance(value, str):
                tags[key] = value.strip()  # Remove unnecessary whitespace
            
        return tags

    def _save_data_to_csv(self):
        """Saves the processed data into separate CSV files."""
        sorted_columns = sorted(self.tag_columns)
        df = pd.DataFrame(self.data)

        df.to_csv(f"{self.csv_file_path}_1.csv", index=False, encoding='utf-8')
