from flask import request, jsonify
import fitz
from bs4 import BeautifulSoup
import csv
import sys
import os
sys.path.append(os.getcwd())
from src.Commands.TaggingSystem import TaggingSystem
from src.Services.GeneralPDFWebScraper import GeneralPDFWebScraper
import re
from app import db, app

class PaperProcessor:
    def __init__(self, query="SELECT DOI FROM love_db where primary_id = 1", csv_file_path="papers_tags.csv"):
        self.csv_file_path = csv_file_path
        self.csv_columns = ["doi"]
        self.query = query

    def process_papers(self):
        with app.app_context():
            conn = db.engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute(self.query)
            papers = cursor.fetchall()
            cursor.close()
            conn.close()

            # Append tag columns dynamically
            self.csv_columns.append("adolescent_young_adult_age_range")

            with open(self.csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=self.csv_columns)
                writer.writeheader()

                for paper in papers:
                    doi = paper[0]
                    doi_url = doi if "https://dx.doi.org/" in doi else "https://dx.doi.org/" + doi
                    scraper = GeneralPDFWebScraper(doi_url)
                    print(doi_url)
                    # print(scraper.fetch_and_extract_first_valid_pdf_text())
                    text = scraper.fetch_and_extract_first_valid_pdf_text()

                    if text:
                        tagging_system = TaggingSystem()

                        # Register all the tagging methods
                        tagging_system.register_tag_method("intervention_vaccinePredictableDisease_tags", TaggingSystem.tag_intervention_vaccinePredictableDisease_tags)
                        tagging_system.register_tag_method("population_OtherSpecificGroup_tags", TaggingSystem.tag_population_OtherSpecificGroup_tags)
                        tagging_system.register_tag_method("population_specificGroup_acronyms_", TaggingSystem.tag_population_specificGroup_acronyms)
                        tagging_system.register_tag_method("intervention_vaccineOptions_tags", TaggingSystem.tag_intervention_vaccineOptions_tags)
                        tagging_system.register_tag_method("topic_efficacyEffectiveness_tags", TaggingSystem.tag_topic_efficacyEffectiveness_tags)
                        tagging_system.register_tag_method("population_specificGroup_tags", TaggingSystem.tag_population_specificGroup_tags)
                        tagging_system.register_tag_method("population_immuneStatus_tags", TaggingSystem.tag_population_immuneStatus_tags)
                        tagging_system.register_tag_method("outcome_hospitalization_tags", TaggingSystem.tag_outcome_hospitalization_tags)
                        tagging_system.register_tag_method("topic_administration_tags", TaggingSystem.tag_topic_administration_tags)
                        tagging_system.register_tag_method("topic_immunogenicity_tags", TaggingSystem.tag_topic_immunogenicity_tags)
                        tagging_system.register_tag_method("population_ageGroup_tags", TaggingSystem.tag_population_ageGroup_tags)
                        tagging_system.register_tag_method("topic_ethicalIssues_tags", TaggingSystem.tag_topic_ethicalIssues_tags)
                        tagging_system.register_tag_method("number_of_studies_tags", TaggingSystem.tag_number_of_studies_tags)
                        tagging_system.register_tag_method("outcome_infection_tags", TaggingSystem.tag_outcome_infection_tags)
                        tagging_system.register_tag_method("topic_acceptance_tags", TaggingSystem.tag_topic_acceptance_tags)
                        tagging_system.register_tag_method("topic_coverage_tags", TaggingSystem.tag_topic_coverage_tags)
                        tagging_system.register_tag_method("topic_economic_tags", TaggingSystem.tag_topic_economic_tags)
                        tagging_system.register_tag_method("outcome_death_tags", TaggingSystem.tag_outcome_death_tags)
                        tagging_system.register_tag_method("topic_safety_tags", TaggingSystem.tag_topic_safety_tags)
                        tagging_system.register_tag_method("outcome_ICU_tags", TaggingSystem.tag_outcome_ICU_tags)
                        tagging_system.register_tag_method("review_tags", TaggingSystem.tag_review_tags)


                        tags =  tagging_system.apply_tags(text) 
                        tags["doi_url"] = doi_url,
                        writer.writerow(tags)
                    
                    
csv_file_path_love = 'love_papers_tags.csv'
query_love_db = "SELECT DOI FROM love_db where primary_id = 1"
paper_processor = PaperProcessor(query=query_love_db, csv_file_path=csv_file_path_love)
paper_processor.process_papers()
print({"status": "success", "message": "Papers processed and CSV generated.", "csv_file_path": csv_file_path})


csv_file_path_ovid = 'ovid_papers_tags.csv'
query_ovid_db = "SELECT DOI FROM ovid_db where primary_id = 1"
paper_processor = PaperProcessor(query=query_ovid_db, csv_file_path=csv_file_path_ovid)
paper_processor.process_papers()
print({"status": "success", "message": "Papers processed and CSV generated.", "csv_file_path": csv_file_path})
