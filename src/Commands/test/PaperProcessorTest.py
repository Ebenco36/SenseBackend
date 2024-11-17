import sys
import os
import re
import fitz
import ast
import pandas as pd
from app import db, app
from itertools import chain
sys.path.append(os.getcwd())
from bs4 import BeautifulSoup
from flask import request, jsonify
from src.Commands.regexp import searchRegEx
from src.Commands.TaggingSystem import Tagging
from src.Services.GeneralPDFWebScraper import GeneralPDFWebScraper
from src.Commands.TaggingSystemFunctionBased import TaggingSystemFunctionBased

class PaperProcessor:
    def __init__(self, query="SELECT DOI FROM love_db where primary_id = 1", csv_file_path="papers_tags.csv", server_headers=None):
        self.csv_file_path = csv_file_path
        self.query = query
        self.tag_columns = set()  # Initialize as a set
        self.data = []
        self.data2 = []
        self.data3 = []
        self.server_headers = server_headers

    def register_tagging_methods(self, tagging_system):
        # Register all tagging methods and collect column names
        methods = [
            ("number_of_studies_tags", TaggingSystemFunctionBased.tag_number_of_studies_tags),
            ("population_specificGroup_healthcareWorkers_tags", TaggingSystemFunctionBased.tag_population_specificGroup_healthcareWorkers_tags),
            ("population_specificGroup_pregnantWomen_tags", TaggingSystemFunctionBased.tag_population_specificGroup_pregnantWomen_tags),
            ("population_specificGroup_parentsCaregivers_tags", TaggingSystemFunctionBased.tag_population_specificGroup_parentsCaregivers_tags),
            ("population_OtherSpecificGroup_travellers_tags", TaggingSystemFunctionBased.tag_population_OtherSpecificGroup_travellers_tags),
            ("population_ageGroup_newborn_0to1_tags", TaggingSystemFunctionBased.tag_population_ageGroup_newborn_0to1_tags),
            ("population_ageGroup_children_2to9_tags", TaggingSystemFunctionBased.tag_population_ageGroup_children_2to9_tags),
            ("population_ageGroup_adolescents_10to17_tags", TaggingSystemFunctionBased.tag_population_ageGroup_adolescents_10to17_tags),
            ("population_ageGroup_adults_18to64_tags", TaggingSystemFunctionBased.tag_population_ageGroup_adults_18to64_tags),
            ("population_ageGroup_olderAdults_65to10000_tags", TaggingSystemFunctionBased.tag_population_ageGroup_olderAdults_65to10000_tags),
            ("population_immuneStatus_immunocompromised_tags", TaggingSystemFunctionBased.tag_population_immuneStatus_immunocompromised_tags),
            ("population_immuneStatus_healthy_tags", TaggingSystemFunctionBased.tag_population_immuneStatus_healthy_tags),
            ("intervention_vaccinePredictableDisease_covid_tags", TaggingSystemFunctionBased.tag_intervention_vaccinePredictableDisease_covid_tags),
            ("intervention_vaccinePredictableDisease_influenza_tags", TaggingSystemFunctionBased.tag_intervention_vaccinePredictableDisease_influenza_tags),
            ("intervention_vaccinePredictableDisease_dengue_tags", TaggingSystemFunctionBased.tag_intervention_vaccinePredictableDisease_dengue_tags),
            ("intervention_vaccinePredictableDisease_rotavirus_tags", TaggingSystemFunctionBased.tag_intervention_vaccinePredictableDisease_rotavirus_tags),
            ("intervention_vaccineOptions_live_tags", TaggingSystemFunctionBased.tag_intervention_vaccineOptions_live_tags),
            ("intervention_vaccineOptions_nonLive_tags", TaggingSystemFunctionBased.tag_intervention_vaccineOptions_nonLive_tags),
            ("intervention_vaccineOptions_adjuvants_tags", TaggingSystemFunctionBased.tag_intervention_vaccineOptions_adjuvants_tags),
            ("intervention_vaccineOptions_nonAdjuvants_tags", TaggingSystemFunctionBased.tag_intervention_vaccineOptions_nonAdjuvants_tags),
            ("topic_acceptance_tags", TaggingSystemFunctionBased.tag_topic_acceptance_tags),
            ("topic_coverage_tags", TaggingSystemFunctionBased.tag_topic_coverage_tags),
            ("topic_economic_tags", TaggingSystemFunctionBased.tag_topic_economic_tags),
            ("topic_ethicalIssues_tags", TaggingSystemFunctionBased.tag_topic_ethicalIssues_tags),
            ("topic_administration_tags", TaggingSystemFunctionBased.tag_topic_administration_tags),
            ("topic_efficacyEffectiveness_tags", TaggingSystemFunctionBased.tag_topic_efficacyEffectiveness_tags),
            ("topic_modeling_tags", TaggingSystemFunctionBased.tag_topic_modeling_tags),
            ("topic_safety_tags", TaggingSystemFunctionBased.tag_topic_safety_tags),
            ("topic_risk_factor_tags", TaggingSystemFunctionBased.tag_topic_risk_factor_tags),
            ("outcome_infection_tags", TaggingSystemFunctionBased.tag_outcome_infection_tags),
            ("outcome_hospitalization_tags", TaggingSystemFunctionBased.tag_outcome_hospitalization_tags),
            ("outcome_death_tags", TaggingSystemFunctionBased.tag_outcome_death_tags),
            ("outcome_ICU_tags", TaggingSystemFunctionBased.tag_outcome_ICU_tags),
                        
        ]
        
        for tag_name, method in methods:
            tagging_system.register_tag_method(tag_name, method)
            self.tag_columns.add(tag_name)

    def flatten_tags(self, tags):
        """Convert nested lists or other complex data types to flattened strings."""
        for key, value in tags.items():
            if isinstance(value, list):
                # Flatten nested lists
                flattened_value = list(chain.from_iterable(
                    v if isinstance(v, list) else [v] for v in value
                ))
                # Join the flattened list into a comma-separated string
                tags[key] = ', '.join(str(v) for v in flattened_value)
            elif isinstance(value, str):
                tags[key] = value.strip()
        return tags

    def construct_pdf_url(self, full_url):
        # Ensure the URL contains the expected structure
        if "/full" in full_url and "/doi/" in full_url:
            # Extract the DOI prefix
            doi_prefix = full_url.split('/')[-2]  # e.g., 14651858.CD013626.pub2
            
            # Extract the article code from the DOI prefix (ignore '.pub2' if present)
            article_code = doi_prefix.split('.')[1]  # e.g., CD013626
            
            # Construct the PDF URL based on the DOI prefix and article code
            pdf_url = f"/cdsr/doi/10.1002/{doi_prefix}/pdf/CDSR/{article_code}/{article_code}.pdf"
            return pdf_url
        else:
            return "Invalid URL format"
        
    @staticmethod
    def extract_dois(doi_string):
        # Convert the input string to a list
        doi_list = ast.literal_eval(doi_string)
        # Extract DOIs with "[doi]" and remove the tag
        return [doi.split()[0] for doi in doi_list if "[doi]" in doi]
    
    
    def process_papers(self, DB_name = None):
        with app.app_context():
            conn = db.engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute(self.query)
            papers = cursor.fetchall()
            cursor.close()
            conn.close()

            # Add DOI and DOI URL columns
            self.tag_columns.add("doi")
            self.tag_columns.add("doi_url")
            header = None
            for paper in papers:
                Id = paper[0]
                doi = paper[1]
                doi_link = None
                if paper and len(paper) == 3:
                    doi_link = paper[2]
                
                if DB_name is None:
                    doi_url = doi if "https://dx.doi.org/" in doi.lower() else "https://dx.doi.org/" + doi
                elif DB_name == "Cochrane":
                    # /cdsr/doi/10.1002/14651858.CD015125/full
                    if self.server_headers:
                        header = self.server_headers
                    else:
                        headers = {
                            "authority": "www.cochranelibrary.com",
                            "method": "GET",
                            "path": doi,
                            "scheme": "https",
                            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                            "accept-encoding": "gzip, deflate, br, zstd",
                            "accept-language": "en-GB-oxendict,en-US;q=0.9,en;q=0.8,yo;q=0.7",
                            "cookie": "GUEST_LANGUAGE_ID=en_US; COOKIE_SUPPORT=true; osano_consentmanager_uuid=ca4d3e91-b57b-4a3d-a016-fde74cd79d96; osano_consentmanager=Nzjf7YZ3KmhYJBGYIahMg5_gokVuae4KhVL-zxFdyAVDtN5z_zqwvLrrsO3w9q00Foyef1VQAUZIGg6tSRF3QMSqPwPCvOnQWoJVXGhc0o_5b12XBqw7aQZLvdmEZYWkM_B5JNyb1ijdX9c12K-E2CscO2useUE1hNFxKSISMz1bGWGew2XIhyn0Dhkgv9tyy4pxXooA7lH5lTxE0YUsziIKDYrA70R8kYCo91Yxw0ZC33Ob0x5CZfGPR1MHNj2Y_C_w1f7S4xq2nBC3fh5j5P9qWpQ=; SCOL_LAST_GROUP_ID=20182; _ga=GA1.2.544934602.1731435906; AMCVS_1B6E34B85282A0AC0A490D44%40AdobeOrg=1; _fbp=fb.1.1731435905876.295006754302034200; s_cc=true; JSESSIONID=cspbwgreclprt160y1~6947DA1DEB3ECE47A5BDA45A8C7B736E; SID_REP=4BB968CF763644DFD62647D1823EF83C; SCOLAUTHSESSIONID=1FFC2D291B77E50B465C395AD899343C; _gid=GA1.2.993814874.1731543190; cf_clearance=lLDRpNQSZj8rEHiFpC3XeAQUUosxlWephh97n.Jqyo4-1731543190-1.2.1.1-oDzFq_do8AWlJnmNSPQEqAYM6_dIEWsRw_MiQAWUg4m16gHTP2pm4bh1WGBFyv7XkuEQH0Goz5JwHP1TDhTbH8hlm2LPFOXsrf93rPY8CZrdAonQ.2.x23q8GzyuCfLid6wxM7wTf2tlZ5NsmXPkiNd4rRg0IJQp0llXyTMpZFlQpfD11LJw1kAvpidoFTJ71fU0Q1WBun4SzCfMEzi2Upgd6fskYARepq7Lur0wZVMGgqJ.NulMRwIA5KhggWexctftQY3bYPK6RmOSh7CxPeP_FvbXJUO3aYvVs.3uJPsR4DMHpG0kaVe8P8qVntBtkWXc5Jf1yvRm__.6gF0Zq.vxFO6n8rwzaKUrGpNdjIXPo5z7qCI0sakmGukw5p1WqJoV9h9vmIPhTmy5BxgajQ; AMCV_1B6E34B85282A0AC0A490D44%40AdobeOrg=-2121179033%7CMCIDTS%7C20042%7CMCMID%7C11280074091188381753314795158505394954%7CMCAAMLH-1732147990%7C6%7CMCAAMB-1732147990%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1731550390s%7CNONE%7CMCAID%7C33310B210879C5EF-40001CD786333A88%7CvVersion%7C5.3.0; CONTENT_LANGUAGE=en; LFR_SESSION_STATE_20159=1731543471854; __cf_bm=9zanr.TCCSmItxf4JET3Ts9kGEfawn3NmsFwHCukYgs-1731544285-1.0.1.1-Q7p9AVWk8FZAVuwp.862aMubTXhet.ZGf1sZTW7ZKdeBNgddLDtm9h82QJKyqkz9hwKxHqaqwekqN66TA_ggcg; SCOL_SESSION_TIMEOUT=1740 Thu, 14 Nov 2024 01:02:12 GMT; _ga_BRLBHKT9XF=GS1.1.1731543199.4.1.1731544466.0.0.0",
                            "if-modified-since": "Thu, 14 Nov 2030 00:33:48 GMT",
                            "priority": "u=0, i",
                            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                            "sec-ch-ua-mobile": "?0",
                            "sec-ch-ua-platform": '"macOS"',
                            "sec-fetch-dest": "document",
                            "sec-fetch-mode": "navigate",
                            "sec-fetch-site": "none",
                            "sec-fetch-user": "?1",
                            "upgrade-insecure-requests": "1",
                            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                        }
                    ddoi_link = doi_link if doi_link else doi
                    doi_complete_link = (self.construct_pdf_url(ddoi_link))
                    doi_url = "https://www.cochranelibrary.com" + doi_complete_link
                elif DB_name == "Medline":
                    doi_list = doi
                    dois = self.extract_dois(doi_list)
                    doi_url = "https://dx.doi.org/" + dois[0]
                    print(doi_url)
                else:
                    doi_url = doi if "https://dx.doi.org/" in doi.lower() else "https://dx.doi.org/" + doi
                scraper = GeneralPDFWebScraper(doi_url, DB_name, header)
                text = scraper.fetch_and_extract_first_valid_pdf_text()

                if text:
                    tagging_system = TaggingSystemFunctionBased()
                    self.register_tagging_methods(tagging_system)
                    
                    tags = tagging_system.apply_tags(text)
                    tags2 = TaggingSystemFunctionBased.tag_all_with_old_implementation(text)
                    
                    tagger = Tagging(text)
                    tags3 = tagger.create_columns_from_text(searchRegEx)
    
                    # tags2["doi"] = doi
                    tags2["doi_url"] = doi_url
                    # Ensure tags dictionary includes doi and doi_url
                    tags3["Id"] = Id
                    # tags3["doi"] = doi
                    tags3["doi_url"] = doi_url
                    
                    # Ensure tags dictionary includes doi and doi_url
                    # tags["doi"] = doi
                    tags["doi_url"] = doi_url
                    
                    # Flatten complex data types
                    tags = self.flatten_tags(tags)
                    tags2 = self.flatten_tags(tags2)
                    tags3 = self.flatten_tags(tags3)
                    
                    # Add tags to the data list
                    self.data.append(tags)
                    self.data2.append(tags2)
                    self.data3.append(tags3)

            # Convert set to list and sort columns
            sorted_columns = sorted(self.tag_columns)
            # Convert list of dictionaries to DataFrame
            df = pd.DataFrame(self.data, columns=sorted_columns)
            df2 = pd.DataFrame(self.data2)
            df3 = pd.DataFrame(self.data3)
            # Save DataFrame to CSV
            df.to_csv(self.csv_file_path + ".csv", index=False, encoding='utf-8')
            df2.to_csv(self.csv_file_path + "_2.csv", index=False, encoding='utf-8')
            df3.to_csv(self.csv_file_path + "_3.csv", index=False, encoding='utf-8')
        return df3
# csv_file_path_love = 'love_papers_tags'
# query_love_db = "SELECT DOI FROM love_db where primary_id = 1"
# paper_processor = PaperProcessor(query=query_love_db, csv_file_path=csv_file_path_love)
# paper_processor.process_papers()
# print({"status": "success", "message": "Papers processed and CSV generated.", "csv_file_path": csv_file_path_love})
