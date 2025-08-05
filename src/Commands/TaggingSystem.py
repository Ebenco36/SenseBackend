import os
import re
import json
import spacy
import pycountry
import pandas as pd
from datetime import date
from word2number import w2n
from typing import List, Tuple, Dict
from dateutil.parser import parse
from collections import defaultdict
from src.Commands.NERInference import NERTester, run_ner_inference
from src.Services.Factories.Sections.SectionExtractor import SectionExtractor
from src.Utils.Helpers import clean_references
from transformers import pipeline, AutoModelForQuestionAnswering, AutoTokenizer
from typing import Optional, List, Dict
from src.Commands.regexp import searchRegEx
from src.Commands.Amstar2 import Amstar2


class Tagging:
    def __init__(self, document, model_path: str = "./models/tinyroberta"):
        self.document = clean_references(document)
        self.sections = SectionExtractor(self.document)
        self.result_columns = defaultdict(list)
        # init
        self.countries = [
            "Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Argentina", "Armenia", "Australia", "Austria",
            "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin",
            "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso",
            "Burundi", "Cabo Verde", "Cambodia", "Cameroon", "Canada", "Central African Republic", "Chad", "Chile",
            "China", "Colombia", "Comoros", "Congo", "Costa Rica", "Croatia", "Cuba", "Cyprus", "Czech Republic",
            "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "America",
            "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia", "Fiji", "Finland", "France", "Gabon",
            "Gambia", "Georgia", "Germany", "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau",
            "Guyana", "Haiti", "Honduras", "Hungary", "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland",
            "Israel", "Italy", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kuwait", "Kyrgyzstan",
            "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg",
            "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Mauritania", "Mauritius", "Mexico",
            "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru",
            "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria", "North Korea", "North Macedonia",
            "Norway", "Oman", "Pakistan", "Palau", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines",
            "Poland", "Portugal", "Qatar", "Romania", "Russia", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia",
            "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia",
            "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Islands",
            "Somalia", "South Africa", "South Korea", "South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname",
            "Sweden", "Switzerland", "Syria", "Taiwan", "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo",
            "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine",
            "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu",
            "Vatican City", "Venezuela", "Vietnam", "Yemen", "Zambia", "Zimbabwe"
        ]

        self.regions = [
            "Africa", "Asia", "Europe", "North America", "South America", "Oceania", "Middle East", "Sub-Saharan Africa",
            "Southeast Asia", "East Asia", "South Asia", "Central Asia", "Western Europe", "Eastern Europe", "Latin America",
            "Caribbean", "Pacific Islands"
        ]

        self.keywords = [
            "effectiveness", "impact of", "effectiveness of", "efficacy", "VE", "CI", "RR", "OR", "odds ratios",
            "odds ratio OR", "odds ratios ORs", "IRR", "relative risksRR", "relative risks", "efficacy rate",
            "effectiveness rate", "vaccine efficacy", "safety", "adverse effects", "adverse events", "risk factor",
            "risk", "coverage", "uptake", "the uptake", "actual uptake", "vaccine uptake", "acceptance", "Barrier",
            "vaccine barriers", "knowledge", "vaccination willingness and intentions", "HPV vaccine acceptability",
            "Awareness and knowledge", "Awareness", "facilitators of and barriers",
            "awareness,knowledge, acceptability, and intention",
            "knowledge and acceptability", "knowledge and awareness", "attitudes and beliefs", "Knowledge and Attitude",
            "attitude", "knowledge, awareness, and attitude", "administration", "vaccine types", "dose schedules",
            "vaccine types and dose schedules", "different dose schedules", "Two doses of", "economic", "cost",
            "financial", "economic impact", "cost effectiveness", "cost-effectiveness", "economic evaluation",
            "Cost-effectiveness of HPV vaccination strategies", "modeling", "racial", "ethnic", "ethnic minority",
            "racial minority", "racial/ethnic", "racial/ethnic minority", "racial disparity", "ethnic disparity",
            "minority", "minority population"
        ]

        self.database_list = [
            "PubMed", "MEDLINE", "Embase", "Web of Science", "Scopus", "CINAHL", "Cochrane Library", "CENTRAL",
            "PubMed Central", "LILACS", "Google Scholar", "ProQuest", "EBSCO", "Ovid", "PsycINFO", "AMED",
            "ClinicalTrials.gov", "BIOSIS", "TOXLINE", "CANCERLIT", "HMIC", "POPLINE", "Global Health",
            "CAB Abstracts", "AGRICOLA", "GeoRef", "ASSIA", "Social Services Abstracts", "Sociological Abstracts",
            "EconLit", "ERIC", "PAIS Index", "IBSS", "UpToDate", "DynaMed", "Clinical Key", "BMJ Best Practice",
            "Cochrane Clinical Answers", "TRIP Database", "NICE Evidence Search", "DrugBank", "PharmGKB", "RxList",
            "Martindale", "AHFS Drug Information", "OMIM", "GenBank", "Gene", "GEO", "UniProt", "EU Clinical Trials Register",
            "ISRCTN Registry", "WHO ICTRP", "Australian New Zealand Clinical Trials Registry", "Epistemonikos",
            "Health Evidence", "Campbell Collaboration Library", "3ie Database of Systematic Reviews", "OpenGrey",
            "GreyNet", "NTIS", "CORDIS", "ProQuest Dissertations & Theses Global", "EThOS", "DART-Europe",
            "Conference Proceedings Citation Index", "IEEE Xplore", "Google Patents", "Espacenet", "USPTO",
            "Directory of Open Access Journals (DOAJ)", "PLoS", "BioMed Central", "arXiv", "medRxiv", "bioRxiv",
            "OpenDOAR", "BASE", "CINAHL Complete", "MEDLINE Complete", "SocINDEX", "SPORTDiscus", "PEDro",
            "OTseeker", "SpeechBITE", "PsycARTICLES", "PsycBOOKS", "Wiley Online Library", "ScienceDirect",
            "SpringerLink", "JSTOR", "Taylor & Francis Online", "Sage Journals", "Oxford Academic",
            "Cambridge Core", "Nature.com", "Science Magazine", "CDC Stacks", "NIH.gov", "WHO.int",
            "WorldBank.org", "UN iLibrary", "Business Source Complete", "ABI/INFORM", "Factiva", "LexisNexis",
            "Westlaw", "HeinOnline", "Nexis Uni", "ProQuest News & Newspapers", "Academic Search Complete",
            "Project MUSE", "Physiotherapy Evidence"
        ]

    def get_combined_text(self, sections: List):
        contents = " ".join(self.sections.get(section, "")
                            for section in sections).strip()
        if contents and contents != "":
            return contents
        else:
            return self.document

    def create_columns_from_text(self):
        """Main function to apply tagging based on the extensive regex structure provided."""
        for category, subcategories in searchRegEx.items():
            for subcategory, terms_dict in subcategories.items():
                for term_key, term_list in terms_dict.items():
                    column_name = f"{category}#{subcategory}#{term_key}"
                    countries, total_count = self.extract_countries_with_total_count()
                    if category == "popu" and subcategory == "age__group":
                        self.result_columns[column_name] = self.process_age_group(
                            term_key, term_list)
                    elif category == "studies" and (subcategory == "studie__no" or subcategory == "rct"):
                        self.result_columns[column_name], total_study_count, total_rct_count, raw_rct_counts, total_nrsi_count, total_mmtd_count, total_quanti_count = self.process_study_count(
                            term_list)
                        self.result_columns["study_types"] = self.extract_study_types(
                            term_list)
                        self.result_columns["total_study_count"] = total_study_count
                        self.result_columns["total_rct_count"] = total_rct_count
                        self.result_columns["RCT_counts"] = raw_rct_counts

                        self.result_columns["total_nrsi_count"] = total_nrsi_count
                        self.result_columns["total_mmtd_count"] = total_mmtd_count
                        self.result_columns["total_quanti_count"] = total_quanti_count
                    elif category == "gender" and subcategory == "group":
                        pass
                    elif category == "topic" and subcategory == "eff":
                        self.result_columns[column_name] = self.extract_ve_related_info(
                            term_list)
                    elif category == "particip" and subcategory == "group":
                        self.result_columns[column_name] = self.extract_population(
                            term_list)
                    elif category == "lit_search_dates" and subcategory == "dates":
                        self.result_columns[column_name] = self.extract_last_literature_search_dates(
                        )
                    elif category == 'open_acc' and subcategory == "opn_access":
                        self.result_columns[column_name] = self.is_open_access(
                            term_list)
                    elif (category == 'study_country' and subcategory == "countries"):
                        self.result_columns[column_name] = countries
                    elif (category == 'study_country' and subcategory == "study_count"):
                        self.result_columns[column_name] = total_count
                    elif (category == 'title_popu' and subcategory == "title_pop"):
                        self.result_columns[column_name] = self.extract_population_from_title(
                            term_list)
                        title_metadata = self.extract_title_metadata()
                        self.result_columns["location_in_title"] = title_metadata.get(
                            "location", None)
                        self.result_columns["race_ethnicity_in_title"] = title_metadata.get(
                            "race_ethnicity", None)
                        self.result_columns["target_population_in_title"] = title_metadata.get(
                            "target_population", None)
                        self.result_columns["topic_in_title"] = title_metadata.get(
                            "topic", None)
                        self.result_columns["num_databases"] = int(
                            title_metadata.get("num_databases", 0))
                        self.result_columns["duration_of_intervention"] = title_metadata.get(
                            "duration_of_intervention", None)
                        self.result_columns["dosage"] = title_metadata.get(
                            "dosage", None)
                        self.result_columns["comparator"] = title_metadata.get(
                            "comparator", None)
                        # new database
                        self.result_columns["database_list"], self.result_columns["database_count"] = self.extract_databases()
                        
                        self.result_columns["bert_integration"] = self.pubmed_bert_integration(self.document)
                    else:
                        self.result_columns[column_name] = self.process_generic_terms(term_list)
        ################# Merge Dict Together ######################
        # amstars_integration = self.amstar2_integration()
        # # self.result_columns = {**self.result_columns, **amstars_integration}
        # self.result_columns.update(amstars_integration)
        # # print(self.clean_result(self.result_columns))
        return self.clean_result(self.result_columns)

    def extract_num_databases_old(self, text):
        match = re.search(
            r'(\d+)\s+(?:databases|sources|electronic databases|data sources)', text, re.IGNORECASE)
        return int(match.group(1)) if match else 0

    def extract_duration_of_intervention_old(self, text):
        match = re.search(
            r'duration of (\d+\s*(?:weeks|months|years))', text, re.IGNORECASE)
        return match.group(1) if match else ""

    def extract_dosage_old(self, text):
        match = re.search(
            r'(\d+\s*(?:mg|ml|g|mcg|IU))\s+(?:dose|doses|dosage)', text, re.IGNORECASE)
        return match.group(1) if match else ""

    def extract_comparator_old(self, text):
        match = re.search(r'compared to ([A-Za-z ,-]+)', text, re.IGNORECASE)
        return match.group(1) if match else ""

    def extract_number(self, text: str) -> int:
        # Try digit-based match first
        digit_match = re.search(r"\b\d+\b", text)
        if digit_match:
            return int(digit_match.group(0))

        # Try word-based number match (supporting hyphenated and joined)
        word_text = text.lower().replace('-', ' ')
        try:
            return w2n.word_to_num(word_text)
        except Exception:
            return 0

    def extract_title_metadata(self):
        text = self.get_combined_text(["title"])
        text2 = self.get_combined_text(["abstract", "results", "methods"])
        # Extract location (country)
        location = next((place for place in self.countries + self.regions if re.search(
            rf'\b{re.escape(place)}\b', text, re.IGNORECASE)), "")

        # Extract race/ethnicity (captures multiple matches)
        race_ethnicity_matches = re.findall(
            r'\b(Black|White|Hispanic|Hispanic/Latino|Latino|Asian|Indigenous|Native American|Pacific Islander|Mixed race|Other)\b', text, re.IGNORECASE)
        race_ethnicity = list(set(race_ethnicity_matches)
                              ) if race_ethnicity_matches else ""

        # Extract target population (captures multiple groups)
        target_population_matches = re.findall(
            r'(?:people with|targeting|focused on|population of|participants were|among) ([A-Za-z0-9\- ]+)', text, re.IGNORECASE)

        predefined_groups = [
            "pregnant women", "adolescents", "elderly", "children", "infants", "newborns", "patients with cancer",
            "diabetics", "immunocompromised individuals", "smokers", "non-smokers", "obese patients"
        ]
        predefined_population_matches = [group for group in predefined_groups if re.search(
            rf'\b{group}\b', text, re.IGNORECASE)]

        target_population = list(set(target_population_matches + predefined_population_matches)
                                 ) if target_population_matches or predefined_population_matches else ""

        # Extract topic (handles multiple topics)
        topic_matches = [keyword for keyword in self.keywords if re.search(
            rf'\b{re.escape(keyword)}\b', text, re.IGNORECASE)]
        topic = list(set(topic_matches)) if topic_matches else ""
        num_db = self.extract_num_databases_old(text2)
        duration_inter = self.extract_duration_of_intervention_old(text2)
        dosage = self.extract_dosage_old(text2)
        comparator = self.extract_comparator_old(text2)
        return {
            # if location else self.extract_location_in_title(text),
            'location': location,
            # if race_ethnicity else self.extract_race_ethnicity_in_title(text),
            'race_ethnicity': race_ethnicity,
            # if target_population else self.extract_target_population_in_title(text),
            'target_population': target_population,
            'topic': topic,  # if topic else self.extract_topic_in_title(text),
            # if num_db else self.extract_num_databases(text2),
            'num_databases': num_db,
            # if duration_inter else self.extract_duration_of_intervention(text2),
            'duration_of_intervention': duration_inter,
            'dosage': dosage,  # if dosage else self.extract_dosage(text2),
            # if comparator else self.extract_comparator(text2)
            'comparator': comparator,
        }

    def extract_population_from_title(self, term_lists):
        document = self.get_combined_text(["title"])
        # Create a regex pattern for keyword search (case-insensitive)
        pattern = r"\b(" + "|".join(map(re.escape, term_lists)) + r")\b"
        regex = re.compile(pattern, re.IGNORECASE)
        matches = regex.findall(document)
        return ", ".join(list(set(matches)))

    def get_max_date(self, date_strings):
        parsed_dates = []
        for date_str in date_strings:
            # Clean ordinal suffixes (e.g., "st", "nd", "rd", "th")
            cleaned_str = re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', date_str)
            try:
                # Parse with dayfirst=True to prioritize day-month order for ambiguous dates
                dt = parse(cleaned_str, dayfirst=True, fuzzy=True)
                parsed_dates.append((dt, date_str))
            except:
                continue  # Skip invalid/unparseable dates

        if not parsed_dates:
            return None  # Return None if no valid dates found

        # Get the latest date (based on parsed datetime) and return its original string
        latest_date = max(parsed_dates, key=lambda x: x[0])
        return latest_date[1]

    def extract_last_literature_search_dates(self):
        """
        Extracts literature search dates from the given text.
        """
        # Combine text from the specified sections
        document = self.get_combined_text(
            ["abstract", "search_strategy", "methods"])
        # with open("text.txt", 'w', encoding='utf-8') as file:
        #     file.write(self.get_combined_text(["search_strategy"]))
        if not document or not isinstance(document, str):
            raise ValueError(
                "The document content is empty or invalid. Please provide a valid string.")

        # Define regex pattern for capturing dates
        pattern = r"""
            (?i)(?:(?:searched\s+from\s+inception\s+to|date\s+of\s+last\s+literature\s+search|last\s+search\s+date|
            the\s+search\s+was\s+conducted|all\s+searches\s+were\s+conducted|systematic\s+search(?:es)?|
            literature\s+search(?:es)?(?:\s+was|\s+were)?(?:\s+conducted|\s+performed)?|we\s+conducted|
            up\s+to\s+our\s+last\s+search\s+on|Cochrane\s+Database\s+of\s+Systematic\s+Reviews\s+up\s+to|
            retrievals?\s+were\s+implemented\s+by|published\s+studies\s+were\s+retrieved|search\s+strategy|
            the\s+last\s+automatic\s+search\s+was\s+performed\s+on|last\s+search\s+was\s+conducted\s+on|published\s+from|
            initially\s+retrieved\s+from|articles\s+were\s+also\s+identified\s+between|study\s+was\s+conducted|we\s+searched|
            was\s+conducted\s+on|database\s+inception\s+date|database\s+inception|articles\s+published\s+from|search\s+for\s+publications\s+was\s+carried\s+out|
            published\s+between|conducted\s+and|published\s+in\s+english\s+between|published\s+through|from\s+inception\s+until|from\s+inception\s+up\s+to|
            conducted\s+an\s+online\s+update\s+on|last\s+performed\s+on|literature\s+were\s+searched\s+in|literature\s+up\s+to|
            published\s+(?:before|until|up\s+to|prior\s+to)|articles?\s+published\s+(?:before|until|up\s+to|prior\s+to)))  # Keywords
            [\s\S]*  # Match across lines (greedy)
            (\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2}(?:,|\s)?\s+\d{4}|\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{4}|  # Date formats like "Jan 1, 1967", "Jan 1967", "January 1, 1967", or "January 1967"
            \b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s*(?:\d{1,2}(?:st|nd|rd|th)?)?,?\s*(\d{4})\b)
        """
        matches = re.finditer(pattern, document, re.IGNORECASE | re.VERBOSE)
        # Extract only the matched date or year
        dates = [match.group(1).strip() for match in matches if match.group(1)]
        # Deduplicate and sort the dates
        max_date = self.get_max_date(sorted(set(dates)))
        return max_date  # if max_date else self.extract_last_search_date()

    def extract_population(self, tag_lists):
        """
        Extracts population data comprehensively from a given text.

        Parameters:
            tag_lists (list): The list of keywords indicating population-related terms.

        Returns:
            list: A list of dictionaries where each dictionary contains:
                - "indicator": The matched population indicator.
                - "populations": The extracted population number.
                - "type_of_participant": Participant type(s) and counts.
                - "context": The matched text segment for reference.
        """
        results = []
        seen = set()  # To store unique (indicator, populations) tuples
        # self.get_combined_text(["abstract", "methods", "results"])
        document = self.document
        # Regex to match patterns where numbers come before participant-related keywords
        pattern = re.compile(
            rf"(\d[\d,]*)\s+({'|'.join(map(re.escape, [term for term, _ in tag_lists]))})\s*(?:\(([^)]+)\))?",
            flags=re.IGNORECASE
        )

        # Search for matches in the document
        matches = pattern.finditer(document)

        for match in matches:
            number_str = match.group(1)  # Extract the number
            numbers = int(number_str.replace(",", "")) if number_str else None
            indicator = match.group(2).strip()  # The population-related phrase
            participant_type_str = match.group(3)  # Extract participant types
            type_of_participant = participant_type_str.strip() if participant_type_str else None

            # Extract words for context
            before_context = document[:match.start()]
            after_context = document[match.end():]

            # Get the last 5 words before the match
            before_words = re.findall(r'\b\w+\b', before_context)
            before_context_words = " ".join(
                before_words[-5:])  # Get the last 5 words

            # Get the next 5 words after the match
            after_words = re.findall(r'\b\w+\b', after_context)
            after_context_words = " ".join(
                after_words[:5])  # Get the next 5 words

            # Combine before and after context
            context = f"{before_context_words} {match.group(0)} {after_context_words}".strip(
            )

            # Create a unique key for the current match
            unique_key = (indicator.lower(), numbers)

            # Add to results only if unique
            if unique_key not in seen:
                seen.add(unique_key)
                results.append({
                    "indicator": indicator,
                    "populations": numbers,
                    "type_of_participant": type_of_participant,
                    "context": context
                })

        return results

    def parse_participant_types(self, participant_type_str):
        """
        Parses subgroup details like 'males=1197, females=1906' into a dictionary.

        Args:
            participant_type_str (str): Subgroup details string.

        Returns:
            dict: Parsed subgroup details.
        """
        subgroups = {}
        pattern = re.compile(
            r"([a-zA-Z\s\-]+)\s*=\s*(\d+)", flags=re.IGNORECASE)
        matches = pattern.findall(participant_type_str)

        for match in matches:
            subgroup = match[0].strip().lower()
            count = int(match[1])
            subgroups[subgroup] = count

        return subgroups

    def process_age_group(self, age_range_key, term_list):
        """Process to detect and extract age-related terms in the document."""
        age_matches = []
        text_terms = []
        document = self.get_combined_text(["abstract", "methods", "results"])
        potential_age_ranges = self.find_potential_age_ranges()
        found_age_ranges = self.age_range_search_algorithm(
            potential_age_ranges)

        # Extract age range values from the key and find overlapping ranges
        age_values = list(map(int, re.findall(r'\d+', age_range_key)))
        overlapping_ranges = self.find_overlapping_groups(
            age_values, found_age_ranges)

        # Append each unique overlapping range as a list (avoid duplicates)
        unique_ranges = {tuple(range_item)
                         for range_item in overlapping_ranges}
        age_matches.extend([list(rng) for rng in unique_ranges])

        # Add age range label text (formatted) as a single entry, if any ranges were found
        if overlapping_ranges:
            text_terms = [age_range_key.replace("__", " ")]

        # Collect matched terms for the text part, ensuring no duplicates
        for term, abbreviation in term_list:
            if re.search(fr'\b{term}\b', document, re.IGNORECASE):
                text_terms.append(f"{term}:{abbreviation}")

        # Append all unique text terms as a single list at the end of `age_matches`
        if text_terms:
            age_matches.append(list(set(text_terms)))

        return age_matches

    def extract_aggregated_relevant_study_counts(self, document: str, inclusion_terms: list[str]) -> list[int]:
        """
        Extracts counts from multiple related clauses *only when contextually related to studies*.
        """
        relevant_counts = []
        study_context = re.compile(
            r"(stud(?:y|ies)|trial|RCT|record|cohort|case[-\s]?control)", re.IGNORECASE)
        sentences = re.split(r'(?<=[\.\?!])\s+', document)

        for sent in sentences:
            if any(term in sent.lower() for term in inclusion_terms) and study_context.search(sent):
                matches = re.findall(
                    r'(\d+)\s+(?:records\s+from\s+)?(?:\d+\s+)?(?:eligible\s+)?(?:stud(?:y|ies)|RCTs?|trials?|cohort|case[-\s]?control)',
                    sent, flags=re.IGNORECASE
                )
                for m in matches:
                    relevant_counts.append(int(m))

        return relevant_counts

    def process_study_count(self, term_list):
        """
        Dynamically extracts study and RCT counts from the document, handling number combinations in both words and digits.
        Logs contextual information around each match for validation.
        """
        from collections import defaultdict
        import re

        raw_counts = defaultdict(set)
        context_log = []

        # Map terms to their category (e.g., "randomized controlled trial" -> "rct")
        replacements = {term.lower(): category for term, category in term_list}
        categories = set(replacements.values())

        # Full text from document
        complete_document = self.get_combined_text(["abstract", "results"])
        document = complete_document

        # Replace category terms in the document with standard category tags
        for phrase, replacement in replacements.items():
            pattern = re.compile(re.escape(phrase), flags=re.IGNORECASE)
            document = pattern.sub(replacement, document)

        # Word and multiplier mappings for written numbers
        word_to_number = {
            "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
            "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
            "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
            "eighteen": 18, "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40,
            "fifty": 50, "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90
        }
        multipliers = {
            "hundred": 100,
            "thousand": 1000,
            "million": 1_000_000,
            "billion": 1_000_000_000,
            "trillion": 1_000_000_000_000
        }

        def parse_written_number(phrase):
            """Convert a written number phrase to its numeric value."""
            words = re.split(r"[-\s]", phrase.lower())
            total = 0
            current = 0

            for word in words:
                if word in word_to_number:
                    current += word_to_number[word]
                elif word in multipliers:
                    if current == 0:
                        current = 1
                    current *= multipliers[word]
                    total += current
                    current = 0
            total += current
            return total

        # Patterns to match numbers followed by category keywords
        category_pattern = "|".join(re.escape(cat) for cat in categories)
        digit_pattern = re.compile(
            rf"\b(\d+)\s*({category_pattern})s?\b", re.IGNORECASE)
        written_pattern = re.compile(
            rf"\b((?:\w+(?:[-\s]\w+)*))\s*({category_pattern})s?\b", re.IGNORECASE)

        def extract_context_around(match, window=40):
            """Returns the context before and after the match within a specified character window."""
            start, end = match.start(), match.end()
            context_before = document[max(0, start - window):start].strip()
            context_after = document[end:end + window].strip()
            return context_before, match.group(0), context_after

        # Extract digit-based matches with context
        for match in digit_pattern.finditer(document):
            count = int(match.group(1))
            cat = match.group(2).lower()
            context_before, matched_text, context_after = extract_context_around(
                match)

            context_log.append({
                "count": count,
                "category": cat,
                "context_before": context_before,
                "matched_text": matched_text,
                "context_after": context_after
            })

            raw_counts[cat].add(count)

        # Extract written number matches with context
        for match in written_pattern.finditer(document):
            phrase = match.group(1)
            cat = match.group(2).lower()
            count = parse_written_number(phrase)
            context_before, matched_text, context_after = extract_context_around(
                match)

            context_log.append({
                "count": count,
                "category": cat,
                "written_phrase": phrase,
                "context_before": context_before,
                "matched_text": matched_text,
                "context_after": context_after
            })

            raw_counts[cat].add(count)

        # Identify relevant "study" inclusion terms
        inclusion_terms = [term.lower()
                           for term, category in term_list if category == "sty"]

        # Use helper to extract additional context-aware study counts
        relevant_counts = self.extract_aggregated_relevant_study_counts(
            document, inclusion_terms)
        all_study_counts = list(raw_counts["sty"].union(relevant_counts))

        return (
            all_study_counts,
            sum(all_study_counts),
            sum(raw_counts["rct"]),
            list(raw_counts["rct"]),
            sum(raw_counts["nrsi"]),
            sum(raw_counts["mmtd"]),
            sum(raw_counts["quanti"])
        )

    def extract_databases(self):
        """
        Extracts databases used in a systematic review with context validation.
        Returns a tuple: (list_of_databases, count_of_databases).
        """
        # Comprehensive database list with common name variations
        databases = [
            "PubMed", "MEDLINE", "Embase", "Web of Science", "Scopus", "CINAHL", "Cochrane Library", "CENTRAL", "PubMed Central",
            "LILACS", "Google Scholar", "ProQuest", "EBSCO", "Ovid", "PsycINFO", "AMED", "ClinicalTrials.gov", "MEDLINE", "PubMed/MEDLINE", 
            "EMBASE", "Web of Science", "Scopus", "CINAHL", "Cochrane Library", "CENTRAL", "PubMed Central (PMC)", "LILACS", "Google Scholar", 
            "BIOSIS", "EBSCO", "ProQuest", "Ovid", "PsycINFO", "AMED", "TOXLINE", "CANCERLIT", "HMIC", "POPLINE", "Global Health", "CAB Abstracts", 
            "AGRICOLA", "GeoRef", "ASSIA", "Social Services Abstracts", "Sociological Abstracts", "EconLit", "ERIC", "PAIS Index", "IBSS",
            "UpToDate", "DynaMed", "Clinical Key", "BMJ Best Practice", "Cochrane Clinical Answers", "TRIP Database", "NICE Evidence Search",
            "DrugBank", "PharmGKB", "RxList", "Martindale", "AHFS Drug Information", "OMIM", "GenBank", "Gene", "GEO", "UniProt",
            "ClinicalTrials.gov", "EU Clinical Trials Register", "ISRCTN Registry", "WHO ICTRP", "Australian New Zealand Clinical Trials Registry",
            "Epistemonikos", "Health Evidence", "Campbell Collaboration Library", "3ie Database of Systematic Reviews", "OpenGrey", "GreyNet", "NTIS", "CORDIS",
            "ProQuest Dissertations & Theses Global", "EThOS", "DART-Europe", "Conference Proceedings Citation Index", "IEEE Xplore",
            "Google Patents", "Espacenet", "USPTO", "Directory of Open Access Journals (DOAJ)", "PLoS", "BioMed Central", "arXiv", "medRxiv", "bioRxiv",
            "OpenDOAR", "BASE (Bielefeld Academic Search Engine)", "CINAHL Complete", "MEDLINE Complete", "SocINDEX", "SPORTDiscus", "PEDro", "OTseeker", 
            "SpeechBITE", "PsycARTICLES", "PsycBOOKS", "Wiley Online Library", "ScienceDirect", "SpringerLink", "JSTOR", "Taylor & Francis Online", 
            "Sage Journals", "Oxford Academic", "Cambridge Core", "Nature.com", "Science Magazine", "CDC Stacks", "NIH.gov", "WHO.int", "WorldBank.org", 
            "UN iLibrary", "Business Source Complete", "ABI/INFORM", "Factiva", "LexisNexis", "Westlaw", "HeinOnline", "Nexis Uni", "Factiva", 
            "ProQuest News & Newspapers", "Academic Search Complete", "JSTOR", "Project MUSE"
        ]

        # Common aliases and alternative spellings
        aliases = {
            "MEDLINE": ["MEDLINE via Ovid", "MEDLINE/PubMed"],
            "Embase": ["Excerpta Medica"],
            "CENTRAL": ["Cochrane Central Register of Controlled Trials"],
            "PsycINFO": ["Psychological Abstracts"],
            "CINAHL": ["Cumulative Index to Nursing and Allied Health Literature"]
        }

        # Extract text from relevant sections
        text = self.get_combined_text(["abstract", "methods"])

        # Preprocess text
        text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
        text = text.lower()  # Normalize case

        # Context-aware trigger phrases to identify database mentions
        trigger_phrases = [
            r'\b(systematic\s+)?search(?:es)?\s+(?:were|was)?\s*(?:conducted|performed|carried\s+out)?',
            r'\b(?:databases?|data\s+sources?)\s+(?:used|searched|included|screened)',
            r'\bsearch\s+(?:strategy|methodology|approach)',
            r'\b(?:literature|database)\s+search',
            r'\b(?:conducted|performed)\s+a\s+(?:database|literature)?\s*search',
            r'\b(?:screened|searched)\s+(?:databases?|sources)',
            r'\b(?:electronic|online)\s+databases?',
            r'\b(?:We)\s+searched?',
            r'\b(?:searches|strategies)\s+(?:included|used|involved)'
        ]
        # Combined database pattern
        db_pattern = r'\b(' + '|'.join(re.escape(db.lower())
                                       for db in databases) + r')\b'

        found_databases = set()

        # Check if database mentions appear near trigger phrases
        for trigger in trigger_phrases:
            matches = re.finditer(trigger, text, re.IGNORECASE)
            for match in matches:
                # Extract context around the trigger phrase
                start = max(0, match.start() - 200)
                end = min(len(text), match.end() + 200)
                context_window = text[start:end]

                # Find explicit database mentions within this context
                found_databases.update(re.findall(db_pattern, context_window))

                # Resolve aliases to standard database names
                for alias, variations in aliases.items():
                    for variation in variations:
                        if variation.lower() in context_window:
                            found_databases.add(alias.lower())

        # Normalize capitalization and remove duplicates
        normalized_databases = {db.capitalize() for db in found_databases}

        return sorted(normalized_databases), len(normalized_databases)

    def process_generic_terms(self, term_list):
        """Extract general terms based on the provided list."""
        generic_matches = []
        for term, abbreviation in term_list:
            if re.search(fr'\b{term}\b', self.document, re.IGNORECASE):
                generic_matches.append(f"{term}:{abbreviation}")
        return list(set(generic_matches))

    def extract_ve_related_info(self, keywords_list):
        text = self.get_combined_text(["abstract", "methods", "results"])

        # Define target terms
        effect_terms = keywords_list

        keywords = [re.escape(term) for term, _ in effect_terms]

        pattern = re.compile(
            r'\b(?:' + '|'.join(keywords) +
            r')\b(?:[^.:\n]{0,100})?(?:\d{1,3}\.?\d{0,2}%?|\(.*?\)|−?\d+\.\d+\s*–\s*−?\d+\.\d+|95% CI.*?)',
            flags=re.IGNORECASE
        )

        found = set()

        # Regex pattern matches
        for match in re.finditer(pattern, text):
            span = match.group().strip()
            for term, label in effect_terms:
                if term.lower() in span.lower():
                    found.add(f"{span}:{label}")
                    break

        return sorted(found)

    def extract_study_types(self, terms_list):
        """
        Extract study types and their counts from text.
        Returns a dictionary with study types and counts.
        """
        document = self.get_combined_text(["main_content"])
        study_terms = [item for item, _ in terms_list]

        # Regex to match study types and their counts
        study_pattern = re.compile(
            rf"(\d+)\s*({'|'.join(re.escape(term) for term in study_terms)})",
            flags=re.IGNORECASE
        )

        matches = study_pattern.findall(document)

        study_types = {}
        for count, study_type in matches:
            study_type_lower = study_type.lower()
            if study_type_lower in study_types:
                study_types[study_type_lower] += int(count)
            else:
                study_types[study_type_lower] = int(count)

        return json.dumps(study_types)

    def is_open_access(self, term_list):
        document = self.get_combined_text(["paper_type"]).lower()
        open_access_keywords = term_list
        for keyword, label in open_access_keywords:
            if keyword in document:
                return f"{keyword}: {label}"
        return ""

    # Utility Functions
    def find_potential_age_ranges(self):
        document = self.get_combined_text(["abstract", "methods", "results"])
        placeholder = r'\d{1,3}'
        pattern = rf'\b(?:ages {placeholder} to {placeholder}|ages {placeholder}-{placeholder}|{placeholder} to {placeholder} years|{placeholder}to{placeholder} yrs|{placeholder}-{placeholder} yrs|{placeholder}-{placeholder} years|{placeholder} - {placeholder} years|{placeholder} - {placeholder} yrs|less than {placeholder} year|less than {placeholder} years|less than {placeholder} yrs|{placeholder} years|{placeholder} yrs|{placeholder} age)\b'
        return re.finditer(pattern, document, flags=re.IGNORECASE)

    def age_range_search_algorithm(self, matches):
        numerical_values_and_operators = []

        for match in matches:
            match_values = re.findall(
                r'(less than|greater than|<|>|\d+)(?:-(\d+))?', match.group(), flags=re.IGNORECASE)

            if match_values:
                operator = "="  # Default operator
                start, end = None, None

                # Interpret values
                if match_values[0][0].isdigit():
                    # Direct age range like "5-10 years"
                    start = int(match_values[0][0])
                    # Handle cases like "5 years"
                    end = int(match_values[0][1]
                              ) if match_values[0][1] else start

                elif match_values[0][0].lower() in ['less than', '<']:
                    if match_values[0][1]:  # Check if there is a number after "less than"
                        start, end = 0, int(match_values[0][1])
                        operator = "<"

                elif match_values[0][0].lower() in ['greater than', '>']:
                    if match_values[0][1]:  # Check if there is a number after "greater than"
                        start = int(match_values[0][1]) + 1
                        end = 1000000  # Arbitrary large number to represent 'no upper limit'
                        operator = ">"

                # Ensure both start and end are set correctly
                if start is not None and end is not None:
                    numerical_values_and_operators.append(
                        [start, end, operator])

        return numerical_values_and_operators

    def parse_match_values(self, match_values):
        if match_values[0].isdigit():
            start = int(match_values[0])
            end = int(match_values[1]) if match_values[1] else start
            operator = "="
        elif match_values[0].lower() in ['less than', '<']:
            start, end, operator = 0, int(match_values[1]), "<"
        elif match_values[0].lower() in ['greater than', '>']:
            start, end, operator = int(match_values[1]) + 1, 1000000, ">"
        else:
            start, end, operator = None, None, "="
        return start, end, operator

    def find_overlapping_groups(self, check_range, list_of_ranges):
        overlapping_ranges = []
        check_start, check_end = check_range

        for start, end, operator in list_of_ranges:
            if operator == "=" and start <= check_end and end >= check_start:
                overlapping_ranges.append([start, end, operator])
            elif operator == "<" and end >= check_start:
                overlapping_ranges.append([start, end, operator])
            elif operator == ">" and start <= check_end:
                overlapping_ranges.append([start, end, operator])

        return overlapping_ranges

    def clean_result(self, result_columns):
        cleaned_result = {}
        for col, values in result_columns.items():
            if isinstance(values, list):
                cleaned_values = [v for v in values if v]
                if cleaned_values:
                    cleaned_result[col] = cleaned_values
            elif isinstance(values, dict):
                relevant_values = {k: v for k, v in values.items() if v}
                if relevant_values:
                    cleaned_result[col] = relevant_values
            else:
                cleaned_result[col] = values
        return cleaned_result

    def word_to_number(self, word):
        number_map = {
            "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
            "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
            "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
            "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
            "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40,
            "fifty": 50, "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
            "hundred": 100, "thousand": 1000
        }

        words = word.lower().split()
        total = 0
        current = 0

        for w in words:
            if w in number_map:
                if w == "hundred" or w == "thousand":
                    current *= number_map[w]
                else:
                    current += number_map[w]
            else:
                if current:
                    total += current
                    current = 0

        return total + current if total + current > 0 else int(word) if word.isdigit() else None

    def extract_countries_with_total_count(self):
        """
            Extracts country names followed by numbers in parentheses, inline counts, 
            and converts written numbers to digits. Returns formatted string and total count.
        """
        country_names = {country.name for country in pycountry.countries}

        pattern = re.compile(
            # USA (11, 45.8%)
            r"([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\s*\(\s*(\d+)(?:,\s*\d+\.\d+%)?\s*\)"
            r"|\b(one|two|three|four|five|six|seven|eight|nine|ten|"
            r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|"
            r"nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|\d+)"
            # five (19.2%) in Germany
            r"\s*\(\d+\.\d+%\)\s*in\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)"
            r"|\b(one|two|three|four|five|six|seven|eight|nine|ten|"
            r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|"
            r"nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|\d+)"
            # 100 from/in Germany
            r"\s+(?:from|in)\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)"
        )

        country_counts = {}

        for match in pattern.findall(self.document):
            country, num = None, None

            if match[0] and match[1]:  # "USA (11, 45.8%)"
                country, num = match[0], int(match[1])
            elif match[2] and match[3]:  # "five (19.2%) in Germany"
                country = match[3]
                num = self.word_to_number(match[2])
            elif match[4] and match[5]:  # "10 in/from Nigeria"
                country = match[5]
                num = self.word_to_number(match[4])

            if country and num is not None and country in country_names:
                country_counts[country] = country_counts.get(country, 0) + num

        formatted_output = ", ".join(
            f"{country}({count})" for country, count in country_counts.items())
        total_count = sum(country_counts.values())

        return formatted_output, total_count

    def convert_dict_to_dataframe(self, data_dict):
        """Convert final results into a DataFrame for analysis or storage."""
        return pd.DataFrame([data_dict])

    def pubmed_bert_integration(self, document):
        MODEL_PATH = "./results/baseline/best_model"
        if not os.path.isdir(MODEL_PATH):
            print(f"Model path not found: {MODEL_PATH}")
            print("Please ensure the model is trained and saved to the specified path.")
        else:
            print(f"Model path found: {MODEL_PATH}")
            tester = NERTester(model_path=MODEL_PATH)

            if tester.pipeline:
                return run_ner_inference(tester, document)
            else:
                print("Failed to initialize NER pipeline. Check model files.")
                return {}
    
    def amstar2_integration(self):
        today = date.today()
        date_str = today.strftime("%Y-%m-%d")
        context = self.document
        checker = Amstar2(review_date=date_str)
        results = checker.evaluate_all(context)
        summary = checker.amstar_label_and_flaws(results)
        update_dict = checker.prepare_amstar_update_dict(results, summary)
        return update_dict
