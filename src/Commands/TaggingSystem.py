import re
import json
import pandas as pd
from word2number import w2n
from dateutil.parser import parse
from collections import defaultdict
from src.Services.Factories.Sections.SectionExtractor import SectionExtractor

class Tagging:
    def __init__(self, document):
        self.document = document
        self.sections = SectionExtractor(self.document)
        self.result_columns = defaultdict(list)
    
    def get_combined_text(self, sections):
        contents = " ".join(self.sections.get(section, "") for section in sections).strip()
        if contents and contents != "":
            return contents
        else:
            return self.document

    def create_columns_from_text(self, searchRegEx):
        """Main function to apply tagging based on the extensive regex structure provided."""
        for category, subcategories in searchRegEx.items():
            for subcategory, terms_dict in subcategories.items():
                for term_key, term_list in terms_dict.items():
                    column_name = f"{category}#{subcategory}#{term_key}"
                    countries, total_count = self.extract_countries_with_total_count()
                    if category == "popu" and subcategory == "age__group":
                        self.result_columns[column_name] = self.process_age_group(term_key, term_list)
                    elif category == "studies" and (subcategory == "studie__no" or subcategory == "rct"):
                        self.result_columns[column_name], total_study_count, total_rct_count, raw_rct_counts, total_nrsi_count, total_mmtd_count, total_quanti_count, fixed_counts = self.process_study_count(term_list)
                        self.result_columns["study_types"] = self.extract_study_types(term_list)
                        self.result_columns["total_study_count"] = total_study_count
                        self.result_columns["total_rct_count"] = total_rct_count
                        self.result_columns["RCT_counts"] = raw_rct_counts
                        self.result_columns["inclusions_exclusions"] = fixed_counts
                        
                        self.result_columns["total_nrsi_count"] = total_nrsi_count
                        self.result_columns["total_mmtd_count"] = total_mmtd_count
                        self.result_columns["total_quanti_count"] = total_quanti_count
                    elif category == "gender" and subcategory == "group":
                        self.result_columns[column_name] = self.process_sex_distribution(term_list)
                    elif category == "topic" and subcategory == "eff":
                        self.result_columns[column_name] = self.extract_ve_related_info(term_list)
                    elif category == "particip" and subcategory == "group":
                        self.result_columns[column_name] = self.extract_population(term_list)
                    elif category == "lit_search_dates" and subcategory == "dates":
                        self.result_columns[column_name] = self.extract_last_literature_search_dates()
                    elif category == 'open_acc' and subcategory == "opn_access":
                        self.result_columns[column_name] = self.is_open_access(term_list)
                    elif (category == 'study_country' and subcategory == "countries"):
                        self.result_columns[column_name] = countries
                    elif (category == 'study_country' and subcategory == "study_count"):
                        self.result_columns[column_name] = total_count
                    elif (category == 'title_popu' and subcategory == "title_pop"):
                        self.result_columns[column_name] = self.extract_population_from_title(term_list)
                    else:
                        self.result_columns[column_name] = self.process_generic_terms(term_list)
        print(self.clean_result(self.result_columns))
        return self.clean_result(self.result_columns)

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
        document = self.get_combined_text(["abstract", "search_strategy", "methods"])
        with open("text.txt", 'w', encoding='utf-8') as file:
            file.write(self.get_combined_text(["search_strategy"]))
        # print(self.get_combined_text(["main_content"]))
        # with open("from_tagging_class.txt", 'w', encoding='utf-8') as file:
        #     file.write(self.get_combined_text(["main_content"]))
        # Check if document is valid
        if not document or not isinstance(document, str):
            raise ValueError("The document content is empty or invalid. Please provide a valid string.")

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
        return max_date
    
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
        document = self.document # self.get_combined_text(["abstract", "methods", "results"])
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
            before_context_words = " ".join(before_words[-5:])  # Get the last 5 words

            # Get the next 5 words after the match
            after_words = re.findall(r'\b\w+\b', after_context)
            after_context_words = " ".join(after_words[:5])  # Get the next 5 words

            # Combine before and after context
            context = f"{before_context_words} {match.group(0)} {after_context_words}".strip()

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
        pattern = re.compile(r"([a-zA-Z\s\-]+)\s*=\s*(\d+)", flags=re.IGNORECASE)
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
        found_age_ranges = self.age_range_search_algorithm(potential_age_ranges)
        
        # Extract age range values from the key and find overlapping ranges
        age_values = list(map(int, re.findall(r'\d+', age_range_key)))
        overlapping_ranges = self.find_overlapping_groups(age_values, found_age_ranges)

        # Append each unique overlapping range as a list (avoid duplicates)
        unique_ranges = {tuple(range_item) for range_item in overlapping_ranges}
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

    def extract_inclusion_exclusion_counts(self, text):
        """
        Extracts study counts using comprehensive regex patterns.
        Returns a dictionary with keys: total_studies, yielded, screened, duplicates, eligible, selected, excluded, included.
        """
        patterns = {
            # Total studies identified (Initial Search)
            "total_studies": re.compile(
                r"A\s+total\s+of\s+(\d+)\s+(?:studies|records|articles|citations|publications|reports|references|observational studies)", 
                re.IGNORECASE
            ),
            "yielded": re.compile(
                r"(?:identified|retrieved|yielded|sourced|found)\s+(\d+)\s+(?:studies|records|articles|citations|publications|reports|references|observational studies)", 
                re.IGNORECASE
            ),
            
            # Studies screened for eligibility
            "screened": re.compile(
                r"(?:screened|assessed|reviewed|evaluated)\s+(\d+)\s+(?:studies|records|articles|citations|publications|reports|references|observational studies)", 
                re.IGNORECASE
            ),

            # Duplicates removed
            "duplicates": re.compile(
                r"(\d+)\s+(?:duplicates|duplicate records|duplicate studies|duplicate articles|redundant studies|redundant records)|"
                r"(?:duplicates|redundant studies|redundant records)\s+\(?(\d+)\)?", 
                re.IGNORECASE
            ),
            "duplicates_of_which": re.compile(
                r"of\s+which\s+(\d+)\s+were\s+duplicates", 
                re.IGNORECASE
            ),
            
            # Studies eligible after screening
            "eligible": re.compile(
                r"(\d+)\s+(?:studies|records|articles|publications|references|observational studies)\s+were\s+(?:eligible|qualified|considered\s+for\s+inclusion)", 
                re.IGNORECASE
            ),

            # Studies selected for inclusion
            "selected": re.compile(
                r"(?:selected|included in the analysis|included for full review)\s+(\d+)", 
                re.IGNORECASE
            ),
            
            # Studies excluded (after screening or eligibility assessment)
            "excluded": re.compile(
                r"(?:excluded|were excluded|exclusion of|did not meet criteria|removed|discarded)\s+(\d+)|"
                r"(\d+)\s+were\s+excluded", 
                re.IGNORECASE
            ),

            # Final inclusion count
            "included": re.compile(
                r"A\s+total\s+of\s+(\d+)\s+(?:observational\s+studies|studies)\s+that\s+met\s+the\s+inclusion\s+criteria\s+were\s+included|"
                r"(\d+)\s+met\s+the\s+inclusion\s+criteria", 
                re.IGNORECASE
            ),
            "inclusion_of_which": re.compile(
                r"of\s+which\s+(\d+)\s+met\s+the\s+inclusion\s+criteria", 
                re.IGNORECASE
            ),
        }

        results = {key: None for key in patterns.keys()}

        # Extract counts using regex patterns
        for key, pattern in patterns.items():
            match = pattern.search(text)
            if match:
                groups = [g for g in match.groups() if g is not None]
                if groups:
                    results[key] = int(groups[0])

        # Fallback handling
        if results["included"] is None and results["inclusion_of_which"] is not None:
            results["included"] = results["inclusion_of_which"]

        if results["duplicates"] is None:
            if results["duplicates_of_which"] is not None:
                results["duplicates"] = results["duplicates_of_which"]
            else:
                identified_duplicates = re.search(r"identified\s+(\d+)\s+duplicates", text, re.IGNORECASE)
                if identified_duplicates:
                    results["duplicates"] = int(identified_duplicates.group(1))

        # Ensure total_studies is captured when yielded is missing
        if results["yielded"] is None and results["total_studies"] is not None:
            results["yielded"] = results["total_studies"]

        # Final dictionary structure
        final_results = {
            "total_studies": results["total_studies"],
            "yielded": results["yielded"],
            "screened": results["screened"],
            "duplicates": results["duplicates"],
            "eligible": results["eligible"],
            "selected": results["selected"],
            "excluded": results["excluded"],
            "included": results["included"]
        }

        return json.dumps(final_results)

    
    def process_study_count(self, term_list):
        """
        Dynamically extracts study and RCT counts from the document, handling any number combinations in words or digits.
        """
        raw_study_counts = []
        raw_rct_counts = []
        raw_nrsi_counts = []
        raw_mmtd_counts = []
        raw_quanti_counts = []

        # Construct replacements dictionary dynamically from term_list
        replacements = {term.lower(): category for term, category in term_list}
        
        complete_document = self.get_combined_text(["main_content"])
        
        for phrase, replacement in replacements.items():
            document = re.sub(phrase, replacement, complete_document, flags=re.IGNORECASE)
        
        # Extract fixed phrase counts
        fixed_counts = self.extract_inclusion_exclusion_counts(document)
        
        # Define word-to-number mappings
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
            "million": 1000000,
            "billion": 1000000000,
            "trillion": 1000000000000
        }
        
        # Precompile regex patterns
        digit_pattern = re.compile(rf"\b(\d+)\s*({'|'.join(replacements.keys())})s?\b", flags=re.IGNORECASE)
        written_pattern = re.compile(
            rf"\b((?:\w+(?:[-\s]\w+)*))\s*({'|'.join(replacements.keys())})s?\b", flags=re.IGNORECASE
        )
        def parse_written_number(phrase):
            """
            Parses a phrase containing written numbers and calculates its numeric value dynamically.
            """
            words = re.split(r"[-\s]", phrase.lower())
            total = 0
            current = 0

            for word in words:
                if word in word_to_number:
                    current += word_to_number[word]
                elif word in multipliers:
                    current *= multipliers[word]
                    total += current
                    current = 0
                else:
                    continue  # Ignore unrecognized words

            total += current
            return total
        
        def extract_numbers():
            """
            Extract numbers (digits and written) from the document and append them to the respective lists.
            """
            # Extract digit-based numbers
            for match in digit_pattern.findall(document):
                count, matched_term = int(match[0]), replacements[match[1].lower()]
                if matched_term == "rct":
                    raw_rct_counts.append(count)
                elif matched_term == "nrsi":
                    raw_nrsi_counts.append(count)
                elif matched_term == "mmtd":
                    raw_mmtd_counts.append(count)
                elif matched_term == "quanti":
                    raw_quanti_counts.append(count)
                else:
                    raw_study_counts.append(count)
        
            # Extract written numbers
            for match in written_pattern.findall(document):
                phrase, matched_term = match
                count = parse_written_number(phrase)
                if count > 0:
                    matched_term = replacements[matched_term.lower()]
                    if matched_term == "rct":
                        raw_rct_counts.append(count)
                    elif matched_term == "nrsi":
                        raw_nrsi_counts.append(count)
                    elif matched_term == "mmtd":
                        raw_mmtd_counts.append(count)
                    elif matched_term == "quanti":
                        raw_quanti_counts.append(count)
                    else:
                        raw_study_counts.append(count)
        
        # Perform extraction
        extract_numbers()
        
        # Deduplicate and calculate totals
        raw_study_counts = list(set(raw_study_counts))
        raw_rct_counts = list(set(raw_rct_counts))
        raw_nrsi_counts = list(set(raw_nrsi_counts))
        raw_mmtd_counts = list(set(raw_mmtd_counts))
        raw_quanti_counts = list(set(raw_quanti_counts))
        total_study_count = sum(raw_study_counts)
        total_rct_count = sum(raw_rct_counts)
        total_nrsi_count = sum(raw_nrsi_counts)
        total_mmtd_count = sum(raw_mmtd_counts)
        total_quanti_count = sum(raw_quanti_counts)
        
        return raw_study_counts, total_study_count, total_rct_count, raw_rct_counts, total_nrsi_count, total_mmtd_count, total_quanti_count, fixed_counts

    def process_generic_terms(self, term_list):
        """Extract general terms based on the provided list."""
        generic_matches = []
        for term, abbreviation in term_list:
            if re.search(fr'\b{term}\b', self.document, re.IGNORECASE):
                generic_matches.append(f"{term}:{abbreviation}")
        return list(set(generic_matches))

    def extract_ve_related_info(self, keywords_list):
        """
        Extracts vaccine efficacy (VE) and related information from a document using a list of keywords.

        Parameters:
            document (str): The input text document.
            keywords_list (list): A list of keywords to search for in the document.

        Returns:
            list: A list of dictionaries where each dictionary contains:
                - "keyword": The matched keyword.
                - "VE": The vaccine efficacy percentage.
                - "CI": A list with the lower and upper bounds of the confidence interval.
                - "context": The matched text segment for reference.
        """
        results = []
        document = self.get_combined_text(["abstract", "methods", "results"])
        # Ensure keywords are valid and escape special characters
        if not keywords_list:
            print("No keywords provided.")
            return results

        enriched_keywords = [re.escape(keyword) for keyword, _ in keywords_list]

        # Construct regex pattern
        pattern = re.compile(
            rf"({'|'.join(enriched_keywords)})\s+was\s+(\d+\.\d+)%\s+\((\d+\.\d+)-(\d+\.\d+)\)",
            flags=re.IGNORECASE
        )

        # Validate document content
        if not document:
            print("Document is empty.")
            return results

        # Search for matches in the document
        matches = pattern.findall(document)

        # Process matches
        for match in matches:
            try:
                keyword = match[0].strip()  # Extract the matched keyword
                ve = float(match[1])  # Vaccine efficacy percentage
                ci = [float(match[2]), float(match[3])]  # Confidence interval
                context = f"{keyword} was {ve}% ({ci[0]}-{ci[1]})"
                results.append({"keyword": keyword, "VE": ve, "CI": ci, "context": context})
            except Exception as e:
                print(f"Error processing match {match}: {e}")

        return results

    
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

    def process_sex_distribution(self, gender_terms_list):
        """
        Extracts and processes sex distribution data for all possible gender-related terms.

        Parameters:
            document (str): Text containing sex distribution data.

        Returns:
            list: A list of dictionaries where each dictionary represents a group with its distribution.
        """

        # Create a regex pattern to capture percentages and gender terms
        pattern = re.compile(
            rf"(\d+)%\s*({'|'.join([gender_item for gender_item, _ in gender_terms_list])})",  # Match percentages and gender terms
            flags=re.IGNORECASE
        )

        sex_distributions = []
        document = self.get_combined_text(["main_content"])
        # Split the document into sections assuming newlines separate groups
        groups = document.split("\n\n")

        for group in groups:
            distribution = {}
            matches = pattern.findall(group)

            for percentage, gender in matches:
                # Normalize the gender label to lowercase
                gender = gender.lower()
                distribution[gender] = int(percentage)

            if distribution:  # Add only non-empty distributions
                sex_distributions.append(distribution)
        
        return sex_distributions

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
            match_values = re.findall(r'(less than|greater than|<|>|\d+)(?:-(\d+))?', match.group(), flags=re.IGNORECASE)
            
            if match_values:
                operator = "="  # Default operator
                start, end = None, None
                
                # Interpret values
                if match_values[0][0].isdigit():
                    # Direct age range like "5-10 years"
                    start = int(match_values[0][0])
                    end = int(match_values[0][1]) if match_values[0][1] else start  # Handle cases like "5 years"
                    
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
                    numerical_values_and_operators.append([start, end, operator])

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
        import pycountry
        """
            Extracts country names followed by numbers in parentheses, inline counts, 
            and converts written numbers to digits. Returns formatted string and total count.
        """
        country_names = {country.name for country in pycountry.countries}

        pattern = re.compile(
            r"([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\s*\(\s*(\d+)(?:,\s*\d+\.\d+%)?\s*\)"  # USA (11, 45.8%)
            r"|\b(one|two|three|four|five|six|seven|eight|nine|ten|"
            r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|"
            r"nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|\d+)"
            r"\s*\(\d+\.\d+%\)\s*in\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)"  # five (19.2%) in Germany
            r"|\b(one|two|three|four|five|six|seven|eight|nine|ten|"
            r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|"
            r"nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|\d+)"
            r"\s+(?:from|in)\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)"  # 100 from/in Germany
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

        formatted_output = ", ".join(f"{country}({count})" for country, count in country_counts.items())
        total_count = sum(country_counts.values())

        return formatted_output, total_count
    
    def convert_dict_to_dataframe(self, data_dict):
        """Convert final results into a DataFrame for analysis or storage."""
        return pd.DataFrame([data_dict])
