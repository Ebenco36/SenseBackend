import re
import pandas as pd
from word2number import w2n
from collections import defaultdict

class Tagging:
    def __init__(self, document):
        self.document = document.lower()
        self.result_columns = defaultdict(list)

    def create_columns_from_text(self, searchRegEx):
        """Main function to apply tagging based on the extensive regex structure provided."""
        for category, subcategories in searchRegEx.items():
            for subcategory, terms_dict in subcategories.items():
                for term_key, term_list in terms_dict.items():
                    column_name = f"{category}#{subcategory}#{term_key}"
                    if category == "Population" and subcategory == "AgeGroup":
                        self.result_columns[column_name] = self.process_age_group(term_key, term_list)
                    elif category == "Studies" and subcategory == "No__Of__Studies":
                        self.result_columns[column_name], total_study_count, total_rct_count, rct_counts = self.process_study_count(term_list)
                        self.result_columns["total_study_count"] = total_study_count
                        self.result_columns["total_rct_count"] = total_rct_count
                        self.result_columns["RCT_counts"] = rct_counts
                        # print(self.result_columns["total_study_count"], self.result_columns["total_rct_count"])
                    elif category == "Gender" and subcategory == "Group":
                        self.result_columns[column_name] = self.process_sex_distribution(term_list)
                    elif category == "Topic" and subcategory == "Efficacy__Effectiveness":
                        self.result_columns[column_name] = self.extract_ve_related_info(term_list)
                    elif category == "Population" and subcategory == "Group":
                        self.result_columns[column_name] = self.extract_population(term_list)
                    else:
                        self.result_columns[column_name] = self.process_generic_terms(term_list)
        
        return self.clean_result(self.result_columns)

    def extract_population(self, tag_lists):
        """
        Extracts population data from a given text.

        Parameters:
            document (str): The input text document.

        Returns:
            list: A list of dictionaries where each dictionary contains:
                - "indicator": The matched population indicator.
                - "populations": A list of extracted population numbers.
                - "context": The matched text segment for reference.
        """
        results = []

        # List of possible population-related indicators
        indicators = tag_lists
        
        # Regex pattern for capturing population numbers
        pattern = re.compile(
            rf"({'|'.join(indicators)})[^\d]*(\d+(?:,\s*\d+)*)",
            flags=re.IGNORECASE
        )
        document = self.document
        # Search for matches in the document
        matches = pattern.findall(document)

        for match in matches:
            indicator = match[0].strip()  # The population-related phrase
            numbers = [int(num.strip()) for num in match[1].split(",")]  # Extract population numbers
            context = f"{indicator}: {', '.join(map(str, numbers))}"
            results.append({"indicator": indicator, "populations": (numbers[0] if len(numbers) >= 1 else 0),})

        return results

    def process_age_group(self, age_range_key, term_list):
        """Process to detect and extract age-related terms in the document."""
        age_matches = []
        text_terms = []
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
        for term in term_list:
            if re.search(fr'\b{term}\b', self.document, re.IGNORECASE):
                text_terms.append(term)

        # Append all unique text terms as a single list at the end of `age_matches`
        if text_terms:
            age_matches.append(list(set(text_terms)))
        
        return age_matches
    
    def process_study_count(self, term_list):
        """
        Corrected function to ensure accurate summation of study and RCT counts.
        """
        raw_study_counts = []
        raw_rct_counts = []

        # Replace multi-word terms with placeholders
        replacements = {
            "randomized controlled trial": "RCT",
            "randomised controlled trial": "RCT",
            "randomized trial": "RCT",
            "randomised trial": "RCT"
        }
        for phrase, replacement in replacements.items():
            document = re.sub(phrase, replacement, self.document, flags=re.IGNORECASE)

        # Define patterns for written numbers and multipliers
        written_numbers = {
            "a": 1, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
            "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
            "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
            "eighteen": 18, "nineteen": 19, "twenty": 20
        }
        multipliers = {
            "thousand": 1000,
            "million": 1000000,
            "billion": 1000000000,
            "trillion": 1000000000000
        }

        # Process document in smaller chunks
        chunks = document.split('.')  # Split by sentence or paragraph

        for chunk in chunks:
            for term in term_list:
                # Match digit-based numbers
                digit_pattern = re.compile(rf'\b(\d+)\s*({term})s?\b', flags=re.IGNORECASE)
                digit_matches = digit_pattern.findall(chunk)
                for count, matched_term in digit_matches:
                    count_val = int(count)
                    if count_val > 0:  # Filter out invalid matches like `0`
                        if matched_term.lower() == "rct":
                            raw_rct_counts.append(f"{count_val} {matched_term}")
                        else:
                            raw_study_counts.append(f"{count_val} {matched_term}")

                # Match written numbers with multipliers
                written_pattern = re.compile(
                    rf'\b((?:a|\w+))\s*(thousand|million|billion|trillion)?\s*({term})s?\b',
                    flags=re.IGNORECASE
                )
                written_matches = written_pattern.findall(chunk)
                for word, multiplier, matched_term in written_matches:
                    try:
                        count_val = written_numbers.get(word.lower(), 0)
                        if multiplier:
                            count_val *= multipliers.get(multiplier.lower(), 1)
                        if count_val > 0:  # Filter out invalid matches like `0`
                            if matched_term.lower() == "rct":
                                raw_rct_counts.append(f"{count_val} {matched_term}")
                            else:
                                raw_study_counts.append(f"{count_val} {matched_term}")
                    except ValueError:
                        continue  # Skip invalid words

        # Deduplicate before summing totals
        unique_study_counts = list(set(raw_study_counts))
        unique_rct_counts = list(set(raw_rct_counts))

        # Recalculate totals from unique counts
        total_study_count = sum(int(item.split()[0]) for item in unique_study_counts)
        total_rct_count = sum(int(item.split()[0]) for item in unique_rct_counts)

        return unique_study_counts, total_study_count, total_rct_count, unique_rct_counts

    def process_generic_terms(self, term_list):
        """Extract general terms based on the provided list."""
        generic_matches = []
        for term in term_list:
            if re.search(fr'\b{term}\b', self.document, re.IGNORECASE):
                generic_matches.append(term)
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

        # Ensure keywords are valid and escape special characters
        if not keywords_list:
            print("No keywords provided.")
            return results

        enriched_keywords = [re.escape(keyword) for keyword in keywords_list]

        # Construct regex pattern
        pattern = re.compile(
            rf"({'|'.join(enriched_keywords)})\s+was\s+(\d+\.\d+)%\s+\((\d+\.\d+)-(\d+\.\d+)\)",
            flags=re.IGNORECASE
        )

        # Validate document content
        if not self.document:
            print("Document is empty.")
            return results

        # Search for matches in the document
        matches = pattern.findall(self.document)

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
            rf"(\d+)%\s*({'|'.join(gender_terms_list)})",  # Match percentages and gender terms
            flags=re.IGNORECASE
        )

        sex_distributions = []
        document = self.document
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

    # Utility Functions
    def find_potential_age_ranges(self):
        placeholder = r'\d{1,3}'
        pattern = rf'\b(?:ages {placeholder} to {placeholder}|ages {placeholder}-{placeholder}|{placeholder} to {placeholder} years|{placeholder}to{placeholder} yrs|{placeholder}-{placeholder} yrs|{placeholder}-{placeholder} years|{placeholder} - {placeholder} years|{placeholder} - {placeholder} yrs|less than {placeholder} year|less than {placeholder} years|less than {placeholder} yrs|{placeholder} years|{placeholder} yrs|{placeholder} age)\b'
        return re.finditer(pattern, self.document, flags=re.IGNORECASE)

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

    def convert_dict_to_dataframe(self, data_dict):
        """Convert final results into a DataFrame for analysis or storage."""
        return pd.DataFrame([data_dict])
