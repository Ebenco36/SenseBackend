import re
import pandas as pd
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
                    elif category == "Studies" and subcategory == "NoOfStudies":
                        self.result_columns[column_name], total_study_count, total_rct_count, rct_counts = self.process_study_count(term_list)
                        self.result_columns["total_study_count"] = total_study_count
                        self.result_columns["total_rct_count"] = total_rct_count
                        self.result_columns["RCT_counts"] = rct_counts
                    else:
                        self.result_columns[column_name] = self.process_generic_terms(term_list)
        
        return self.clean_result(self.result_columns)

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
            text_terms = [age_range_key.replace("_", " ")]

        # Collect matched terms for the text part, ensuring no duplicates
        for term in term_list:
            if re.search(fr'\b{term}\b', self.document, re.IGNORECASE):
                text_terms.append(term)

        # Append all unique text terms as a single list at the end of `age_matches`
        if text_terms:
            age_matches.append(list(set(text_terms)))

        return age_matches

    def process_study_count(self, term_list):
        """Extract study counts and separate RCT counts."""
        study_counts = []
        rct_counts = []
        total_study_count = 0
        total_rct_count = 0

        for term in term_list:
            pattern = re.compile(rf'\b(\d+)\s*{term}\b', flags=re.IGNORECASE)
            matches = pattern.findall(self.document)

            for count in matches:
                count_val = int(count)
                if "RCT" in term:
                    rct_counts.append(f"{count_val} RCT")
                    total_rct_count += count_val
                else:
                    study_counts.append(f"{count_val} studies")
                    total_study_count += count_val

        # Remove duplicates from the lists and return them
        return list(set(study_counts)), total_study_count, total_rct_count, list(set(rct_counts))

    def process_generic_terms(self, term_list):
        """Extract general terms based on the provided list."""
        generic_matches = []
        for term in term_list:
            if re.search(fr'\b{term}\b', self.document, re.IGNORECASE):
                generic_matches.append(term)
        return list(set(generic_matches))

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
        return cleaned_result

    def convert_dict_to_dataframe(self, data_dict):
        """Convert final results into a DataFrame for analysis or storage."""
        return pd.DataFrame([data_dict])
