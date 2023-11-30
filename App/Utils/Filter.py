from App.Utils.Helpers import getUniqueCommaSeperatedColumnValues
import pandas as pd


def getDataFilters(dataset):

    review_tags = getUniqueCommaSeperatedColumnValues(dataset, "review_tags") 
    # number_of_studies_tags = getUniqueCommaSeperatedColumnValues(dataset, "number_of_studies_tags")
    population_OtherSpecificGroup = getUniqueCommaSeperatedColumnValues(dataset, "population_OtherSpecificGroup_tags")
    population_specificGroup_tags = getUniqueCommaSeperatedColumnValues(dataset, "population_specificGroup_tags")
    # population_specificGroup_acronyms_ = getUniqueCommaSeperatedColumnValues(dataset, "population_specificGroup_acronyms_")
    population_ageGroup_tags = getUniqueCommaSeperatedColumnValues(dataset, "population_ageGroup_tags")
    population_immuneStatus_tags = getUniqueCommaSeperatedColumnValues(dataset, "population_immuneStatus_tags")
    intervention_vaccinePredictableDisease_tags = getUniqueCommaSeperatedColumnValues(dataset, "intervention_vaccinePredictableDisease_tags")
    intervention_vaccineOptions_tags = getUniqueCommaSeperatedColumnValues(dataset, "intervention_vaccineOptions_tags")
    topic_acceptance_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_acceptance_tags")
    topic_coverage_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_coverage_tags")
    topic_economic_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_economic_tags")
    topic_ethicalIssues_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_ethicalIssues_tags")
    topic_administration_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_administration_tags")
    topic_efficacyEffectiveness_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_efficacyEffectiveness_tags") 
    topic_immunogenicity_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_immunogenicity_tags")
    topic_safety_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_safety_tags")
    outcome_infection_tags = getUniqueCommaSeperatedColumnValues(dataset, "outcome_infection_tags")
    outcome_hospitalization_tags = getUniqueCommaSeperatedColumnValues(dataset, "outcome_hospitalization_tags")
    outcome_death_tags = getUniqueCommaSeperatedColumnValues(dataset, "outcome_death_tags")
    outcome_ICU_tags = getUniqueCommaSeperatedColumnValues(dataset, "outcome_ICU_tags")
    openAccess_tags = getUniqueCommaSeperatedColumnValues(dataset, "openAccess_tags")
    publication_year = getUniqueCommaSeperatedColumnValues(dataset, "publication_year")
    country = getUniqueCommaSeperatedColumnValues(dataset, "country")
    region = getUniqueCommaSeperatedColumnValues(dataset, "region")

    table = {
        "review_tags" : review_tags,
        "population_OtherSpecificGroup_tags" : population_OtherSpecificGroup,
        "population_specificGroup_tags" : population_specificGroup_tags,
        # "population_specificGroup_acronyms_": population_specificGroup_acronyms_,
        "population_ageGroup_tags" : population_ageGroup_tags,
        "population_immuneStatus_tags" : population_immuneStatus_tags,
        "intervention_vaccinePredictableDisease_tags": intervention_vaccinePredictableDisease_tags,
        "intervention_vaccineOptions_tags": intervention_vaccineOptions_tags,
        "topic_acceptance_tags": topic_acceptance_tags,
        "topic_coverage_tags": topic_coverage_tags,
        "topic_economic_tags": topic_economic_tags,
        "topic_ethicalIssues_tags": topic_ethicalIssues_tags,
        "topic_administration_tags": topic_administration_tags,
        "topic_efficacyEffectiveness_tags": topic_efficacyEffectiveness_tags,
        "topic_immunogenicity_tags": topic_immunogenicity_tags,
        "topic_safety_tags": topic_safety_tags,
        "outcome_infection_tags": outcome_infection_tags,
        "outcome_hospitalization_tags": outcome_hospitalization_tags,
        "outcome_death_tags": outcome_death_tags,
        "outcome_ICU_tags": outcome_ICU_tags,
        "openAccess_tags": openAccess_tags,
        "publication_year": publication_year,
        "country": country,
        "region": region
    }


    response = [
        # { "key": "review_tags", "value" : review_tags, "order": 8},
        { "key": "population_specificGroup_tags", "value" : population_specificGroup_tags, "order": 1},
        # { "key": "population_ageGroup_tags", "value" : population_ageGroup_tags, "order": 1},
        { "key": "population_OtherSpecificGroup_tags", "value" : population_OtherSpecificGroup, "order": 2},
        { "key": "population_immuneStatus_tags", "value" : population_immuneStatus_tags, "order": 3},
        filter_keys_by_prefix(table, "intervention", 4),
        filter_keys_by_prefix(table, "topic", 5),
        filter_keys_by_prefix(table, "outcome", 6),
        { "key": "openAccess_tags", "value" : openAccess_tags, "order": 8},
        { "key": "publication_year", "value" : publication_year, "order": 9},
        { "key": "country", "value" : country, "order": 10},
        { "key": "region", "value" : region, "order": 11},
        # dummy
        # { "key": "Dummy Template", "value" : [
        #     "Test 1", "Test 2", "Test 3", "Test 4"
        # ], "order": 11},
    ]

    return response

def lookUpTable(dataset):
    review_tags = getUniqueCommaSeperatedColumnValues(dataset, "review_tags") 
    population_OtherSpecificGroup = getUniqueCommaSeperatedColumnValues(dataset, "population_OtherSpecificGroup_tags")
    population_specificGroup_tags = getUniqueCommaSeperatedColumnValues(dataset, "population_specificGroup_tags")
    # population_specificGroup_acronyms_ = getUniqueCommaSeperatedColumnValues(dataset, "population_specificGroup_acronyms_")
    population_ageGroup_tags = getUniqueCommaSeperatedColumnValues(dataset, "population_ageGroup_tags")
    population_immuneStatus_tags = getUniqueCommaSeperatedColumnValues(dataset, "population_immuneStatus_tags")
    intervention_vaccinePredictableDisease_tags = getUniqueCommaSeperatedColumnValues(dataset, "intervention_vaccinePredictableDisease_tags")
    intervention_vaccineOptions_tags = getUniqueCommaSeperatedColumnValues(dataset, "intervention_vaccineOptions_tags")
    topic_acceptance_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_acceptance_tags")
    topic_coverage_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_coverage_tags")
    topic_economic_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_economic_tags")
    topic_ethicalIssues_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_ethicalIssues_tags")
    topic_administration_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_administration_tags")
    topic_efficacyEffectiveness_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_efficacyEffectiveness_tags") 
    topic_immunogenicity_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_immunogenicity_tags")
    topic_safety_tags = getUniqueCommaSeperatedColumnValues(dataset, "topic_safety_tags")
    outcome_infection_tags = getUniqueCommaSeperatedColumnValues(dataset, "outcome_infection_tags")
    outcome_hospitalization_tags = getUniqueCommaSeperatedColumnValues(dataset, "outcome_hospitalization_tags")
    outcome_death_tags = getUniqueCommaSeperatedColumnValues(dataset, "outcome_death_tags")
    outcome_ICU_tags = getUniqueCommaSeperatedColumnValues(dataset, "outcome_ICU_tags")
    openAccess_tags = getUniqueCommaSeperatedColumnValues(dataset, "openAccess_tags")

    table = {
        "review_tags" : review_tags,
        "population_OtherSpecificGroup_tags" : population_OtherSpecificGroup,
        "population_specificGroup_tags" : population_specificGroup_tags,
        # "population_specificGroup_acronyms_": population_specificGroup_acronyms_,
        "population_ageGroup_tags" : population_ageGroup_tags,
        "population_immuneStatus_tags" : population_immuneStatus_tags,
        "intervention_vaccinePredictableDisease_tags": intervention_vaccinePredictableDisease_tags,
        "intervention_vaccineOptions_tags": intervention_vaccineOptions_tags,
        "topic_acceptance_tags": topic_acceptance_tags,
        "topic_coverage_tags": topic_coverage_tags,
        "topic_economic_tags": topic_economic_tags,
        "topic_ethicalIssues_tags": topic_ethicalIssues_tags,
        "topic_administration_tags": topic_administration_tags,
        "topic_efficacyEffectiveness_tags": topic_efficacyEffectiveness_tags,
        "topic_immunogenicity_tags": topic_immunogenicity_tags,
        "topic_safety_tags": topic_safety_tags,
        "outcome_infection_tags": outcome_infection_tags,
        "outcome_hospitalization_tags": outcome_hospitalization_tags,
        "outcome_death_tags": outcome_death_tags,
        "outcome_ICU_tags": outcome_ICU_tags,
        "openAccess_tags": openAccess_tags
    }

    return table

"""
    This is needed so that we can map back to the original structure backend.
"""
def translateKeys(dataset, search_terms):
    keys = translatePlaceholderToRealColumnName(dataset, search_terms)
    return lookUpTable(dataset).get(search_terms, [])


def translatePlaceholderToRealColumnName(dataset, search_terms:list=[]):
    table = lookUpTable(dataset)
    matching_content = [{key: table[key]} for key in table.keys() if any(term.lower() in key.lower() for term in search_terms)]
    return matching_content
"""
    Merge all the options into one for better filter from the frontend
"""
def filter_keys_by_prefix(data, prefix, order=0):
    filtered_keys = [key[len(prefix+'_'):] for key in data.keys() if key.startswith(prefix)]
    # Remove "_tags" suffix from each key
    filtered_keys = [split_string_by_capital(key[:-5]) if key.endswith("_tags") else key for key in filtered_keys]
    return {"key": f"{prefix}", "value": filtered_keys, "order": order}


def split_string_by_capital(input_string):
    result = [input_string[0]]
    for char in input_string[1:]:
        if char.isupper():
            result.append(' ')
        result.append(char)
    return ''.join(result)


# def search_dataframe(dataframe, query):
#     filtered_data = dataframe.copy()

#     for key, values in query.items():
#         if key in dataframe.columns:
#             if values:
#                 if isinstance(values, list):
#                     # Check if values are present in a comma-separated string
#                     # Convert 'Column1' to string
#                     dataframe[key] = dataframe[key].astype(str)
#                     condition = dataframe[key].apply(lambda x: isinstance(x, str) and any(search_item in x.split(', ') for search_item in values))
#                 else:
#                     # Check if single value is present in a string
#                     condition = dataframe[key].apply(lambda x: values in x.split(', '))

#                 # Apply the condition to filter the DataFrame
#                 filtered_data = filtered_data[condition]

#     return filtered_data


def search_dataframe(dataframe, query):
    filtered_data = dataframe.copy()

    # Exclude 'intervention', 'topic', 'outcome' from the query items
    excluded_keys = {'intervention', 'topic', 'outcome'}
    valid_keys = set(query.keys()) - excluded_keys

    for key in valid_keys:
        if key not in dataframe.columns:
            raise KeyError(f"Key '{key}' not found in DataFrame columns.")

        dataframe[key] = dataframe[key].astype(str).str.lower()

        values = query[key]
        if values:
            if isinstance(values, list):
                condition = dataframe[key].apply(lambda x: any(search_item.lower() in x.split(', ') for search_item in values))
            else:
                condition = dataframe[key].apply(lambda x: values.lower() in x.split(', '))

            filtered_data = filtered_data[condition]

    return filtered_data
