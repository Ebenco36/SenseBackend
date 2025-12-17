import os
import json
from flask import request, jsonify
from flask_restful import Resource
from src.Utils.Helpers import clean_json
from src.Utils.Filter import getDataFilters, search_dataframe, translatePlaceholderToRealColumnName


project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class DataProcessorResource(Resource):
    def post(self):
        import pandas as pd
        dataset = pd.read_csv(project_dir+"/results/ProcessedData.csv")
        # header = tableHeader(dataset.columns.tolist())
        # Assuming df is your DataFrame
        dataset.rename(columns={'Unnamed: 0': 'id'}, inplace=True)
        header = dataset.columns.tolist()
        elements_to_remove = [
            "journal_source",
            "DBCOllection", 
            "publication_date_obj", 
            "journal_abstract", 
            "journal_URL",
            "other_domain",
            "authors",
            "PII",
            "country",
            "region",
            "document_type",
            "review_tags",
            "number_of_studies_tags",
            "population_specificGroup_tags",
            "population_OtherSpecificGroup_tags",
            "population_specificGroup_acronyms_",
            "population_ageGroup_tags",
            "population_immuneStatus_tags",
            "intervention_vaccinePredictableDisease_tags",
            "intervention_vaccineOptions_tags",
            "topic_acceptance_tags",
            "topic_coverage_tags",
            "topic_economic_tags",
            "topic_ethicalIssues_tags",
            "topic_administration_tags",
            "topic_efficacyEffectiveness_tags",
            "topic_immunogenicity_tags",
            "topic_safety_tags",
            "outcome_infection_tags",
            "outcome_hospitalization_tags",
            "outcome_death_tags",
            "outcome_ICU_tags",
        ]
        header = [x for x in header if x not in elements_to_remove and not x.endswith("_bag")]
        # Get the pagination parameters from the query string
        page = int(request.args.get('page', 1))
        records_per_page = int(request.args.get('records_per_page', 20))
        post_data = request.get_json()
        
        if (post_data):
            for key in post_data:
                if(key in post_data and post_data[key] and len(post_data[key]) > 0):
                    # Filter rows based on the list of values
                    exceptional_keys = ["outcome", "intervention", "topic"]
                    if(key in exceptional_keys):
                        # search to relevant keys
                        # Create a copy of the original dictionary
                        temp_dict = post_data.copy()
                        result_array = ["".join(item.split()) for key in exceptional_keys if key in post_data for item in post_data[key]]
                        filter_data = translatePlaceholderToRealColumnName(dataset, result_array)
                        for item in filter_data:
                            for key, value in item.items():
                                temp_dict[key] = value
                        # Update the original dictionary with the values from the temporary dictionary
                        filtered_data = {key: value for key, value in temp_dict.items() if key not in exceptional_keys}
                        post_data = filtered_data
                        # remove what we do not need to avoid causing error since the key doesn't exist within dataframe
                    dataset = search_dataframe(dataset, post_data)
                    # dataset = dataset[dataset[key].apply(lambda x: isinstance(x, str) and any(search_item in x.split(', ') for search_item in post_data[key]))]


        # Calculate the start and end index for slicing the DataFrame
        start_idx = (page - 1) * records_per_page
        end_idx = start_idx + records_per_page

        # Slice the DataFrame based on the pagination parameters
        paginated_df = dataset[start_idx:end_idx].fillna("").to_dict('records')
        # Generate the DataFrame summary statistics
        df_summary = dataset.describe(include='all').fillna("").to_dict()
        # Combine the paginated data and the summary statistics into a single dictionary
        rows, columns = dataset.shape
        
        result = {
            'header':header,
            'rows': rows,
            'columns': columns,
            'summary': df_summary,
            'data': paginated_df
        }

        # Return the result as JSON using Flask's jsonify function
        return jsonify(result)

class DataFilterResource(Resource):

    def get(self):
        import pandas as pd
        dataset = pd.read_csv(project_dir+"/results/ProcessedData.csv")
        
        result = getDataFilters(dataset)
        
        # Return the result as JSON using Flask's jsonify function
        return jsonify(result)

class DataProcessorFilterResource(Resource):
    def get(self, row_id):
        import pandas as pd
        dataset = pd.read_csv(project_dir+"/results/ProcessedData.csv")
        dataset.rename(columns={'Unnamed: 0': 'id'}, inplace=True)
        row = clean_json(dataset[dataset['id'] == row_id])
        if not row.empty:
            return jsonify(row.to_json(orient="records"))
        else:
            return jsonify({'error': 'Row not found'}), 404