from App.Utils.Filter import getDataFilters, search_dataframe, translatePlaceholderToRealColumnName
from flask import Flask, Blueprint, request, current_app, jsonify
from App.api_utils import embase_access, ilove_access, furtherProcessEmbase
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from flask_cors import CORS
from rq import Queue
from redis import Redis
import pandas as pd
import os

app = Flask(__name__)

# Configure SQLAlchemy job store using SQLite
jobstore = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
}

scheduler = BackgroundScheduler(jobstores=jobstore)
scheduler.start()

app.config.from_mapping(
    RQ_REDIS_URL='redis://localhost:6379/0'  # Replace with your Redis URL
)
CORS(app)
redis_conn = Redis.from_url(app.config['RQ_REDIS_URL'])
queue = Queue(connection=redis_conn)

# Get the project directory path
project_dir = os.path.abspath(os.path.dirname(__file__))

def my_queued_job(arg1, arg2):
    return f"Queued job executed with args: {arg1}, {arg2}"

# job = queue.enqueue(my_queued_job, 'arg1_value', 'arg2_value')

# Schedule the job to run every 5 seconds
# scheduler.add_job(my_job, 'interval', seconds=5)

bp = Blueprint('routes', __name__)

@app.route('/spool-from-embase', methods=['GET', 'POST'])
def embase_endpoint():
    embase_access()
    return jsonify({})

@app.route('/spool-from-embase-process', methods=['GET', 'POST'])
def embase_endpoint_processing():
    # add a job for this.
    furtherProcessEmbase()
    return jsonify({})

@app.route('/spool-from-ilove', methods=['GET', 'POST'])
def iLove_endpoint():
    ilove_access()
    return jsonify({})

@app.route('/page-content', methods=['GET', 'POST'])
def get():
    dataset = pd.read_csv(project_dir+"/results/ProcessedData.csv")
    # header = tableHeader(dataset.columns.tolist())
    # Assuming df is your DataFrame
    dataset.rename(columns={'Unnamed: 0': 'id', 'openAccess_tags': 'Open Access'}, inplace=True)
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
    records_per_page = int(request.args.get('records_per_page', 100))
    post_data = request.get_json()

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

@app.route('/page-filter', methods=['GET'])
def getFieldValues():
    dataset = pd.read_csv(project_dir+"/results/ProcessedData.csv")
    
    result = getDataFilters(dataset)
    
    # Return the result as JSON using Flask's jsonify function
    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True)
