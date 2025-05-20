import os
import pandas as pd
from celery import shared_task
from src.Commands.regexp import searchRegEx
from src.Commands.TaggingSystem import Tagging
from concurrent.futures import ThreadPoolExecutor
from src.Services.DatabaseHandler import DatabaseHandler
from src.Commands.PaperProcessorPipeline import PaperProcessorPipeline

@shared_task(name="process_uploaded_file_task")
def process_uploaded_file_task(temp_file_path, output_file_path):
    """
    1) Load & validate CSV
    2) Filter out existing DOIs
    3) Early exit if no new records
    4) Insert new records
    5) Build safe IN‐clause
    6) Run downstream pipeline in parallel
    7) Write out processed CSV
    """
    # 1) Load CSV and validate columns
    data = pd.read_csv(temp_file_path)
    required = [
        "Authors", "Year", "Title", "DOI", "Open Access", "Abstract",
        "Id", "Source", "Language", "Country", "Database", "Journal"
    ]
    missing = [c for c in required if c not in data.columns]
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")
    data = data[required]

    # 2) Filter new DOIs
    db_handler  = DatabaseHandler()
    existing    = set(db_handler.fetch_existing_dois())
    new_records = data[~data["DOI"].isin(existing)]

    # 3) Early exit if nothing to do
    if new_records.empty:
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        new_records.to_csv(output_file_path, index=False)
        return {
            "status":      "no_new",
            "message":     "No new records to insert.",
            "output_file": output_file_path
        }

    # 4) Insert the new rows
    db_handler.insert_records(new_records)

    # 5) Build a safe IN‐clause for downstream query
    dois = new_records["DOI"].tolist()
    in_clause = ", ".join(f"'{doi}'" for doi in dois)
    downstream_query = f"""
        SELECT primary_id, "DOI", "Source"
        FROM all_db
        WHERE "DOI" IS NOT NULL
          AND "DOI" != ''
          AND "DOI" IN ({in_clause})
    """

    # 6) Run downstream pipeline in parallel
    pipeline = PaperProcessorPipeline(
        table_name="all_db",
        column_mapping={"Id": "primary_id"}
    )
    sources = [{
        "query":         downstream_query,
        "csv_file_path": temp_file_path,
        "db_name":       "all_db"
    }]

    def _process(source):
        try:
            return pipeline.process_source_in_batches(
                query=source["query"],
                csv_file_path=source["csv_file_path"],
                db_name=source["db_name"],
                batch_size=10
            )
        except Exception as e:
            # log internally, but don’t crash the whole task
            print(f"Pipeline error for {source['db_name']}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=4) as executor:
        pipeline_results = list(executor.map(_process, sources))

    # 7) Write out the processed CSV
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    new_records.to_csv(output_file_path, index=False)

    return {
        "status":      "success",
        "output_file": output_file_path,
        "pipeline":    pipeline_results
    }

# @shared_task
def process_uploaded_data_task(data: dict) -> dict:
    topic = data.get('topic')
    context = data.get('context')
    lastSearch = data.get('lastSearch')
    intervention = data.get('intervention')
    amstarLabelFlaws = data.get('amstarLabelFlaws')
    
    if not context:
        return {'error': "No 'context' provided"}

    try:
        # Initialize Tagging with provided context
        tagger = Tagging(context)

        # Get available sections
        available_sections = tagger.sections.available_sections()

        # Tag using provided regex mapping (empty by default)
        search_map = searchRegEx
        tagging_results = {}
        if lastSearch:
            tagging_results["lastSearch"] = tagger.extract_last_literature_search_dates()
        if intervention:
            for intervention in search_map.get('intervention'):
                for items in intervention:
                    for item in items:
                        tagging_results['intervention'][intervention] = tagger.process_generic_terms(item)
        if topic:
            for topic in search_map.get('topic'):
                for items in intervention:
                    for item in items:
                        tagging_results['topic'][topic] = tagger.process_generic_terms(item)
        if amstarLabelFlaws: 
            tagging_results['amstarLabelFlaws'] = tagger.amstar2_integration()
        
        print(tagging_results)

        return {
            'available_sections': available_sections,
            'tagging_results': tagging_results
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error processing data: {e}")
        return {'error': str(e)}