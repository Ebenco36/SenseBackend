from flask import request
from flask_restful import Resource
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from src.Commands.PaperProcessorPipeline import PaperProcessorPipeline
from src.Utils.response import ApiResponse
from src.Services.DatabaseHandler import DatabaseHandler
import os

class PaperProcessorAPI(Resource):
    def __init__(self):
        # Initialize the pipeline and database handler
        self.pipeline = PaperProcessorPipeline(
            table_name='all_db',
            column_mapping={'Id': 'primary_id'}
        )
        self.db_handler = DatabaseHandler()

    def post(self):
        """
        Accepts a CSV file from the user, validates it, adds records to the database,
        and processes the records in parallel.
        """
        if 'file' not in request.files:
            return ApiResponse.error(message="No file provided", status_code=400)

        file = request.files['file']

        if file.filename == '':
            return ApiResponse.error(message="No file selected", status_code=400)

        # Save the uploaded file temporarily
        temp_file_path = os.path.join("temp", file.filename)
        os.makedirs("temp", exist_ok=True)
        file.save(temp_file_path)

        try:
            # Read the CSV file
            data = pd.read_csv(temp_file_path)

            # Validate the CSV structure
            required_columns = [
                "Authors", "Year", "Title", "DOI", "Open Access", "Abstract",
                "Id", "Source", "Language", "Country", "Database", "Journal"
            ]
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                return ApiResponse.error(
                    message=f"Invalid CSV format. Missing columns: {', '.join(missing_columns)}",
                    status_code=400
                )

            # Check for duplicate DOIs in the database
            existing_dois = self.db_handler.fetch_existing_dois()
            new_records = data[~data['DOI'].isin(existing_dois)]

            if new_records.empty:
                return ApiResponse.error(
                    message="All records in the uploaded file already exist in the database.",
                    status_code=400
                )

            # Insert new records into the database
            self.db_handler.insert_records(new_records)

            # Prepare sources for parallel processing
            sources = [
                {
                    "query": f"""
                        SELECT primary_id, "DOI", "Source" 
                        FROM all_db 
                        WHERE "DOI" IS NOT NULL AND "DOI" != '' 
                        AND "DOI" IN ({','.join([f"'{doi}'" for doi in new_records['DOI'].tolist()])})
                    """,
                    "csv_file_path": temp_file_path,
                    "db_name": "all_db"
                }
            ]

            # Process the records in parallel using process_source_in_batches
            with ThreadPoolExecutor(max_workers=4) as executor:
                executor.map(
                    lambda source: self.pipeline.process_source_in_batches(
                        query=source["query"],
                        csv_file_path=source["csv_file_path"],
                        db_name=source["db_name"],
                        batch_size=10
                    ),
                    sources
                )

            # Save the processed results
            output_file_path = os.path.join("Data/output", f"processed_{file.filename}")
            os.makedirs("Data/output", exist_ok=True)
            pd.DataFrame(new_records).to_csv(output_file_path, index=False)

            return ApiResponse.success(
                message="File processed successfully",
                data={"output_file": output_file_path},
                status_code=200
            )

        except Exception as e:
            # Log the error and return a proper error response
            print(f"Error occurred: {e}")
            return ApiResponse.error(message="An internal server error occurred", status_code=500)

        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)