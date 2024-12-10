import sys
import os
sys.path.append(os.getcwd())
from concurrent.futures import ThreadPoolExecutor
from src.Services.DatabaseHandler import DatabaseHandler
from src.Commands.PaperProcessor import PaperProcessor
from src.Commands.DatabaseUpdater import DatabaseUpdater

class PaperProcessorPipeline:
    def __init__(self, table_name, column_mapping, tracker_table='processing_tracker'):
        self.table_name = table_name
        self.column_mapping = column_mapping
        self.tracker_table = tracker_table

    def ensure_tracker_table_exists(self, db_handler):
        """
        Ensures the processing_tracker table exists. Creates it if not present.
        """
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.tracker_table} (
            source_name VARCHAR(255) PRIMARY KEY,
            last_processed_id BIGINT DEFAULT 0,
            status VARCHAR(50) DEFAULT 'pending',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        db_handler.execute_query(create_table_query)

    def get_last_processed_id(self, db_handler, source_name):
        """
        Retrieves the last processed ID for the given source from the tracker table.
        """
        query = f"SELECT last_processed_id FROM {self.tracker_table} WHERE source_name = %s"
        result = db_handler.execute_query(query, (source_name,))
        return result[0][0] if result else 0

    def update_tracker(self, db_handler, source_name, last_processed_id, status='in_progress'):
        """
        Updates the tracker table with the latest processed ID and status.
        """
        query = f"""
        INSERT INTO {self.tracker_table} (source_name, last_processed_id, status, updated_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (source_name)
        DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id,
                      status = EXCLUDED.status,
                      updated_at = CURRENT_TIMESTAMP
        """
        db_handler.execute_query(query, (source_name, last_processed_id, status))

    def process_source_in_batches(self, query, csv_file_path, db_name, batch_size=5):
        """
        Processes records in batches for a given source query, saving results and updating the database.
        """
        db_handler = DatabaseHandler(query)
        updater = DatabaseUpdater(table_name=self.table_name, column_mapping=self.column_mapping)

        try:
            # Ensure the tracker table exists
            self.ensure_tracker_table_exists(db_handler)
            
            # Get the last processed ID for this source
            last_id = self.get_last_processed_id(db_handler, db_name)

            while True:
                # Dynamically create a query for the current batch
                batch_query = f"{query} AND primary_id > {last_id} ORDER BY primary_id ASC LIMIT {batch_size}"
                db_handler.query = batch_query  # Update query directly in db_handler

                # Fetch and process the batch
                processor = PaperProcessor(db_handler=db_handler, csv_file_path=csv_file_path)
                data_return = processor.process_papers(db_name=db_name)  # Assume returns a DataFrame

                if data_return.empty:
                    # No more records to process
                    self.update_tracker(db_handler, db_name, last_id, status='completed')
                    break

                # Save last processed ID for the next iteration
                last_id = data_return["Id"].max()

                # Update the database with processed data
                updater.update_columns_for_existing_records(data_return, id_column='Id')

                # Update the tracker table
                self.update_tracker(db_handler, db_name, last_id)

        except Exception as e:
            print(f"Error processing source {db_name}: {e}")
        finally:
            db_handler.close_connection()
            updater.close_connection()

# def main():
#     pipeline = PaperProcessorPipeline(
#         table_name='all_db',
#         column_mapping={'Id': 'primary_id'}
#     )

#     sources = [
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE \"Source\"='Cochrane'",
#             "csv_file_path": "output/papers_data",
#             "db_name": "Cochrane"
#         },
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE \"Source\"='LOVE'",
#             "csv_file_path": "output/papers_data_love",
#             "db_name": "LOVE"
#         },
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE \"Source\"='OVID'",
#             "csv_file_path": "output/papers_data_OVID",
#             "db_name": "OVID"
#         },
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE \"Source\"='Medline'",
#             "csv_file_path": "output/papers_data_medline",
#             "db_name": "Medline"
#         }
#     ]

#     # Use parallel processing to process multiple sources simultaneously
#     with ThreadPoolExecutor(max_workers=4) as executor:
#         executor.map(
#             lambda source: pipeline.process_source_in_batches(
#                 query=source["query"],
#                 csv_file_path=source["csv_file_path"],
#                 db_name=source["db_name"],
#                 batch_size=100
#             ),
#             sources
#         )

# if __name__ == "__main__":
#     main()
