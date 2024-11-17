from src.Services.DatabaseHandler import DatabaseHandler
from src.Commands.PaperProcessor import PaperProcessor
from src.Commands.DatabaseUpdater import DatabaseUpdater

class PaperProcessorPipeline:
    def __init__(self, table_name, column_mapping):
        self.table_name = table_name
        self.column_mapping = column_mapping

    def process_source(self, query, csv_file_path, db_name):
        # Initialize DatabaseHandler with the query
        db_handler = DatabaseHandler(query)
        
        # Initialize PaperProcessor with the db_handler and csv_file_path
        processor = PaperProcessor(db_handler=db_handler, csv_file_path=csv_file_path)
        
        # Process papers and get the data
        data_return = processor.process_papers(db_name=db_name)
        
        # Initialize DatabaseUpdater
        updater = DatabaseUpdater(table_name=self.table_name, column_mapping=self.column_mapping)
        
        # Update database with processed data
        updater.update_columns_for_existing_records(data_return, id_column='Id')
        
        # Close the connection
        updater.close_connection()

# # Usage example
# if __name__ == "__main__":
#     pipeline = PaperProcessorPipeline(
#         table_name='all_db',
#         column_mapping={'Id': 'primary_id'}
#     )
    
#     sources = [
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE Source='Cochrane'",
#             "csv_file_path": "output/papers_data",
#             "db_name": "Cochrane"
#         },
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE Source='LOVE' LIMIT 10",
#             "csv_file_path": "output/papers_data_love",
#             "db_name": "LOVE"
#         },
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE Source='OVID' LIMIT 5",
#             "csv_file_path": "output/papers_data_OVID",
#             "db_name": "OVID"
#         },
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE Source='Medline' and primary_id=1593",
#             "csv_file_path": "output/papers_data_medline",
#             "db_name": "Medline"
#         }
#     ]
    
#     for source in sources:
#         pipeline.process_source(
#             query=source["query"],
#             csv_file_path=source["csv_file_path"],
#             db_name=source["db_name"]
#         )
