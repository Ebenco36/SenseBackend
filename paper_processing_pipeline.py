import sys
import os

# Add the current directory to sys.path for imports
sys.path.append(os.getcwd())

from src.Commands.TaggingSystem import TaggingSystem
# from src.Services.Factories.scrapers import GeneralPDFWebScraper
from src.Commands.PaperProcessor import PaperProcessor

class PaperProcessingPipeline:
    def __init__(self):
        self.db_queries = {
            "Ovid": {
                "query": 'SELECT upper("doi") FROM ovid_db WHERE primary_id IN (1, 2, 3, 4, 5)',
                "csv_file_path": 'Data/output/ovid_papers_tags'
            },
            "Cochrane": {
                "query": 'SELECT upper("doi"), doi_link FROM cochrane_db WHERE primary_id IN (1, 2)',
                "csv_file_path": 'Data/output/cochrane_papers_tags'
            },
            "Love": {
                "query": 'SELECT upper("doi") FROM love_db WHERE primary_id IN (1, 2)',
                "csv_file_path": 'Data/output/love_papers_tags'
            },
            "Medline": {
                "query": 'SELECT upper("doi") FROM medline_db WHERE primary_id IN (1, 2)',
                "csv_file_path": 'Data/output/medline_papers_tags'
            }
        }
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)

    def process_database(self, db_name):
        if db_name not in self.db_queries:
            print({"status": "error", "message": f"Database '{db_name}' not found in configuration."})
            return
        
        config = self.db_queries[db_name]
        paper_processor = PaperProcessor(query=config["query"], csv_file_path=config["csv_file_path"])
        paper_processor.process_papers(DB_name=db_name)
        
        print({
            "status": "success",
            "message": f"Papers processed and CSV generated for {db_name}.",
            "csv_file_path": config["csv_file_path"]
        })

    def process_all_databases(self):
        for db_name in self.db_queries.keys():
            self.process_database(db_name)

# Example usage
if __name__ == "__main__":
    pipeline = PaperProcessingPipeline()
    pipeline.process_all_databases()
