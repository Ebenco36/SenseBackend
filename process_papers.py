import os
import sys
from src.Commands.PaperProcessor import PaperProcessor

def main():
    db_name = sys.argv[1]
    query = sys.argv[2]
    csv_file_path = sys.argv[3]

    # Initialize and run the paper processor
    processor = PaperProcessor(query=query, csv_file_path=csv_file_path)
    processor.process_papers(DB_name=db_name)
    
    # Verify that the output file exists after processing
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"Output file was not created: {csv_file_path}")
    else:
        print(f"Successfully created: {csv_file_path}")

if __name__ == "__main__":
    main()
