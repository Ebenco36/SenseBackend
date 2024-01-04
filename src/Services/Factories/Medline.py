import subprocess
import json
import os
import pandas as pd
import pandas as pd
from glob import glob
from pandas import json_normalize
from Bio import Medline as MedlinePackage, Entrez
from Bio.Entrez import efetch, read
from src.Services.Service import Service
from src.Request.ApiRequest import ApiRequest
Entrez.email = "ebenco94@gmail.com"

class Medline(Service):
    
    def authenticate(self):
        return self
    
    def fetch(self):
        """
            ((((((systematic* near/2 (review* OR overview)) ti, ab)) OR ((((((review) OR (literature near/3 review*) ti, ab) OR meta analysis) OR systematic review)) AND ((((((review) OR (literature near/3 review*) ti, ab) OR meta analysis) OR systematic review)) OR (((medline ti, ab OR medlars ti, ab OR embase ti, ab OR pubmed ti, ab OR cinahl ti, ab OR aged ti, ab OR psychlit ti, ab OR psyclit ti, ab OR psychinfo ti, ab OR psycinfo ti, ab OR scisearch ti, ab OR cochrane ti, ab)) OR retraction notice)))) OR ((meta*anal* ti, ab OR meta ti, ab) AND anal* ti, ab OR meta anal*' ti, ab OR metaanal* ti, ab OR metaanal* ti, ab))) AND ((((immunization AND humans)) OR (vaccine AND humans)) OR ((immunisation ti, ab OR immunization ti, ab OR immunizes ti, ab OR immunized ti, ab OR immunizing ti, ab OR immunizing ti, ab OR immunized ti, ab OR immunized ti, ab OR immunizes ti, ab OR immunizes ti, ab OR (vaccine ti, ab AND immunity ti, ab)) AND humans)). 
            
            
            ((((((systematic* NEAR/2 (review* OR overview)):ti,ab)) OR ((((((review) OR (literature near/3 review*) ti, ab) OR meta analysis) OR systematic review)) AND ((((((review) OR (literature near/3 review*) ti, ab) OR meta analysis) OR systematic review)) OR (((medline ti, ab OR medlars ti, ab OR embase ti, ab OR pubmed ti, ab OR cinahl ti, ab OR aged ti, ab OR psychlit ti, ab OR psyclit ti, ab OR psychinfo ti, ab OR psycinfo ti, ab OR scisearch ti, ab OR cochrane ti, ab)) OR retraction notice)))) OR ((meta*anal*:ti,ab OR meta:ti,ab) AND anal*:ti,ab OR 'meta anal*':ti,ab OR metaanal*:ti,ab OR metaanal*:ti,ab))) AND (((('immunization' AND humans)) OR ('vaccine' AND humans)) OR ((immunisation:ti,ab OR immunization:ti,ab OR immunise:ti,ab OR immunize:ti,ab OR immunising:ti,ab OR immunizing:ti,ab OR immunised:ti,ab OR immunized:ti,ab OR immunises:ti,ab OR immunizes:ti,ab OR (vaccine:ti,ab AND immunity:ti,ab)) AND humans))
        """
        term = """((((((systematic* NEAR/2 (review* OR overview)):ti,ab)) OR ((((((review) OR (literature near/3 review*) ti, ab) OR meta analysis) OR systematic review)) AND ((((((review) OR (literature near/3 review*) ti, ab) OR meta analysis) OR systematic review)) OR (((medline ti, ab OR medlars ti, ab OR embase ti, ab OR pubmed ti, ab OR cinahl ti, ab OR aged ti, ab OR psychlit ti, ab OR psyclit ti, ab OR psychinfo ti, ab OR psycinfo ti, ab OR scisearch ti, ab OR cochrane ti, ab)) OR retraction notice)))) OR ((meta*anal*:ti,ab OR meta:ti,ab) AND anal*:ti,ab OR meta anal*:ti,ab OR metaanal*:ti,ab OR metaanal*:ti,ab))) AND (((('immunization' AND humans)) OR ('vaccine' AND humans)) OR ((immunisation:ti,ab OR immunization:ti,ab OR immunise:ti,ab OR immunize:ti,ab OR immunising:ti,ab OR immunizing:ti,ab OR immunised:ti,ab OR immunized:ti,ab OR immunises:ti,ab OR immunizes:ti,ab OR (vaccine:ti,ab AND immunity:ti,ab)) AND humans))"""
        csv_filename = "Medline"
        email = "ebenco94@gmail.com"

        # Use esearch to get the list of PubMed IDs matching the query
        esearch_command = f"esearch -db pubmed -query '{term}' -email {email} | efetch -format uid"
        process = subprocess.Popen(esearch_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            print(f"Error during esearch: {stderr.decode('utf-8')}")
            return
        else:
            print("We are done with this phase...")

        # Extract PubMed IDs from the output
        pubmed_ids = stdout.decode('utf-8').strip().split('\n')

        # Use efetch to retrieve the records in batches
        batch_size = 1000
        all_records = []

        for i in range(0, len(pubmed_ids), batch_size):
            batch_ids = ",".join(pubmed_ids[i:i+batch_size])
            self.fetch_pubmed_results(batch_ids, email, i)
            
        print("We are done downloading data for each batch")
        self.merge_files_by_pattern("Medline/Batches", "batch_*", "Medline/Medline.csv")


    def fetch_pubmed_results(self, batch_ids, email, i):
        # Build the efetch command
        efetch_command = f"efetch -db pubmed -id {batch_ids} -format medline -email {email}"

        try:
            # Run the command and capture the output
            result = subprocess.check_output(efetch_command, shell=True, text=True)

            # Parse the result as Medline records
            medline_records = [record.strip() for record in result.split('\n\n') if record.strip()]

            # Convert Medline records to a list of dictionaries
            medline_dicts = []
            for record in medline_records:
                medline_dict = dict(line.split(" - ", 1) for line in record.split('\n') if " - " in line)
                medline_dicts.append(medline_dict)

            # Normalize JSON and save to CSV
            df = json_normalize(medline_dicts)
            df.to_csv('Medline/Batches/batch_' + str(i) + '.csv', index=False)

        except subprocess.CalledProcessError as e:
            # Handle any errors that may occur during command execution
            print(f"Error executing efetch command: {e}")
            return None
        
        
    
    def merge_files_by_pattern(self, directory_path, pattern, output_file_path):
        # Check if the directory exists
        if not os.path.exists(directory_path):
            raise FileNotFoundError(f"Directory not found: {directory_path}")

        # Construct the full pattern for globbing
        full_pattern = os.path.join(directory_path, pattern)

        # Use glob to find files matching the pattern
        matching_files = glob(full_pattern)

        # Check if there are any matching files
        if not matching_files:
            print(f"No matching files found in '{directory_path}' with pattern '{pattern}'.")
            return

        # Initialize an empty DataFrame to store the merged data
        merged_df = pd.DataFrame()

        # Loop through each matching file and merge it into the DataFrame
        for file_path in matching_files:
            df = pd.read_csv(file_path)
            merged_df = pd.concat([merged_df, df], ignore_index=True)

        # Save the merged DataFrame to the specified output file
        merged_df.to_csv(output_file_path, index=False)

        print(f"Matching files in '{directory_path}' have been merged into '{output_file_path}'.")
