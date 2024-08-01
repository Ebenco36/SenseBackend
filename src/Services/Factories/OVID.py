import os
import re
import io
import csv
import json
import xml.etree.ElementTree as ET
import pandas as pd
from glob import glob
from src.Commands.services import fetch_first_scholar_result
from src.Services.Service import Service
from src.Request.ApiRequest import ApiRequest
from src.Utils.Helpers import (
    create_directory_if_not_exists,
    check_file_existence, 
    create_directory_if_not_exists,
    getDOI,
    process_data_valid, process_new_sheet
)

class OVID(Service):

    def __init__(self, pageSize = 200):
        self.pageSize = pageSize


    def authenticate(self, headers):
        self.auth_headers = headers
        return self

    def fetch(self, headers):
        return self.authenticate(headers).retrieveRecord()

    """
        Process flow of retrieving records from L.ove
        Stages are listed here.
    """
    
                   
    def retrieveRecord(self):
        return self.process_all_xml_files("./OVID/raw", "./OVID/data.csv")
        
    
    def list_of_dicts_to_csv(self, data, file_path=None):
        if not data:
            return ""
        
        # Extract all unique keys from the list of dictionaries to define the fieldnames
        headers = set()
        for d in data:
            headers.update(d.keys())
        headers = list(headers)
        
        # Create a string buffer
        output = io.StringIO()
        
        # Create a CSV writer object
        writer = csv.DictWriter(output, fieldnames=headers)
        
        # Write headers
        writer.writeheader()
        
        # Write rows
        for row in data:
            # Normalize the row to ensure it contains all the headers
            normalized_row = {header: row.get(header, "") for header in headers}
            writer.writerow(normalized_row)
        
        # Get the CSV string from the string buffer
        csv_string = output.getvalue()
        
        # Save to file if file_path is provided
        if file_path:
            with open(file_path, 'w', newline='') as csvfile:
                csvfile.write(csv_string)
        
        # Close the string buffer
        output.close()
            
            
    def parse_xml_to_json(self, xml_file_path):
        # Parse the XML file
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Initialize an empty list to store records
        records = []

        # Iterate over each <record> element
        for record in root.findall('.//record'):
            record_dict = {}
            for field in record.findall('F'):
                field_code = field.attrib.get('C', '')
                field_label = field.attrib.get('L', '')
                field_value_elem = field.find('D')
                field_value = field_value_elem.text.strip() if field_value_elem is not None and field_value_elem.text is not None else ""

                # Special handling for authors
                if field_code == 'au':
                    authors_list = field.findall('D')
                    authors = "; ".join(author.text.strip() for author in authors_list if author.text) if authors_list else ""
                    record_dict[field_label] = authors
                else:
                    record_dict[field_label] = field_value

            records.append(record_dict)

        return records

    def list_of_dicts_to_csv(self, records, csv_file_path):
        if records:
            df = pd.DataFrame(records)
            
            # Path for the existing CSV file
            existing_csv_path = './OVID/data.csv'

            if os.path.exists(existing_csv_path):
                # Load the existing data
                existing_df = pd.read_csv(existing_csv_path)

                # Create a mapping from Title to DOI from the existing data
                title_to_doi = dict(zip(existing_df['Title'], existing_df['DOI']))

                condition = (
                    (df['Accession Number'].isna() | (df['Accession Number'] == '')) &
                    ((df['DOI'].isna() & (df['DOI'] != '')) &
                    (df['Digital Object Identifier'].isna() & (df['Digital Object Identifier'] != '')))
                )
                print(condition.sum())
                # Update DOI values from the existing data if the condition is met
                df.loc[condition, 'DOI'] = df.loc[condition, 'Title'].map(title_to_doi)

            condition = (
                (df['Accession Number'].isna() | (df['Accession Number'] == '')) &
                ((df['DOI'].isna() & (df['DOI'] != '')) &
                (df['Digital Object Identifier'].isna() & (df['Digital Object Identifier'] != '')))
            )
            print(condition.sum())
            df.loc[condition, 'DOI'] = df.loc[condition, 'Title'].apply(fetch_first_scholar_result)
            
            # Save the updated DataFrame to the CSV file
            df.to_csv(csv_file_path, index=False)
            
        
    # def list_of_dicts_to_csv(self, records, csv_file_path):
    #     if records:
    #         df = pd.DataFrame(records)
            
    #         # We manually spool data. Implementation below is to fetch doi link for paper with doi from google scholar
    #         # Apply the function to each row in the DataFrame
    #         condition = (
    #             (df['Accession Number'].isna() | (df['Accession Number'] == '')) &
    #             ((df['DOI'].isna() & (df['DOI'] != '')) &
    #             (df['Digital Object Identifier'].isna() & (df['Digital Object Identifier'] != '')))
    #         )
    #         df.loc[condition, 'DOI'] = df.loc[condition, 'Title'].apply(fetch_first_scholar_result)
    #         df.to_csv(csv_file_path, index=False)

    def process_all_xml_files(self, folder_path, output_csv_path):
        all_records = []
        
        # Loop through all XML files in the specified directory
        for filename in os.listdir(folder_path):
            if filename.endswith('.xml'):
                file_path = os.path.join(folder_path, filename)
                print(f"Processing file: {file_path}")
                records = self.parse_xml_to_json(file_path)
                all_records.extend(records)

        # Save all records to CSV
        self.list_of_dicts_to_csv(all_records, output_csv_path)
        print(f"All data saved to: {output_csv_path}")
        