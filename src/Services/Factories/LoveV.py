import os
import re
import json
import pandas as pd
from glob import glob
from src.Services.Service import Service
from src.Request.ApiRequest import ApiRequest
from src.Utils.Helpers import (
    create_directory_if_not_exists,
    check_file_existence, 
    create_directory_if_not_exists,
    getDOI,
    process_data_valid, process_new_sheet
)

class LoveV(Service):

    def __init__(self, pageSize = 500):
        self.pageSize = pageSize


    def authenticate(self, headers):
        self.auth_headers = headers
        return self

    def fetch(self, headers):
        return self.authenticate(headers).executeRetrieveData()

    """
        Process flow of retrieving records from L.ove
        Stages are listed here.
    """
    
                   
    def retrieveRecord(self, page = 1):
        # https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references?metadata_ids=5e7fce7e3d05156b5f5e032a,603b9fe03d05151f35cf13dc&classification_filter=systematic-review,primary-study&hide_excluded=true&page=1&sort_by=year&show_summary=true
        # url = 'https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references?metadata_ids=5e7fce7e3d05156b5f5e032a,603b9fe03d05151f35cf13dc&classification_filter=systematic-review,primary-study&hide_excluded=true&page=' + str(page) + '&sort_by=year&show_summary=true'
        url = "https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references"
        # url = f"https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references?metadata_ids=5e7fce7e3d05156b5f5e032a,603b9fe03d05151f35cf13dc&classification_filter=systematic-review&hide_excluded=true&page={page}&size={self.pageSize}&sort_by=year&show_summary=true&year_from_filter=2011"
        print(url)
        """
            Work on payload
        """
        payload = {
            "sort_by": "year",
            "metadata_ids": [
                "603b9fe03d05151f35cf13dc"
            ],
            "query": "",
            "page": page,
            "size": self.pageSize,
            "classification_filter": "systematic-review",
            "year_from_filter": 2011
        }
        
        record_details_data = ApiRequest('json', url, headers=self.auth_headers)
        rec = record_details_data.send_data(payload)
        data = rec.get('data')
        
        # record_details_data = ApiRequest('json', url, headers=self.auth_headers)
        # rec = record_details_data.fetch_records() #.send_data(payload)
        # data = rec.get('data')
        return data
    
    """
        We are following the structure as described on EMBASE Server.
    """

    def executeRetrieveData(self, batch_size=10, save_interval=10, max_records=0):
        # 10 is the number of record per page
        max_records = self.pageSize * save_interval
        # Save DataFrame to a CSV file
        file_path = 'Data/L-OVE/Batches/'
        file_path_processed = 'Data/L-OVE/Batches/Processed/'

        create_directory_if_not_exists(file_path)

        if(not check_file_existence(file_path, "batch_final.csv")):
            # continue from where we stop to avoid starting all over again
            result_max_num = get_max_batch_file(file_path)
            page = 1

            if (result_max_num and result_max_num > 0):
                # input and calculate continuation flow
                """
                    if per page record = 10
                    max_record is 2000 (save_interval * record per page)
                    total record = max_records * batch_number
                    page = ?
                    Page = max_record / 10 == page number
                """
                page = int((max_records * result_max_num) / batch_size) + 1
            all_records = []
            while True:
                # Fetch records for the current page
                records = self.retrieveRecord(page)
                
                # Break the loop if no more records are returned
                if not records or len(records.get('items')) == 0:
                    break

                # Append records to the list
                all_records.extend(records.get('items'))

                print(str(page) + '=====' + str((page % save_interval)) +'====='+ str(len(all_records)) +'====='+ str(max_records))
                # Check if it's time to save the records to a CSV file
                if page % save_interval == 0 and len(all_records) == max_records:
                    # Convert the list of records to a pandas DataFrame
                    df = pd.DataFrame(all_records)
                    csv_filename = file_path + "batch_" + str(page/save_interval) + '.csv'
                    df.to_csv(csv_filename, index=False, encoding='utf-8')

                    print(f"Records spooled and saved to {csv_filename}.")
                    # empty all_records = []
                    all_records = []
                # Move to the next page
                page += 1

            # Save any remaining records
            if all_records:
                # Convert the list of records to a pandas DataFrame
                df = pd.DataFrame(all_records)
                csv_filename = file_path + "batch_final.csv"

                df.to_csv(csv_filename, index=False, encoding='utf-8')

                print(f"Records spooled and saved to {csv_filename}.")
        # come back to this later
        # process_csv_files(file_path, file_path_processed)  
        print("All records saved successfully. Now merging files into one csv")
        merge_files_by_pattern("Data/L-OVE/Batches", "batch_*", "Data/L-OVE/LOVE.csv")
        print("Done merging the csv files.")
        
        return self
    
def get_max_batch_file(directory):
    # Ensure the directory exists
    if not os.path.exists(directory):
        return None

    # Get a list of CSV files in the directory
    csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]

    # If no CSV files are found, return None
    if not csv_files:
        return None

    # Define a regular expression pattern to extract numbers from filenames
    pattern = re.compile(r'batch_(\d+)')

    # Initialize variables to store the maximum number and corresponding filename
    max_number = 0
    max_filename = None

    # Iterate through the CSV files and find the maximum number
    for csv_file in csv_files:
        match = pattern.search(csv_file)
        if match:
            number = int(match.group(1))
            if number > max_number:
                max_number = number
                max_filename = csv_file

    # Return the filename with the maximum number
    return max_number

def merge_files_by_pattern(directory_path, pattern, output_file_path):
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


def process_csv_files(directory_path, file_path_processed):
    # Use glob to find all CSV files in the directory
    csv_files = glob(f"{directory_path}/*.csv")

    # Check if the target directory exists, if not, create it
    if not os.path.exists(file_path_processed):
        os.makedirs(file_path_processed)

    # Loop through each CSV file and perform DataFrame operation
    for csv_file in csv_files:
        # split file by separating ext from file name
        file_name = csv_file.split("/")[-1].split(".csv")[0]
        modified_csv_file = os.path.join(file_path_processed, f"{file_name}_modified.csv")
        print(modified_csv_file)
        if not os.path.exists(modified_csv_file):
            file_name_with_extension = os.path.basename(csv_file)
            
            # Save the modified DataFrame back to the CSV file or to a new file
            furtherProcessiLoveValid(directory_path, file_name_with_extension, modified_csv_file)
            
            print(f"Processed: {csv_file} -> {modified_csv_file}")
        else:
            print("Already processed...")
        


def furtherProcessiLoveValid(dir, CSV_FILE, file_modified_name ):
    data = pd.read_csv(dir+CSV_FILE)
    result_dataframe = process_data_valid(data)
    result_dataframe.to_csv(file_modified_name, index=False)
        
def furtherProcessiLove(dir, CSV_FILE, file_modified_name ):
    # check if file already exist to avoid start all over again.
    # dir = "./results/"
    create_directory_if_not_exists(dir)
    # CSV_FILE = 'LOVEDB.csv'
    if check_file_existence(dir, CSV_FILE):
        print("We have a file so we do not need to download anything...")
        # itemInfo_itemIdList_doi
        df = pd.read_csv(dir+CSV_FILE)
        # df =df.head(4)
        # check if DF has 'full_text_URL', 'full_text_content_type'
        if(not 'full_text_URL' in df.columns and not 'full_text_content_type' in df.columns):
            result = df['doi'].apply(lambda row: getDOI(row))
            # Create a new DataFrame with the results
            result_df = pd.DataFrame(result.tolist(), columns=['full_text_URL', 'full_text_content_type'])
            # Concatenate the new DataFrame with the original DataFrame
            df = pd.concat([df, result_df], axis=1)
        # Use a context manager to ensure proper file closure
        df.to_csv(dir+CSV_FILE, index=False)
        """Create our new dataframe and save it"""
        process_new_sheet(df).to_csv(file_modified_name)
    else:
        # itemInfo_itemIdList_doi
        df = pd.read_csv(dir+CSV_FILE)
        # df = df.head(4)
        # print(df['itemInfo_itemIdList_doi'])
        # df['full_text'] = df['itemInfo_itemIdList_doi'].apply(lambda row: getContent("", row))
        result = df['doi'].apply(lambda row: getDOI(row))
        # Create a new DataFrame with the results
        result_df = pd.DataFrame(result.tolist(), columns=['full_text_URL', 'full_text_content_type'])
        # Concatenate the new DataFrame with the original DataFrame
        df = pd.concat([df, result_df], axis=1)
        # Save DataFrame to a CSV file
        df.to_csv(dir+CSV_FILE, index=False)
        process_new_sheet(df).to_csv(file_modified_name)