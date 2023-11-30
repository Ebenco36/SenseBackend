import os
import re
import pandas as pd
from App.Services.Service import Service
from App.Request.ApiRequest import ApiRequest
from App.Utils.Helpers import save_json_to_csv
from App.Utils.Helpers import create_directory_if_not_exists, save_json_to_csv, append_json_response_to_file, \
    json_to_dataframe_and_save, get_remainder_and_quotient, convert_json_to_List_of_dict


class LoveV(Service):

    def __init__(self, pageSize = 200):
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
        url = 'https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references?metadata_ids=5e7fce7e3d05156b5f5e032a,603b9fe03d05151f35cf13dc&classification_filter=systematic-review,primary-study&hide_excluded=true&page=' + str(page) + '&sort_by=year&show_summary=true'
        print(url)
        # print(page)
        record_details_data = ApiRequest('json', url, headers=self.auth_headers)
        data = record_details_data.fetch_records()
        
        rec = record_details_data.fetch_records()
        data = rec.get('data')
        return data
    
    """
        We are following the structure as described on EMBASE Server.
    """

    def executeRetrieveData(self, batch_size=10, save_interval=200, max_records=0):
        # 10 is the number of record per page
        max_records = 10 * save_interval
        # Save DataFrame to a CSV file
        file_path = 'L-OVE/Batches/'

        create_directory_if_not_exists(file_path)

        
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

        print("All records saved successfully.")
        return self
    

    def executeRetrieveData1(self):
        create_directory_if_not_exists("L-OVE/")
        page = 1
        all_records = []

        while True:
            # Fetch records for the current page
            records = self.retrieveRecord(page)
            # Break the loop if no more records are returned
            if not records or len(records.get('items')) == 0:
                break

            # Append records to the list
            all_records.extend(records.get('items'))

            # Move to the next page
            page += 1

        # Convert the list of records to a pandas DataFrame
        df = pd.DataFrame(all_records)
        # Save DataFrame to a CSV file
        csv_filename = 'L-OVE/records.csv'
        df.to_csv(csv_filename, index=False, encoding='utf-8')

        print(f"Records spooled and saved to {csv_filename}.")
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