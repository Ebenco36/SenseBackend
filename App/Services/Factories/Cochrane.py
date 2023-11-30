import pandas as pd
from App.Services.Service import Service
from App.Request.ApiRequest import ApiRequest
from App.Utils.Helpers import save_json_to_csv
from App.Utils.Helpers import create_directory_if_not_exists, save_json_to_csv, append_json_response_to_file, \
    json_to_dataframe_and_save, get_remainder_and_quotient, convert_json_to_List_of_dict


class Cochrane(Service):
    # token from cochrane fca8eec8-dc0c-4d58-99f0-11483bbc5ad0
    # https://onlinelibrary.wiley.com/library-info/resources/text-and-datamining
    def __init__(self, pageSize = 200):
        self.pageSize = pageSize


    def authenticate(self, headers):
        self.auth_headers = headers
        return self

    def fetch(self, headers):
        return self.authenticate(headers).executeRetrieveData()

    """
        Process flow of retrieving records from EMBASE
        Stages are listed here.
    """
    
    # @retry(exceptions=requests.exceptions.ProxyError, tries=3, delay=2, backoff=2)                
    def retrieveRecord(self, page = 0):
        url = 'https://api.iloveevidence.com/v2.1/loves/5d7e804169c00e72a5188e50/references?metadata_ids=5d7e803069c00e72a5188e4f&classification_filter=systematic-review,primary-study&hide_excluded=true&page=' + str(page) + '&sort_by=year&show_summary=true'
        print(url)
        record_details_data = ApiRequest('json', url, headers=self.auth_headers)
        data = record_details_data.fetch_records()
        if (data.get('status') and data.get('status') is True):
            rec = record_details_data.fetch_records()
            data = rec.get('data')
        else:
            print(data)
            data = None
        return data
    
    """
        We are following the structure as described on EMBASE Server.
    """
    def executeRetrieveData(self):
        create_directory_if_not_exists("L-OVE/")
        page = 1
        all_records = []

        while True:
            # Fetch records for the current page
            records = self.retrieveRecord(page)

            # Break the loop if no more records are returned
            if not records:
                break

            # Append records to the list
            all_records.extend(records)

            # Move to the next page
            page += 1

        # Convert the list of records to a pandas DataFrame
        df = pd.DataFrame(all_records)

        # Save DataFrame to a CSV file
        csv_filename = 'L-OVE/records.csv'
        df.to_csv(csv_filename, index=False, encoding='utf-8')

        print(f"Records spooled and saved to {csv_filename}.")
        return self.retrieveRecord()
        
