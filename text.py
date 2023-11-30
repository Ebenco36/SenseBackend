import requests
from App.Utils.Helpers import (
    check_file_existence, 
    create_directory_if_not_exists,
    getDOI, process_new_sheet,
    search_and_extract_html
)
from App.Utils.Reexpr import pattern_dict_regex
import pandas as pd

from App.api_utils import ilove_access


def loopThrough():
    # check if file already exist to avoid start all over again.
    dir = "./results/"
    create_directory_if_not_exists(dir)
    CSV_FILE = 'withFullTextLink.csv'
    if check_file_existence(dir, CSV_FILE):
        print("We have a file so we do not need to download anything...")
        # itemInfo_itemIdList_doi
        df = pd.read_csv(dir+CSV_FILE)
        # df =df.head(4)
        # check if DF has 'full_text_URL', 'full_text_content_type'
        if(not 'full_text_URL' in df.columns and not 'full_text_content_type' in df.columns):
            result = df['itemInfo_itemIdList_doi'].apply(lambda row: getDOI(row))
            # Create a new DataFrame with the results
            result_df = pd.DataFrame(result.tolist(), columns=['full_text_URL', 'full_text_content_type'])
            # Concatenate the new DataFrame with the original DataFrame
            df = pd.concat([df, result_df], axis=1)
        # Use a context manager to ensure proper file closure
        df.to_csv(dir+CSV_FILE, index=False)
        """Create our new dataframe and save it"""
        process_new_sheet(df).to_csv("./results/ProcessedDataNew.csv")
    else:
        # itemInfo_itemIdList_doi
        df = pd.read_csv('EMBASE/EMBASECOMBINED.csv')
        # df = df.head(4)
        # print(df['itemInfo_itemIdList_doi'])
        # df['full_text'] = df['itemInfo_itemIdList_doi'].apply(lambda row: getContent("", row))
        result = df['itemInfo_itemIdList_doi'].apply(lambda row: getDOI(row))
        # Create a new DataFrame with the results
        result_df = pd.DataFrame(result.tolist(), columns=['full_text_URL', 'full_text_content_type'])
        # Concatenate the new DataFrame with the original DataFrame
        df = pd.concat([df, result_df], axis=1)
        # Save DataFrame to a CSV file
        df.to_csv(dir+CSV_FILE, index=False)
        process_new_sheet(df).to_csv("./results/ProcessedDataNew.csv")

# loopThrough()

# print(search_and_extract_html("https://www.sciencedirect.com/science/article/pii/S2405852118300211", pattern_dict_regex))

ilove_access()