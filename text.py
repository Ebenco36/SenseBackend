import json
import re
import requests
from src.Utils.Helpers import (
    ageRangeSearchAlgorithm,
    check_file_existence,
    convert_dict_to_dataframe,
    create_columns_from_text, 
    create_directory_if_not_exists,
    extract_identifier_from_url,
    find_overlapping_groups,
    getDOI,
    is_sciencedirect_url,
    is_within_range,
    process_data_valid, process_new_sheet,
    replace_with_acronyms,
    search_and_extract_html,
    unique_elements,
    xml_to_text
)
from src.Utils.Reexpr import pattern_dict_regex
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from src.Utils.data import population_acronyms
from src.Utils.ResolvedReturn import preprocessResolvedData
from src.api_utils import embase_access, ilove_access, cochrane_access, medline_access


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




def furtherProcessiLoveRunthrough():
    # check if file already exist to avoid start all over again.
    dir = "./results/"
    create_directory_if_not_exists(dir)
    CSV_FILE = 'LOVEDB.csv'
    if check_file_existence(dir, CSV_FILE):
        print("We have a file so we do not need to download anything...")
        # itemInfo_itemIdList_doi
        df = pd.read_csv(dir+CSV_FILE)
        df = df.rename(columns=lambda x: x.strip())
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
        process_new_sheet(df).to_csv("./results/ProcessedLOVEDB.csv")
    else:
        # itemInfo_itemIdList_doi
        df = pd.read_csv('L-OVE/LOVE.csv')
        df = df.rename(columns=lambda x: x.strip())
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
        process_new_sheet(df).to_csv("./results/ProcessedLOVEDB.csv")
# loopThrough()

# print(search_and_extract_html("https://www.sciencedirect.com/science/article/pii/S2405852118300211", pattern_dict_regex))

# ilove_access()


#cochrane_access()


# embase_access()



medline_access()

from src.Utils.Reexpr import searchRegEx

from src.Utils.Helpers import search_and_extract_html_valid





# Read data from CSV
# data = pd.read_csv("withFullTextLink.csv")
# result_dataframe = process_data_valid(data)
# print(result_dataframe.to_csv("testHH.csv"))
