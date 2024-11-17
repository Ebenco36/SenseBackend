import ast
import os
import csv
import re
import json
import xmltodict
from urllib.parse import urlparse
import requests
import pycountry
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from pandas import json_normalize
import xml.etree.ElementTree as ET
from src.Utils.Reexpr import searchRegEx
# from PyPDF2 import PdfReader, PdfFileReader, errors
from pypdf import PdfReader, PdfReader, errors
from src.Utils.data import population_acronyms
from src.Utils.Reexpr import pattern_dict_regex
from src.Utils.ResolvedReturn import ourColumns, preprocessResolvedData

"""
    Sample Data for Header
    ==============================================
    GET /facet/1/filter/applied HTTP/2
    Host: www.embase.com
    User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0
    Accept: application/json, text/javascript, */*; q=0.01
    Accept-Language: en-US,en;q=0.5
    Accept-Encoding: gzip, deflate, br
    Referer: https://www.embase.com/
    X-NewRelic-ID: VgYOVFRVChABUVZQAgUEUlAF
    newrelic: eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjIwOTUyNjIiLCJhcCI6IjE1ODg2ODc2MjUiLCJpZCI6ImNiNjFlNWQ4NjBkNDJhMjciLCJ0ciI6ImI1YTI1NDc5YjkwOTE4YzkxZjViYWIxNTFhMDc1NzAwIiwidGkiOjE2OTM0MDE1Mzc3ODUsInRrIjoiMjAzODE3NSJ9fQ==
    traceparent: 00-b5a25479b90918c91f5bab151a075700-cb61e5d860d42a27-01
    tracestate: 2038175@nr=0-1-2095262-1588687625-cb61e5d860d42a27----1693401537785
    Content-Type: application/json; charset=utf-8
    X-Requested-With: XMLHttpRequest
    DNT: 1
    Connection: keep-alive
    Cookie: EMBASE_TRACKING_ID=1ff718bb-c9cd-4e0a-99d7-8b5fe4a9f9e3; AMCV_4D6368F454EC41940A4C98A6%40AdobeOrg=-2121179033%7CMCIDTS%7C19600%7CMCMID%7C01214066067756635329220484246223413350%7CMCAID%7CNONE%7CMCOPTOUT-1693408737s%7CNONE%7CvVersion%7C5.3.0; s_pers=%20v8%3D1693401537548%7C1788009537548%3B%20v8_s%3DLess%2520than%25201%2520day%7C1693403337548%3B%20c19%3Dem%253Aresults%253Aresults%253Aother%253A3.4%2520search%2520submit%7C1693403337550%3B%20v68%3D1693401482068%7C1693403337553%3B; AWSALB=FNsnv+JyHcN20U3k27tVgn8UqVnlCjBKDIam9AtGOjALc5z0zrXw0ilCYMYQmKKKcsH3zyOpEfL1+Kw9ZGVWxkR9aWIIDfsEf3936UXGktAFVY+TA0T/LEJAJ0ZO; AWSALBCORS=FNsnv+JyHcN20U3k27tVgn8UqVnlCjBKDIam9AtGOjALc5z0zrXw0ilCYMYQmKKKcsH3zyOpEfL1+Kw9ZGVWxkR9aWIIDfsEf3936UXGktAFVY+TA0T/LEJAJ0ZO; search_maptoemtree=; search_majorfocus=; search_narrowterms=; search_extensive=; search_map_explosion_extensive=; search_since=; search_to=; initialSearchValue=; JSESSIONID=551B9FFAFA021C2E1275EDEA49BABC11; EMBASE_TRACKING_ID=1ff718bb-c9cd-4e0a-99d7-8b5fe4a9f9e3; SESSION=F752426E7C2D7AC259A167D464C07093; at_check=true; AMCVS_4D6368F454EC41940A4C98A6%40AdobeOrg=1; s_sess=%20s_cpc%3D0%3B%20s_cc%3Dtrue%3B%20s_sq%3D%3B%20c21%3Dd91d1ba6-1ccd-115b-a1bb-9037d2b523dc%3B%20e13%3D%253A%3B%20e41%3D1%3B%20s_ppvl%3Dem%25253Asearch%25253Aadvanced%252520search%25253Aother%25253A1.2%252520advanced%252520search%252C100%252C45%252C831%252C1438%252C360%252C1512%252C945%252C1%252CP%3B%20s_ppv%3Dem%25253Aresults%25253Aresults%25253Aother%25253A3.4%252520search%252520submit%252C53%252C53%252C416%252C1438%252C360%252C1512%252C945%252C1%252CP%3B; historyExpanded=null; __cf_bm=uMg_izxPX2MgqclPwi9Rie9fluiqtz39sZeMlzqmPO4-1693401452-0-ASh214eH97NMMnA8s0u8ucDUNqscCtM6inHebWs/TG3jZ2P7sy3BSv+aKTE2r7yXnEwSP1JxrVkaxhSIMIkBXys=; mbox=session#2a16c666bc94415dac324dcb9561ae65#1693403342
    Sec-Fetch-Dest: empty
    Sec-Fetch-Mode: cors
    Sec-Fetch-Site: same-origin
    TE: trailers
"""
def format_text_to_json(content):
    content_lines = content.strip().split('\n')
    content_dict = {}

    for line in content_lines:
        key, value = line.split(':', 1)
        content_dict[key.strip()] = value.strip()

    return content_dict

def save_json_to_csv(json_data, csv_filename):
    if not json_data:
        print("No JSON data to save.")
        return
    
    if not isinstance(json_data, list):
        print("JSON data must be a list of dictionaries.")
        return
    
    # Open the CSV file for writing
    with open(csv_filename, "w", newline="") as csvfile:
        # Define CSV writer
        csv_writer = csv.DictWriter(csvfile, fieldnames=json_data[0].keys())
        
        # Write header row
        csv_writer.writeheader()
        
        # Write JSON data to CSV
        csv_writer.writerows(json_data)

def append_json_response_to_file(json_response, filename):
    try:
        with open(filename, "a") as f:
            json.dump(json_response, f, indent=4)
            f.write("\n")  # Add a newline between JSON objects
            
        print("JSON response saved to", filename)
    
    except Exception as e:
        print("Error:", e)
  
def json_to_dataframe_and_save(json_data, csv_filename):

    # Convert JSON data to a Pandas DataFrame
    df = json_normalize(json_data, sep='_', max_level=2)
    # # Convert JSON data to a Pandas DataFrame
    # df = pd.DataFrame(json_data)
    
    # Save the DataFrame to a CSV file
    df.to_csv(csv_filename, index=False)
    
    print("DataFrame saved to", csv_filename)

def get_remainder_and_quotient(dividend, divisor):
    # Calculate the remainder using the modulo operator (%)
    remainder = dividend % divisor
    
    # Calculate the quotient using floor division (//)
    quotient = dividend // divisor

    round_up_value = round(dividend / divisor)
    
    return remainder, quotient, round_up_value

def convert_json_to_List_of_dict():
    # Define the pattern you want to search for
    pattern = r'EMBASEexportPage_\d+__\d+'

    # Specify the directory where you want to search for files
    directory_path = './EMBASE/'
    # Initialize an empty dictionary to store the merged data
    merged_data = []
    # Loop through all files in the specified directory
    for filename in os.listdir(directory_path):
        # Check if the filename matches the pattern
        if re.search(pattern, filename):
            # If it matches, print or process the filename as needed
            print(f"Found matching file: {filename}")
            with open(filename, 'r') as infile:
                merged_data.extend(json.load(infile).get('bibrecords'))
    merge_path = directory_path + '/EMBASECOMBINED.json'
    with open(merge_path, 'w') as output_file:
        json.dump(merged_data, output_file)

    # generate the CSV file for further preprocessing
    convert_list_of_dict_to_csv(merge_path)

    print("Combined record saved to path: " + str(merge_path))

def convert_list_of_dict_to_csv(filename):
    with open(filename, 'r') as infile:
        content = json.load(infile)
        json_to_dataframe_and_save(content, "EMBASECOMBINED.csv")

def create_directory_if_not_exists(directory_path):
    """
    Creates a directory if it doesn't exist.

    Args:
        directory_path (str): The path of the directory to be created.
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")

def check_file_existence(directory_path, file_name):
    """
    Checks if a file exists within a directory.

    Args:
        directory_path (str): The path of the directory to search in.
        file_name (str): The name of the file to check for existence.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    file_path = os.path.join(directory_path, file_name)
    return os.path.exists(file_path)

def getDOI(doi):
    # Construct the CrossRef API URL
    crossref_api_url = f'https://api.crossref.org/works/{doi}'

    # Send a GET request to the CrossRef API
    response = requests.get(crossref_api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Extract the URL from the response
        url = data['message']['link'][0]['URL']
        
        return url
    else:
        return doi

def downloadDOIPDF(doi_path):
    base_url = 'https://dx.doi.org/'
    # Replace 'your_pdf_url' with the URL of the PDF you want to extract text from
    doi_url = base_url + doi_path

    # doi_url = 'https://dx.doi.org/10.1038/s41435-018-0040-1'
    pdf_url = getDOI(doi_url)

    # Send an HTTP GET request to download the PDF
    response = requests.get(pdf_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Create a file-like object from the PDF content
        pdf_file = open(doi_path + '.pdf', 'wb')
        pdf_file.write(response.content)
        pdf_file.close()

        # Open the downloaded PDF file using PdfReader
        pdf_reader = PdfReader(doi_path + '.pdf')

        # Initialize a variable to store the extracted text
        extracted_text = ''

        # Iterate through each page in the PDF
        for page in pdf_reader.pages:
            # Extract the text from the current page
            extracted_text += page.extract_text()

        # Print or manipulate the extracted text as needed
        return extracted_text
    else:
        return f'Failed to download the PDF from the URL. Status code: {response.status_code}'

"""Column content processing"""
"""
    column content with [{'content': 'EMBASE'}, {'content': 'MEDLINE'}]
    can be transformed to EMBASE, MEDLINE
"""
def convert_content_to_comma_separated_string(items, key_focus):
    try:
        row = ast.literal_eval(items)
        return ', '.join([item.get(key_focus) for item in row])
    except (ValueError, SyntaxError):
        return ""

# Define a function to extract 'ttltext'
def extract_ttltext(row):
    try:
        row = ast.literal_eval(row)
        return row[0]['ttltext'].replace('@hit_start', '').replace('@hit_end', '')
    except Exception as e:
        return row

# Define a function to extract 'paras' field
def extract_paras(row):
    try:
        row = ast.literal_eval(row)
        abstract = row[0]['paras'][0].replace('@hit_start', '').replace('@hit_end', '')
        return abstract
    except Exception as e:
        return row

def extract_source(row):
    try:
        row = ast.literal_eval(row)
        journal = row[0]
        return journal
    except Exception as e:
        return row

# Define a function to extract values from the JSON string safely
def extract_citations(row):
    try:
        row = ast.literal_eval(row)
        citation_type = extract_citation_type(row[0].get('citationType', []))
        citation_lang = extract_citation_language(row[0].get('citationLanguage', []))
        citation_keywords = extract_author_keywords(row[0].get('authorKeywords', {}))
        return citation_type, citation_lang, citation_keywords
    except Exception as e:
        return None, None, None  # Return None if there is an error in parsing
    
# process the citation column
# Define a function to extract values
def extract_citation_type(row):
    return row[0].get('content', '')

def extract_citation_language(row):
    citation_language = row[0].get('content', '')
    return citation_language.replace('@hit_start', '').replace('@hit_end', '')

def extract_author_keywords(row):
    return ', '.join(row.get('authorKeyword', [])).replace('@hit_start', '').replace('@hit_end', '')

def extract_country(row):
    try:
        row = ast.literal_eval(row)
        country = row[0]['affiliation']['country']['content']
        return country
    except (KeyError, Exception):
        return row

def process_domains(row):
    try:
        row = ast.literal_eval(row)
        # Initialize variables to store 'domain' and 'other_domain' values
        domain_values = []
        other_domain_values = []

        # Process the 'data' column in the current row
        for item in row['other']:
            mainterm_content = item['mainterm']['content']
            ancestor_list = item['ancestor']

            # Accumulate 'domain' values in a list
            domain_values.append(mainterm_content.replace('@hit_start', '').replace('@hit_end', ''))

            # Accumulate 'other_domain' values as a list of dictionaries
            other_domain_values.append({'ancestor': ancestor_list})

        # Update the 'data' column with the processed result
        domain = ', '.join(domain_values)
        other_domain = other_domain_values

        return domain, other_domain
    except (ValueError, SyntaxError):
        return ""

# Function to extract keywords from a PDF file
def extract_keywords_from_pdf(pdf_file_path, keyword_pattern = None):
    try:
        # Open the PDF file
        pdf_file = open(pdf_file_path, 'rb')
        pdf_reader = PdfReader(pdf_file)

        # Extract text from the PDF
        pdf_text = ''
        for page_num in range(pdf_reader.numPages):
            page = pdf_reader.getPage(page_num)
            pdf_text += page.extractText()

        # Extract keywords using regex
        keywords = re.findall(keyword_pattern, pdf_text, flags=re.IGNORECASE)

        return keywords

    except Exception as e:
        print(f"Error: {e}")
        return []

# Function to extract keywords from HTML content
def extract_keywords_from_html(url, keyword_pattern = None):
    try:
        # Fetch HTML content from the URL
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        response = requests.get(url, headers={'User-Agent': user_agent})
        html_content = response.text

        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract text content from the HTML
        text = soup.get_text()
        # Open the file in write mode ('w')
        with open("file_path.txt", 'w') as file:
            # Write the text to the file
            file.write(text)
        # Extract keywords using regex
        keywords = re.findall(keyword_pattern, text, flags=re.IGNORECASE)

        return keywords

    except Exception as e:
        print(f"Error: {e}")
        return []

def remove_html_tags(text):
    """Remove html tags from a string"""
    import re
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def extract_identifier_from_url(url):
    # Define a regular expression pattern to match the identifier
    pattern = r'/pii/([A-Z0-9]+)$'

    # Use re.search to find the pattern in the URL
    match = re.search(pattern, url)

    # Check if a match is found
    if match:
        identifier = match.group(1)
        return identifier
    else:
        return None


def is_sciencedirect_url(url):
    # Parse the URL
    parsed_url = urlparse(url)

    # Check if the scheme is 'https' and the netloc (host) is 'www.sciencedirect.com'
    return parsed_url.scheme == 'https' and parsed_url.netloc == 'www.sciencedirect.com'

def xml_to_text(xml_content):
    # Parse the XML content
    root = ET.fromstring(xml_content)

    # Function to recursively extract text from XML elements
    def extract_text(element):
        text = element.text or ''
        for child in element:
            text += extract_text(child)
        return text

    # Extract text from the root element
    complete_text = extract_text(root)

    return complete_text

def search_and_extract_html_valid(url):
    # Create a session
    with requests.Session() as session:
        max_retries = 5  # Set the maximum number of retry attempts

        for retry_count in range(1, max_retries + 1):
            try:
                # Fetch the HTML content of a webpage
                # science direct with exception to APIKEY
                if(is_sciencedirect_url(url)):
                    key_id = extract_identifier_from_url(url)
                    url = "https://api.elsevier.com/content/article/pii/"+key_id+"?apiKey=4c0c39804a18d64a4fc58bc1953c05bc"

                user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                response = requests.get(url, headers={'User-Agent': user_agent})
                if(response.status_code == 403):
                    scraper = cloudscraper.create_scraper()
                    response = scraper.get(url, headers={'User-Agent': user_agent})

                if response.status_code == 200:
                    _content = response.content

                    if(is_sciencedirect_url(url)):
                        page_content = xml_to_text(_content)
                    else:
                        # Parse HTML using BeautifulSoup
                        soup = BeautifulSoup(_content, 'html.parser')

                        # Extract text from the parsed HTML
                        page_content = soup.get_text()

                    result = create_columns_from_text(page_content, searchRegEx)
                    print("We are done with " + url)
                    return result
                elif response.status_code == 403:
                    print(f"Received 403 error on attempt {retry_count}. Retrying...")
                else:
                    print(f"Failed to retrieve the paper. Status code: {response.status_code}")
                    # break  # Break the loop for other status codes
            except Exception as e:
                return {"error": str(e)}
            
def process_data_valid(data, key='doi'):
    new_data = pd.DataFrame()
    for column_ in data.columns:
        column = column_.strip()
        # if column in columns_to_process:
        if column == 'itemInfo_dbCollection':
            new_data["DBCOllection"] = data[column].apply(lambda x: convert_content_to_comma_separated_string(x, "content"))
        if column == 'head_correspondence' or column == 'PL' or column == 'countries':
            new_data["country"] = data[column_].apply(extract_country)
            new_data["region"] = new_data["country"].apply(get_region_by_country_name)
        if column == 'head_citationTitle_titleText' or column == 'TI' or column == "title":
            new_data["title"] = data[column_].apply(extract_ttltext)
        if column == 'head_abstracts_abstracts' or column == 'AB' or column == "abstract":
            new_data["abstract"] = data[column_].apply(extract_paras)
        if column == 'head_source_sourceTitle' or column == 'JT' or column == "journal":
            new_data["journal"] = data[column_].apply(extract_source)
        if column == 'head_source_publicationYear' or column == "year":
            new_data["year"] = data[column_]
        if column == 'head_source_publicationDate' or column == 'DP':
            new_data["publication_date_obj"] = data[column_]
            if(column == 'DP'):
                # Extract the year and create a new 'Year' column
                try:
                    new_data['year'] = pd.to_datetime(new_data['publication_date_obj'], errors='coerce').dt.year
                except (Exception) as e:
                    pass
        if column == 'itemInfo_itemIdList_doi' or column == 'doi' or column == 'AID':
            new_data["doi"] = data[column_]
        if column == 'head_enhancement_descriptors':
            domains_data = data[column_].apply(process_domains)
            domain_df = pd.DataFrame(domains_data.tolist(), columns=['domain', 'other_domain'])
            new_data = pd.concat([new_data, domain_df], axis=1)
        if column == 'head_authorList_authors' or column == 'authors' or column == 'AU':
            new_data['authors'] = data[column_]
        if column == 'itemInfo_itemIdList_pii' or column == 'id' or column == 'IS':
            new_data['PII'] = data[column_]
        if column == 'full_text_URL':
            new_data['URL'] = data[column_]
        if column == 'full_text_content_type':
            new_data['document_type'] = data[column_]
        if column == 'head_citationInfo':
            citation_data = data[column_].apply(extract_citations)
            citation_df = pd.DataFrame(citation_data.tolist(), columns=['citation_type', 'citation_lang', 'citation_keywords'])
            new_data = pd.concat([new_data, citation_df], axis=1)
        else:
            pass
            # new_data[column] = data[column_]
    # check if url exist
    if(not 'full_text_URL' in new_data.columns):    
        # Apply the getDOI function to the 'doi' column
        result = new_data[key].apply(lambda row: getDOI(row))

        # Create a new DataFrame with the results
        result_df = pd.DataFrame(result.tolist(), columns=['full_text_URL', 'full_text_content_type'])

    # Apply search_and_extract_html to the 'full_text_URL' column
    keyword_data = result_df['full_text_URL'].apply(lambda x: search_and_extract_html_valid(x))

    # Convert the list of dictionaries to a DataFrame
    # Filter out None values
    keyword_data = keyword_data.dropna()

    # Check if there are any valid results before creating the DataFrame
    if not keyword_data.empty:
        keyword_data_df = pd.DataFrame.from_records(keyword_data)

        # Concatenate the DataFrames horizontally
        merged_df = pd.concat([new_data, result_df, keyword_data_df], axis=1)
    else:
        merged_df = pd.DataFrame()

    return merged_df

   
def search_and_extract_html(url, patterns:dict = {}, context_length=100):
    # Create a session
    with requests.Session() as session:
        max_retries = 5  # Set the maximum number of retry attempts

        for retry_count in range(1, max_retries + 1):
            try:
                # Fetch the HTML content of a webpage
                # science direct with exception to APIKEY
                if(is_sciencedirect_url(url)):
                    key_id = extract_identifier_from_url(url)
                    url = "https://api.elsevier.com/content/article/pii/"+key_id+"?apiKey=4c0c39804a18d64a4fc58bc1953c05bc"

                user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                response = requests.get(url, headers={'User-Agent': user_agent})
                if(response.status_code == 403):
                    scraper = cloudscraper.create_scraper()
                    response = scraper.get(url, headers={'User-Agent': user_agent})

                if response.status_code == 200:
                    _content = response.content

                    if(is_sciencedirect_url(url)):
                        page_content = xml_to_text(_content)
                    else:
                        # Parse HTML using BeautifulSoup
                        soup = BeautifulSoup(_content, 'html.parser')

                        # Extract text from the parsed HTML
                        page_content = soup.get_text()

                    # loop through each content
                    resolved_result = {}
                    for pattern in patterns:
                        # Find all matches within the HTML content
                        matches = re.finditer(patterns[pattern], page_content)
                        # Initialize a list to store results
                        results = []
                        match_group = []
                        for match in matches:
                            match_text = match.group(0)
                            start_index = match.start()
                            end_index = match.end()
                            text_before = page_content[max(0, start_index - context_length):start_index]
                            text_after = page_content[end_index:end_index + context_length]

                            results.append({
                                "Matched Content": match_text,
                                "Text Before": text_before,
                                "Text After": text_after
                            })
                            match_group.append(match_text)
                        match_group = unique_elements(match_group)
                        tags = ", ".join(match_group)
                        results = json.dumps(results)
                        short_acronyms = ", ".join(replace_with_acronyms(tags, population_acronyms))
                        resolved_result[pattern] = [tags, short_acronyms, results]
                    
                    print("successfully downloaded data for : " + url)
                    return preprocessResolvedData(resolved_result)
                elif response.status_code == 403:
                    print(f"Received 403 error on attempt {retry_count}. Retrying...")
                else:
                    print(f"Failed to retrieve the paper. Status code: {response.status_code}")
                    break  # Break the loop for other status codes
            except Exception as e:
                return str(e), str(e)
        
def unique_elements(input_list):
    return [x for i, x in enumerate(input_list) if x not in input_list[:i]]

def search_and_extract_pdf(pdf_file_path, pattern, context_length=100):
    try:
        # Open the PDF file
        pdf_file = open(pdf_file_path, 'rb')

        # Create a PDF reader object
        pdf_reader = PdfReader(pdf_file)

        # Initialize a list to store results
        results = []
        match_group = []
        for page_number in range(pdf_reader.getNumPages()):
            page = pdf_reader.getPage(page_number)
            page_text = page.extractText()

            # Find all matches within the page's text
            matches = re.finditer(pattern, page_text)

            for match in matches:
                match_text = match.group(0)
                start_index = match.start()
                end_index = match.end()
                text_before = page_text[max(0, start_index - context_length):start_index]
                text_after = page_text[end_index:end_index + context_length]

                results.append({
                    "Matched Content": match_text,
                    "Text Before": text_before,
                    "Text After": text_after
                })
                match_group.append(match_text)
            match_group = unique_elements(match_group)
        return ", ".join(match_group), results
    except Exception as e:
        return str(e)
    finally:
        # Close the PDF file
        pdf_file.close()

def get_region_by_country_name(country_name):
    url = f"https://restcountries.com/v3.1/name/{country_name}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data:
                return data[0].get('region', 'Region not found')
            else:
                return "Country not found"
        else:
            return "Region not found"
    except Exception as e:
        print(f"Error: {e}")
        return "Failed to retrieve region"

def getDOI(doi):
    # Construct the CrossRef API URL
    crossref_api_url = f'https://api.crossref.org/works/{doi}'

    # Send a GET request to the CrossRef API
    response = requests.get(crossref_api_url)

    # Check if the request was successful (status code 200)
    data_url = ""
    type = ""
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        if 'message' in data and 'resource' in data['message']:
            # Extract the URL from the response
            content = data['message']['resource']['primary'] \
                if 'primary' in data['message']['resource'] \
                    else ""
            
            content_type = data['message']['link'][0] \
                if 'link' in data['message'] and 'URL' in data['message']['link'][0] \
                    else ""
            if(('content-type' in content_type) and content_type['content-type'] == "application/pdf"):
                data_url = content['URL']
                type = 'PDF'
            elif(('content-type' in content_type) and content_type['content-type'] == "text/xml"):
                data_url = getLinkInXML(content_type['URL'])
                type = 'XML'
            elif(('content-type' in content_type) and content_type['content-type'] == "text/html"):
                data_url = content['URL']
                type = 'HTML'
            elif(('content-type' in content_type) and content_type['content-type'] == "unspecified"):
                # check if file is pdf since content type is not specified.
                data_url = content['URL']
                type = 'NOTSPECIFIED'
            else:
                # check if file is pdf since content type is not specified.
                data_url = content['URL']
                type = 'NOTSPECIFIED'
        else:
            print("No Links for: " + crossref_api_url)
    else:
        data_url = f'Failed to resolve DOI {doi}. Status code: {response.status_code}'
        type = "None"
    return data_url, type

def getContent(old_content = '', doi_path = ""):
    base_url = 'https://dx.doi.org/'
    # Replace 'your_pdf_url' with the URL of the PDF you want to extract text from
    doi_url = base_url + str(doi_path)
    
    url, type = getDOI(doi_url)
    content = ""
    if(type == "XML"):
        content = getLinkInXML(url)
    elif(type == "PDF"):
        content = downloadDOIPDF(url, doi_path)
    elif(type == "HTML" or type == "NOTSPECIFIED"):
        content = htmlToText(url)

    data = content if pd.notna(content) and content != "" else old_content

    return data

def getLinkInXML(url):
    # Send an HTTP GET request to fetch the XML content
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    response = requests.get(url, headers={'User-Agent': user_agent})
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the XML content
        xml_content = response.text
        # Parse the XML content to get the root element
        root = ET.fromstring(xml_content)

        # Get the default namespace from the root element
        namespace = root.tag.split('}')[0][1:]

        # Find the link with rel="scidir" using the dynamically obtained namespace
        scidir_link = root.find(".//{%s}link[@rel='scidir']" % namespace)

        if scidir_link is not None:
            scidir_url = scidir_link.get('href')
            return scidir_url
        else:
            print("The 'scidir' link was not found in the XML.")
            return None

def htmlToText(html_url):
    # Send an HTTP GET request to fetch the HTML content
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(html_url, headers=headers)
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract the text from the parsed HTML
        plain_text = soup.get_text()
        return plain_text
    else:
        print(html_url)
        print(f'Failed to fetch HTML content from the URL. Status code: {response.status_code}')

def processPDFDoc(response_content, save_file):
    try:
        dir = "pdfs/"
        if response_content and response_content != "":
            # Create a file-like object from the PDF content
            pdf_file = open(dir + "/" + str(save_file) + '.pdf', 'wb')
            pdf_file.write(response_content)
            pdf_file.close()
        # Open the downloaded PDF file using PdfReader
        pdf_reader = PdfReader(dir + "/" + str(save_file) + '.pdf')

        # Initialize a variable to store the extracted text
        extracted_text = ''

        # Iterate through each page in the PDF
        for page in pdf_reader.pages:
            # Extract the text from the current page
            extracted_text += page.extract_text()

        # Print or manipulate the extracted text as needed
        return extracted_text
    except errors.PdfReadError as e:
        print(f"Error reading PDF: {e}")

def downloadDOIPDF(pdf_url, doi_path):
    # check if file exist before downloading.
    dir = 'pdfs'
    # name of the file we want to save
    save_file = doi_path.split('/')[0]
    if check_file_existence(dir, save_file + ".pdf"):
        print("Download is done already... Going to the next inline.")
        return processPDFDoc("", save_file)
    else:
        # Send an HTTP GET request to download the PDF
        response = requests.get(pdf_url)
        create_directory_if_not_exists(dir)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            return processPDFDoc(response.content, save_file)
        else:
            return f'Failed to download the PDF from the URL. Status code: {response.status_code}'

def check_resource_type(url):
    try:
        # Send an HTTP HEAD request to the URL to retrieve headers
        response = requests.head(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Get the Content-Type header
            content_type = response.headers.get('Content-Type', '').lower()

            # Check the content type
            if 'text' in content_type:
                return "Text"
            elif 'html' in content_type:
                return "HTML"
            elif 'image' in content_type:
                return "Image"
            else:
                return "Unknown"

        else:
            return "Failed to fetch the resource. Status code: {}".format(response.status_code)

    except Exception as e:
        return "Error: {}".format(str(e))

def process_new_sheet(df):
    # Specify the columns you want to process
    """
    columns_to_process = [
        'itemInfo_dbCollection',
        'itemInfo_history_dateCreated',
        'itemInfo_history_dateLoaded',
        'head_citationTitle_titleText',
        'head_abstracts_abstracts',
        'head_source_sourceTitle',
        'head_source_publicationYear',
        'head_source_publicationDate',
        'head_enhancement_descriptors',
        'head_authorList_authors',
        'itemInfo_itemIdList_pii',
        'full_text_URL', 
        'full_text_content_type',
        'head_citationInfo',
        'head_correspondence',
        'systematic_review',
        'population',
        'number_of_studies',
        'ethnicities_and_races',
        'open_access',
        'topic',
        'authors','classification',
        'doi','year','publication_type',
        'title','abstract','clinicaltrials',
        'registry_of_trials','countries', 'PL ', 
        'links','journal','excluded',
        'reason_for_discrepancy','matches_criteria',
        'relations_status','study_design',
        'broad_synthesis_design','highlighted_abstract',
        'highlighted_title','keywords','threads','screening_info'

    ]
    """

    new_data = pd.DataFrame()
    for column_ in df.columns:
        column = column_.strip()
        # if column in columns_to_process:
        if column == 'itemInfo_dbCollection':
            new_data["DBCOllection"] = df[column].apply(lambda x: convert_content_to_comma_separated_string(x, "content"))
        if column == 'head_correspondence' or column == 'PL' or column == 'countries':
            new_data["country"] = df[column_].apply(extract_country)
            new_data["region"] = new_data["country"].apply(get_region_by_country_name)
        if column == 'head_citationTitle_titleText' or column == 'TI' or column == "title":
            new_data["title"] = df[column_].apply(extract_ttltext)
        if column == 'head_abstracts_abstracts' or column == 'AB' or column == "abstract":
            new_data["abstract"] = df[column_].apply(extract_paras)
        if column == 'head_source_sourceTitle' or column == 'JT' or column == "journal":
            new_data["journal"] = df[column_].apply(extract_source)
        if column == 'head_source_publicationYear' or column == "year":
            new_data["year"] = df[column_]
        if column == 'head_source_publicationDate' or column == 'DP':
            new_data["publication_date_obj"] = df[column_]
        if column == 'head_enhancement_descriptors':
            data = df[column_].apply(process_domains)
            domain_df = pd.DataFrame(data.tolist(), columns=['domain', 'other_domain'])
            new_data = pd.concat([new_data, domain_df], axis=1)
        if column == 'head_authorList_authors' or column == 'authors' or column == 'AU':
            new_data['authors'] = df[column_]
        if column == 'itemInfo_itemIdList_pii' or column == 'id' or column == 'IS':
            new_data['PII'] = df[column_]
        if column == 'full_text_URL':
            new_data['URL'] = df[column_]
        if column == 'full_text_content_type':
            new_data['document_type'] = df[column_]
        if column == 'head_citationInfo':
            citation_data = df[column_].apply(extract_citations)
            citation_df = pd.DataFrame(citation_data.tolist(), columns=['citation_type', 'citation_lang', 'citation_keywords'])
            new_data = pd.concat([new_data, citation_df], axis=1)
        else:
            new_data[column] = df[column_]
    
    pattern_dict = pattern_dict_regex
    keyword_data = df['full_text_URL'].apply(lambda x:search_and_extract_html(x, pattern_dict))

    derived_df = pd.DataFrame(keyword_data.tolist(), columns=ourColumns())
    new_data = pd.concat([new_data, derived_df], axis=1)
    return new_data



def replace_with_acronyms(input_strings, acronyms_dict):
    input_strings = input_strings.split(",")
    input_strings = [s.strip() for s in input_strings]
    """
    Replace the content of strings in input_strings with acronyms using acronyms_dict.
    
    Args:
        input_strings (list): List of strings to be replaced.
        acronyms_dict (dict): Dictionary mapping original content to acronyms.
    
    Returns:
        list: List of strings with content replaced by acronyms.
    """
    return [acronyms_dict.get(s, s) for s in input_strings]

def format_string_caps(input_string):
    # Replace underscores with spaces
    formatted_string = input_string.replace('_', ' ')
    
    # Capitalize the first character
    formatted_string = formatted_string.capitalize()

    return formatted_string

def tableHeader(header_list:list = []):
    list_of_objects = [
        {'id': i, 'text': format_string_caps(item), 'value': item, "sortable": True} for i, item in enumerate(header_list, start=1)
    ]

    return list_of_objects

def getUniqueCommaSeperatedColumnValues(df, column_name):
    df = df.copy().fillna("N/A")
    filtered_list = []
    if(column_name in df.columns):
        # Split the comma-separated values and stack them into separate rows
        df[column_name] = df[column_name].astype(str)
        df_column = df[column_name].str.split(', ', expand=True).stack()
        # Get unique values
        unique_values = df_column.unique()
        items = unique_values.tolist()
        remove_from_item = "No connection adapters were found for 'Failed to resolv"
        filtered_list = [item for item in items if (remove_from_item not in item and "N/A" not in item)]

    return sorted(filtered_list)


# Function to clean up the JSON data
def clean_json(json_str):
    # Replace NaN with None
    json_str = json_str.replace("NaN", "null")
    
    # Replace single quotes with double quotes around keys and values
    json_str = json_str.replace("'", "\"")

    return json_str


def convert_xml_to_json(xml_string):
    # Parse the XML string to a Python dictionary
    xml_dict = xmltodict.parse(xml_string)

    # Convert the dictionary to JSON format
    json_data = json.dumps(xml_dict, indent=2)

    return json_data


def xml_to_dict(self, element):
    # Recursive function to convert XML element to a Python dictionary
    result = {}
    for child in element:
        child_data = xml_to_dict(child)
        if child_data:
            result[child.tag] = child_data
        else:
            result[child.tag] = child.text
    return result



def ageRangeSearchAlgorithm(matches) -> list(list()):
    numerical_values_and_operators = []
    for match in matches:
        match_values = re.findall(r'(less than|greater than|<|>|\d+)(?:-(\d+))? (\w+)', match.group(), flags=re.IGNORECASE)
        if match_values:
            if match_values[0][0].isdigit():
                start = int(match_values[0][0])
                end = int(match_values[0][1]) if match_values[0][1] else None
            elif match_values[0][0].lower() in ['less than', 'greater than']:
                start = int(match_values[0][1]) if (match_values[0][1] and match_values[0][1].isdigit()) else None
                end = int(match_values[0][2]) if (match_values[0][2] and match_values[0][2].isdigit()) else None
            else:
                # Handle other cases where the format doesn't match expectations
                continue
            operator = "="
            if(match_values[0][0].lower() in ['greater than', '>']):
                operator = ">"
            elif(match_values[0][0].lower() in ['less than', '<']):
                operator = "<"
            # set None value based on operators
            if(operator == "<" and start is None):
                start = 0
                end = end # not substract 1 becos that will be done in is_within_range function
            elif(operator == ">" and start is None):
                start = end + 1
                end = 1000000 #imaginary unlimited age
            elif(operator == "=" and end is None):
                end = start
            numerical_values_and_operators.append([start, end, operator])
            
    return numerical_values_and_operators


def find_overlapping_groups(check_range, list_of_ranges):
    """
    Find all groups (sublists) that overlap with the given range.
    
    Parameters:
    - check_range: The range to check [start, end].
    - list_of_ranges: A list of ranges where each sublist contains three items [start, end, operator].
    
    Returns:
    - A list of sublists that overlap with the given range.
    """
    overlapping_groups = []
    for sublist in list_of_ranges:
        start, end, operator = sublist[0], sublist[1], sublist[2]
        
        # Check if the ranges overlap
        if start < check_range[1] and end > check_range[0]:
            overlapping_groups.append(sublist)
        elif start >= check_range[0] and end <= check_range[1]:
            overlapping_groups.append(sublist)
    
    return overlapping_groups


def append_to_dict_value(my_dict, key, values):
    if key in my_dict:
        my_dict[key].extend(values)
    else:
        my_dict[key] = values
        
"""
    This function helps in preprocessing and generating new columns to fit in into our work.
"""  
def convert_dict(data, delimiter="#"):
    result = {}

    for key, value in data.items():
        if not value:
            continue

        # Split the key using the specified delimiter
        parts = key.split(delimiter)

        if len(parts) > 1:
            category = delimiter.join(parts[:-1])
            specific_item = parts[-1]

            if category not in result:
                result[category] = specific_item
            else:
                result[category] += f", {specific_item}"

        result[key] = value

    return result


def create_columns_from_text(document, searchRegEx):
    result_columns = {}
    for category, subcategories in searchRegEx.items():
        for subcategory, terms_dict in subcategories.items():
            for term_key, term_list in terms_dict.items():
                column_name = f"{category}#{subcategory}#{term_key}"
                result_columns[column_name] = None
                # result_columns[column_name] = None
                assigned_values = {}
                if category == "Population" and subcategory == "AgeGroup":
                    for age_range, age_keywords in terms_dict.items():
                        # Extract range from the key
                        age_range_values = list(map(int, re.findall(r'\d+', age_range)))
                        # Extract potential age ranges from the document
                        placeholder = r'\d{1,3}'
                        potential_age_ranges = re.finditer(
                            rf'\b(?:ages {placeholder} to {placeholder}|ages {placeholder}-{placeholder}|{placeholder} to {placeholder} years|{placeholder}to{placeholder} yrs|{placeholder}-{placeholder} yrs|{placeholder}-{placeholder} years|{placeholder} - {placeholder} years|{placeholder} - {placeholder} yrs|less than {placeholder} year|less than {placeholder} years|less than {placeholder} yrs|{placeholder} years|{placeholder} yrs|{placeholder} age)\b',
                            document, flags=re.IGNORECASE
                        )
                        
                        # Check if any potential age range overlaps with the specified age range
                        found_age_ranges = ageRangeSearchAlgorithm(potential_age_ranges)
                        overlapping_ranges = find_overlapping_groups(age_range_values, found_age_ranges)
                        
                        assigned_values[age_range] = overlapping_ranges
                    """
                        Search for other keywords such as Newborn and others
                    """
                    list_search_item = []
                    for term in term_list:
                        term_pattern = re.compile(fr'\b{term}\b', flags=re.IGNORECASE)
                        if term_pattern.search(document):
                            list_search_item.append(term_key)
                    """
                    this function helps to append other searched items aside from the age ranges
                    """
                    append_to_dict_value(assigned_values, term_key, list(set(list_search_item)))
                    
                    # Store the assigned age values for the current column
                    result_columns[column_name] = assigned_values[term_key]
                
                elif(category == "NoOfStudies" and subcategory == "number_of_studies"):
                    for term in term_list:
                        term = r'(?:\d+ ' + term + ')'
                        term_pattern = re.compile(fr'\b{term}\b', flags=re.IGNORECASE)
                        if term_pattern.search(document):
                            assigned_values[term] = term
                            # Store the assigned values for the current column
                    result_columns[column_name] = list(assigned_values.values())
                    
                else:
                    for term in term_list:
                        term_pattern = re.compile(fr'\b{term}\b', flags=re.IGNORECASE)
                        if term_pattern.search(document):
                            assigned_values[term] = term
                            # Store the assigned values for the current column
                    result_columns[column_name] = list(assigned_values.values())
    result_columns = convert_dict(result_columns)
    return result_columns


def convert_dict_to_dataframe(data_dict):
    # Create a DataFrame with a single row
    df = pd.DataFrame([data_dict])

    return df


def is_within_range(lower_limit, upper_limit, range_val:list, step=1):
    start, end, _ = tuple(range_val)
    if (lower_limit in range(start, end, step) or upper_limit in range(start, end, step)):
        resp = True
    else:
        resp = False
    
    print(start, end, lower_limit, upper_limit, resp)
    return resp

import requests
from urllib.parse import urlparse

def is_complete_url(url):
    parsed = urlparse(url)
    # A complete URL has a scheme (http, https) and netloc (domain)
    return bool(parsed.scheme and parsed.netloc)

def get_final_url(url):
    try:
        while True:
            # Make a GET request without following redirects
            response = requests.get(url, allow_redirects=False)
            # Check if the response status code indicates a redirect
            if response.status_code in (301, 302, 303, 307, 308):
                # Update the URL to the new location
                url = response.headers.get('Location')
            else:
                # No more redirects, return the final URL
                if is_complete_url(url):
                    return url
                return guess_host(url)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return guess_host(url)



def guess_host(relative_path):
    common_hosts = [
        "https://www.frontiersin.org",
        "https://journals.plos.org",
        "https://www.nature.com",
        "https://www.tandfonline.com",
        "https://elifesciences.org",
    ]
    
    # Iterate through possible hosts and attempt a match
    for host in common_hosts:
        test_url = host + relative_path
        try:
            # Check if the URL is valid and accessible
            response = requests.head(test_url, allow_redirects=True, timeout=5)
            if response.status_code == 200:
                return test_url
        except requests.RequestException:
            continue  # Skip to the next host

    return "Unable to determine the host. Please specify or verify the correct base URL."
  
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def extract_pdf_links(url, headless=True):
    """
    Extracts all PDF links from a given webpage using Selenium.

    Args:
        url (str): The URL of the webpage to scan for PDF links.
        headless (bool): Whether to run the browser in headless mode. Defaults to True.

    Returns:
        list: A list of found PDF URLs. Returns an empty list if no PDF links are found.
    """
    # Set up Selenium WebDriver options
    options = Options()
    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Navigate to the target URL
        driver.get(url)

        # Find all anchor tags
        links = driver.find_elements(By.TAG_NAME, "a")

        # Filter and collect PDF links
        pdf_links = [
            link.get_attribute("href")
            for link in links
            if link.get_attribute("href") and ".pdf" in link.get_attribute("href").lower()
        ]

        # Return the list of PDF links
        return pdf_links

    except Exception as e:
        print(f"Error while extracting PDF links: {e}")
        return []

    finally:
        # Close the browser
        driver.quit()



from bs4 import BeautifulSoup

def html_to_plain_text_selenium(url, headless=True):
    print(url)
    """
    Fetches a webpage using Selenium, extracts its HTML content, converts it to plain text, 
    and saves it to a text file.

    Args:
        url (str): The URL of the webpage to fetch.
        output_file (str): Path to the output text file to save the plain text content.
        headless (bool): Whether to run the browser in headless mode.

    Returns:
        None
    """
    # Set up Selenium options
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36")

    # Initialize WebDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Navigate to the webpage
        driver.get(url)

        # Fetch the full page HTML content
        page_html = driver.page_source

        # Use BeautifulSoup to parse the HTML and extract plain text
        soup = BeautifulSoup(page_html, "html.parser")
        plain_text = soup.get_text(separator="\n", strip=True)

        return plain_text

    except Exception as e:
        print(f"Error occurred while processing the page: {e}")

    finally:
        # Close the browser
        driver.quit()
        
def get_final_redirected_url(url, headless=True):
    """
    Gets the final redirected URL, including handling dynamic JavaScript or meta-refresh-based redirections, using Selenium.

    Parameters:
        url (str): The initial URL to start tracking.
        headless (bool): Whether to run the browser in headless mode. Defaults to True.

    Returns:
        str: The final redirected URL.

    Raises:
        Exception: If there is an error setting up Selenium or navigating the URL.
    """
    # Configure Selenium WebDriver
    options = Options()
    if headless:
        options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    # Use ChromeDriverManager to handle the WebDriver setup
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Navigate to the initial URL
        driver.get(url)

        # Return the final URL
        return driver.current_url
    except Exception as e:
        raise Exception(f"Error getting final URL: {e}")
    finally:
        # Ensure the browser is closed
        driver.quit()
