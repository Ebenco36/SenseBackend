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
from PyPDF2 import PdfReader, PdfFileReader, errors
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
        return None

# Define a function to extract 'paras' field
def extract_paras(row):
    try:
        row = ast.literal_eval(row)
        return row[0]['paras'][0].replace('@hit_start', '').replace('@hit_end', '')
    except Exception as e:
        return None

def extract_source(row):
    try:
        row = ast.literal_eval(row)
        return row[0]
    except Exception as e:
        return None

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
        return None

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
        pdf_reader = PdfFileReader(pdf_file)

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
        pdf_reader = PdfFileReader(pdf_file)

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
        'registry_of_trials','countries',
        'links','journal','excluded',
        'reason_for_discrepancy','matches_criteria',
        'relations_status','study_design',
        'broad_synthesis_design','highlighted_abstract',
        'highlighted_title','keywords','threads','screening_info'

    ]

    new_data = pd.DataFrame()
    for column in df.columns:
        if column in columns_to_process:
            if column == 'itemInfo_dbCollection':
                new_data["DBCOllection"] = df[column].apply(lambda x: convert_content_to_comma_separated_string(x, "content"))
            if column == 'head_correspondence':
                new_data["country"] = df[column].apply(extract_country)
                new_data["region"] = new_data["country"].apply(get_region_by_country_name)
            if column == 'head_citationTitle_titleText':
                new_data["title"] = df[column].apply(extract_ttltext)
            if column == 'head_abstracts_abstracts':
                new_data["abstract"] = df[column].apply(extract_paras)
            if column == 'head_source_sourceTitle':
                new_data["journal_source"] = df[column].apply(extract_source)
            if column == 'head_source_publicationYear':
                new_data["year"] = df[column]
            if column == 'head_source_publicationDate':
                new_data["publication_date_obj"] = df[column]
            if column == 'head_enhancement_descriptors':
                data = df[column].apply(process_domains)
                domain_df = pd.DataFrame(data.tolist(), columns=['domain', 'other_domain'])
                new_data = pd.concat([new_data, domain_df], axis=1)
            if column == 'head_authorList_authors':
                new_data['authors'] = df[column]
            if column == 'itemInfo_itemIdList_pii':
                new_data['PII'] = df[column]
            if column == 'full_text_URL':
                new_data['URL'] = df[column]
            if column == 'full_text_content_type':
                new_data['document_type'] = df[column]
            if column == 'head_citationInfo':
                citation_data = df[column].apply(extract_citations)
                citation_df = pd.DataFrame(citation_data.tolist(), columns=['citation_type', 'citation_lang', 'citation_keywords'])
                new_data = pd.concat([new_data, citation_df], axis=1)
            else:
                new_data[column] = df[column]
    
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