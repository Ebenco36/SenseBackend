import json
import csv
import pandas as pd
from pandas import json_normalize

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
    directory_path = './'
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
    merge_path = 'EMBASECOMBINED.json'
    with open(merge_path, 'w') as output_file:
        json.dump(merged_data, output_file)

    # generate the CSV file for further preprocessing
    convert_list_of_dict_to_csv(merge_path)

    print("Combined record saved to path: " + str(merge_path))


def convert_list_of_dict_to_csv(filename):
    with open(filename, 'r') as infile:
        content = json.load(infile)
        json_to_dataframe_and_save(content, "EMBASECOMBINED.csv")