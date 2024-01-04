import re
import requests
from src.Services.Api.ApiService import ApiService
import xml.etree.ElementTree as ET
from src.Utils.Helpers import xml_to_dict

class XmlApiService(ApiService):
    def __init__(self, api_url, headers=None):
        self.api_url = api_url
        self.headers = headers

    def get_records(self):
        
        try:
            # Make a GET request to the XML URL
            response = requests.get(self.api_url, headers=self.headers)
            response.raise_for_status()  # Raise an HTTPError for bad responses

            # Parse the XML content
            root = ET.fromstring(response.content)

            # Convert XML to a Python dictionary (optional)
            xml_data = self.xml_to_dict(root)
        

            return xml_data
        except requests.exceptions.RequestException as e:
            return {'error': f'Request error: {e}'}, 500
        except ET.ParseError as e:
            return {'error': f'XML parsing error: {e}'}, 500
    
        
    
    def post_data(self, data):
        response = requests.post(self.api_url, data=data)
        if response.status_code == 200:
            return response.text
        else:
            return ""
    
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