import requests
from serpapi import GoogleSearch
from bs4 import BeautifulSoup

def fetch_first_scholar_result(title):
    # Define your SerpApi API key
    SERPAPI_API_KEY = '0bf330f9baf5859f7c11d23ee091c313ef6c140707e96516ba1fb7352f52b7bd'
    # Define the parameters for SerpApi Google Scholar search
    params = {
        "api_key": SERPAPI_API_KEY,
        "engine": "google_scholar",
        "q": title, "hl": "en", "num": 1
    }
    
    search = GoogleSearch(params)
    results = search.get_dict()
    
    # Extract the DOI from the search results
    try:
        if results.get("organic_results"):
            first_result = results["organic_results"][0]
            print(results["organic_results"][0].get("link"))
            return first_result.get("link", "")  # Assuming the DOI or relevant URL is in 'link'
        else:
            return ''
    except Exception as e:
        print(f"Error fetching result for title '{title}': {e}")
        return ''