import requests
import cloudscraper
from bs4 import BeautifulSoup

def htmlToText(html_url):
    # Send an HTTP GET request to fetch the HTML content
    response = requests.get(html_url)
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

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    "Connection":"keep-alive",
    "DNT":"1",
    "Origin":"https://www.tandfonline.com",
    "Sec-Fetch-Mode":"cors",
    "Sec-Fetch-Site":"cross-site",
}

url = 'https://www.tandfonline.com/doi/full/10.1080/07448481.2019.1594826'
scraper = cloudscraper.create_scraper()
response = scraper.get(url, headers=headers)
print(response.text)
print(response.status_code)
# print(htmlToText(response.text))
# Continue processing the response