import requests
import json
import glob
import os
import re
import pandas as pd
from src.Request.ApiRequest import ApiRequest
from urllib.parse import urljoin
from src.Request.Headers import headers
from src.Services import Service
from src.Utils.Helpers import (
    format_text_to_json,
)
from src.Services.Factories.ServiceFactory import ServiceFactory


def scraping():
    import requests
    from bs4 import BeautifulSoup

    # URL of the webpage you want to scrape
    # url = 'https://www.embase.com/rest/spring/searchresults2/executeSearch2'  # Replace with the actual login URL
    url = 'https://bmcmedgenomics.biomedcentral.com/articles/10.1186/s12920-022-01426-2'
 
    header = """
        Host: www.embase.com
        User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0
        Accept: application/json, text/javascript, */*; q=0.01
        Accept-Language: en-US,en;q=0.5
        Accept-Encoding: gzip, deflate, br
        Referer: https://www.embase.com/
        X-NewRelic-ID: VgYOVFRVChABUVZQAgUEUlAF
        newrelic: eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjIwOTUyNjIiLCJhcCI6IjE1ODg2ODc2MjUiLCJpZCI6ImViYWJiNjk1M2M5MWZjYWEiLCJ0ciI6ImY3ODYyYzYwZmMyNDc3ZmFlYzQ2MzlkYjNjMTI3YTAwIiwidGkiOjE2OTM0ODU0MTg2MDAsInRrIjoiMjAzODE3NSJ9fQ==
        traceparent: 00-f7862c60fc2477faec4639db3c127a00-ebabb6953c91fcaa-01
        tracestate: 2038175@nr=0-1-2095262-1588687625-ebabb6953c91fcaa----1693485418600
        Content-Type: application/json; charset=utf-8
        X-Requested-With: XMLHttpRequest
        Content-Length: 354
        Origin: https://www.embase.com
        DNT: 1
        Connection: keep-alive
        Cookie: AMCV_4D6368F454EC41940A4C98A6%40AdobeOrg=-2121179033%7CMCIDTS%7C19600%7CMCMID%7C01214066067756635329220484246223413350%7CMCAID%7CNONE%7CMCOPTOUT-1693492611s%7CNONE%7CvVersion%7C5.3.0; s_pers=%20v8%3D1693485411091%7C1788093411091%3B%20v8_s%3DLess%2520than%25201%2520day%7C1693487211091%3B%20c19%3Dem%253Aresults%253Aresults%253Aother%253A3.1%2520results%7C1693487211094%3B%20v68%3D1693478899701%7C1693487211103%3B; AWSALB=jBM0e9p6XON6iNJOHj0yQLj0+6OigqSpzRjNDN3maqfblbmsJSKNP+xY2VkwgUo2cYQHkQjn9xvC2fmel0KpAFAlLA3NZ4+wBYPopYNoeZEErDE7FfDFa6u349bE; AWSALBCORS=jBM0e9p6XON6iNJOHj0yQLj0+6OigqSpzRjNDN3maqfblbmsJSKNP+xY2VkwgUo2cYQHkQjn9xvC2fmel0KpAFAlLA3NZ4+wBYPopYNoeZEErDE7FfDFa6u349bE; search_maptoemtree=; search_majorfocus=; search_narrowterms=; search_extensive=; search_map_explosion_extensive=; search_since=; search_to=; initialSearchValue=; JSESSIONID=32FEE9653C0807F30608843AA0E00F4E; EMBASE_TRACKING_ID=1ff718bb-c9cd-4e0a-99d7-8b5fe4a9f9e3; SESSION=F90EFDA08DE966A55A46660005EBD1B9; at_check=true; AMCVS_4D6368F454EC41940A4C98A6%40AdobeOrg=1; s_sess=%20s_cpc%3D0%3B%20s_cc%3Dtrue%3B%20s_sq%3D%3B%20c21%3De3e662d2-e1b8-10fa-bcb4-0a088524d7ba%3B%20e13%3D%253A%3B%20e41%3D1%3B%20s_ppvl%3Dem%25253Aresults%25253Aresults%25253Aother%25253A3.4%252520search%252520submit%252C1%252C1%252C271%252C1438%252C271%252C1512%252C945%252C1%252CP%3B%20s_ppv%3Dem%25253Aresults%25253Aresults%25253Aother%25253A3.1%252520results%252C19%252C19%252C271%252C1438%252C271%252C1512%252C945%252C1%252CP%3B; historyExpanded=null; pageSize=200; savedSearchesIDS=37; __cf_bm=LfZcD3vAPLqwfS8axRWdT_q3vedITOs_yCFhBmRbiG4-1693484698-0-AVEZVdLRAsSNxsHZs+Dv1a5uc2Exu8D4n34KA6m7eI7iInq6Uwi6DetU/fe8Zg07D8DeOB4poAKXoihyhNeWtwE=; mbox=session#24d3ba6a9274461f97133ebbd26dd44d#1693487100
        Sec-Fetch-Dest: empty
        Sec-Fetch-Mode: cors
        Sec-Fetch-Site: same-origin
        TE: trailers
    """
    headers = format_text_to_json(header)

    # Send an HTTP POST request with the data
    # response = requests.post(url, data=post_data, headers=headers)
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the page content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Process the content as needed
        # For example, you can find elements, extract data, etc.
        
        # Print the extracted information
        print(soup.prettify())  # Print the entire HTML content
    else:
        print(response.text)
        print("Failed to retrieve the webpage")


def embase_access():
    head = """
            Host: www.embase.com
            User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0
            Accept: application/json, text/javascript, */*; q=0.01
            Accept-Language: en-US,en;q=0.5
            Accept-Encoding: gzip, deflate, br
            Referer: https://www.embase.com/
            X-NewRelic-ID: VgYOVFRVChABUVZQAgUEUlAF
            newrelic: eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjIwOTUyNjIiLCJhcCI6IjE1ODg2ODc2MjUiLCJpZCI6ImE1MTcwYTg5YWRlMzQwZjgiLCJ0ciI6IjYyNjg3MjlkODNjNmU3MjFlNzI0ZmJkMDEyNmQyYzAwIiwidGkiOjE2OTM3NzA4Mjk3NzIsInRrIjoiMjAzODE3NSJ9fQ==
            traceparent: 00-6268729d83c6e721e724fbd0126d2c00-a5170a89ade340f8-01
            tracestate: 2038175@nr=0-1-2095262-1588687625-a5170a89ade340f8----1693770829772
            X-Requested-With: XMLHttpRequest
            DNT: 1
            Connection: keep-alive
            Cookie: AMCV_4D6368F454EC41940A4C98A6%40AdobeOrg=-2121179033%7CMCIDTS%7C19604%7CMCMID%7C01214066067756635329220484246223413350%7CMCAID%7CNONE%7CMCOPTOUT-1693778028s%7CNONE%7CvVersion%7C5.3.0; s_pers=%20v8%3D1693770828063%7C1788378828063%3B%20v8_s%3DLess%2520than%25201%2520day%7C1693772628063%3B%20c19%3Dem%253Aresults%253Aresults%253Aother%253A3.1%2520results%7C1693772628075%3B%20v68%3D1693767340289%7C1693772628081%3B; AWSALB=bPS9q0l0Zqp4pfCSePaqjFijrkOyQFPoDhdhj2IYyEmtNW4h+gGGrsD+uHXArbWSAxgsQbkV3iIO3oOcNqR18a3DKumwfbQSX9G/5HNL0XEI+k7mfhyt++nRDgL2; AWSALBCORS=bPS9q0l0Zqp4pfCSePaqjFijrkOyQFPoDhdhj2IYyEmtNW4h+gGGrsD+uHXArbWSAxgsQbkV3iIO3oOcNqR18a3DKumwfbQSX9G/5HNL0XEI+k7mfhyt++nRDgL2; search_maptoemtree=; search_majorfocus=; search_narrowterms=; search_extensive=; search_map_explosion_extensive=; search_since=; search_to=; pageSize=200; JSESSIONID=3BF72F3BA39D778242AA373CF51723C7; EMBASE_TRACKING_ID=720d0d98-60e8-4666-94ad-c632b0d1149c; SESSION=C0E873C9F55312BE910C73C7C5D62862; at_check=true; AMCVS_4D6368F454EC41940A4C98A6%40AdobeOrg=1; s_sess=%20s_cpc%3D0%3B%20s_sq%3D%3B%20s_cc%3Dtrue%3B%20c21%3D49cb4321-1439-11c3-b8fa-836b04d2d0f1%3B%20e13%3D%253A%3B%20e41%3D1%3B%20s_ppvl%3Dem%25253Aresults%25253Aresults%25253Aother%25253A3.4%252520search%252520submit%252C2%252C2%252C389%252C1438%252C389%252C1512%252C945%252C1%252CP%3B%20s_ppv%3Dem%25253Aresults%25253Aresults%25253Aother%25253A3.1%252520results%252C28%252C28%252C389%252C1438%252C389%252C1512%252C945%252C1%252CP%3B; initialSearchValue=; historyExpanded=null; __cf_bm=Jb2_f7QpiGyLw4Emh25xWUtV6.AawGY09HLX7YShNRY-1693770811-0-AQ0yC/ewD53K93Ori45ZNFqM+a8/VfM8z/m/MBJBaz6yLaqBYAbp/uxzhfqN34HLlpXmOrJUEBLWTLn9CRE7Nck=
            Sec-Fetch-Dest: empty
            Sec-Fetch-Mode: cors
            Sec-Fetch-Site: same-origin
            TE: trailers
        """
    headers = format_text_to_json(head)
    ServiceFactory.create_service("embase").fetch(headers)
    
    
def ilove_access():
    head = """
            Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8
            Accept-Encoding: gzip, deflate, br
            Accept-Language: en-US,en;q=0.5
            Connection: keep-alive
            DNT: 1
            Host: api.iloveevidence.com
            Sec-Fetch-Dest: document
            Sec-Fetch-Mode: navigate
            Sec-Fetch-Site: none
            Sec-Fetch-User: ?1
            Upgrade-Insecure-Requests: 1
            User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0
        """
    headers = format_text_to_json(head)
    ServiceFactory.create_service("L.ove").fetch(headers)
        
        
def medline_access():
    head = """
            Host: www.embase.com
            User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0
            Accept: application/json, text/javascript, */*; q=0.01
            Accept-Language: en-US,en;q=0.5
            Accept-Encoding: gzip, deflate, br
        """
    headers = format_text_to_json(head)
    ServiceFactory.create_service("medline").fetch()
    

def cochrane_access():
    head = """
        authority:www.cochranelibrary.com
        method:POST
        path:/en/advanced-search/search-manager?p_p_id=scolarissearchresultsportlet_WAR_scolarissearchresults&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=get-query-result-counts&p_p_cacheability=cacheLevelPage&p_p_col_id=column-1&p_p_col_count=2
        scheme:https
        Accept:application/json, text/javascript, */*; q=0.01
        Accept-Encoding:gzip, deflate, br
        Accept-Language:en-GB,en-US;q=0.9,en;q=0.8
        Content-Length:2914
        Content-Type:application/x-www-form-urlencoded; charset=UTF-8
        Cookie:GUEST_LANGUAGE_ID=en_US; COOKIE_SUPPORT=true; osano_consentmanager_uuid=8c1fd0b4-1c3d-4eb9-8169-e0496269349f; osano_consentmanager=urgXNfqlfpUPrarNpGmUZ782396AHPD7PoA6HqYnfEf-svBX_Hpb20R3cZ_IimbCt8q-SmfrssvOaCA0czyhBXxHzrBQ5mSKGQ6wBLzBgYs-0kDT5gOcY5MBTl4BGmz8yewzlw8o9zIKsgPAX0FsQhdWExL0g4BYcdWe8og-1-9mNaR3xe5liyJjR0gitAs1uT_I2JMNT2Xftut7U64VNw02Ah2zM1l4-FLQLfFUC7ud5u39IOmeMpz1r8qZkpqNmZbAnqwQi9owkwnUJaPTvrOc9Ik=; JSESSIONID=cspbwgreclprt160x1~2886A8EF9BA61EA55A95C5896426D4E1; SID_REP=DF77514EC68E969CD6D7205BF27FD66C; SCOLAUTHSESSIONID=E9A52B2370B0E15BB32A22D9B128199A; cf_clearance=YiLdyQ.Zouty8fSp5nMMK4uMfThFleTjo2wTGyLuH2c-1704111394-0-2-c67a6992.ab6f4418.917b3672-0.2.1704111394; _ga_BRLBHKT9XF=GS1.1.1704111426.1.0.1704111426.0.0.0; LFR_SESSION_STATE_20159=1704111426856; __cf_bm=8x_gs4iSOcea7JGsfUJCgvYVqwGGyb0q5Rd6VCwrqOY-1704112322-1-AbQ7MKQvsmh0qe3rrULgi3JefSNpHQqZDXKo5+lepzEX7gRkUvV3UQ1W5SyaqHLwAG6T7ArdmDa0DV8G9nK63lk=; SCOL_SESSION_TIMEOUT=1740 Mon, 01 Jan 2024 13:06:07 GMT
        Origin:https://www.cochranelibrary.com
        Referer:https://www.cochranelibrary.com/advanced-search/search-manager
        Sec-Ch-Ua:"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"
        Sec-Ch-Ua-Mobile:?0
        Sec-Ch-Ua-Platform:"macOS"
        Sec-Fetch-Dest:empty
        Sec-Fetch-Mode:cors
        Sec-Fetch-Site:same-origin
        User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36
        X-Requested-With: XMLHttpRequest
    """
    headers = format_text_to_json(head)
    print(headers)
    ServiceFactory.create_service("cochrane").fetch(headers)