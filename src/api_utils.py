import requests
import json
import glob
import os
import re
import pandas as pd
from src.Request.ApiRequest import ApiRequest
from urllib.parse import urljoin
from src.Services import Service
from src.Utils.Helpers import (
    format_text_to_json,
)
from src.Services.Factories.ServiceFactory import ServiceFactory

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
            
def medline_class_access(searchText=[
    """
        (
        (review[pt] OR "review, tutorial"[pt] OR "review, academic"[pt])
        AND 
        (
            medline[tw] OR medlars[tw] OR embase[tw] OR pubmed[tw] OR cochrane[tw]
            OR scisearch[tw] OR psychinfo[tw] OR psycinfo[tw]
            OR psychlit[tw] OR psyclit[tw] 
            OR cinahl[tw] 
            OR ((hand[tw] AND search*[tw]) OR (manual*[tw] AND search*[tw]))
            OR ("electronic database*"[tw] OR "bibliographic database*"[tw] OR "computerized database*"[tw] OR "online database*"[tw])
            OR pooling[tw] OR pooled[tw] OR "mantel haenszel"[tw]
            OR peto[tw] OR dersimonian[tw] OR "der simonian"[tw] OR "fixed effect"[tw]
            OR "retraction of publication"[pt] OR "retracted publication"[pt]
        )
        )
        OR
        (
        meta-analysis[pt] 
        OR meta-analysis[sh] 
        OR (meta-analys*[tw] OR meta analys*[tw] OR metaanalys*[tw])
        OR (systematic*[tw] AND review*[tw])
        OR (quantitative*[tw] AND review*[tw])
        OR (methodologic*[tw] AND review*[tw])
        OR ("integrative research review"[tw] OR "research integration"[tw])
        )
        AND
        (
        immunization[mesh] 
        OR Immunization Programs[mesh] 
        OR vaccines[mesh]
        OR (immunisation[tiab] OR immunization[tiab] OR immunise[tiab] OR immunize[tiab] OR vaccine[tiab])
        )
        AND humans[filter]
        AND 
        ("2011"[edat] : "3000"[edat])
    """
    ]):
    ServiceFactory.create_service("medline_class").fetch(searchText)
    
def cochrane_access(searchText="COvid"):
    ServiceFactory.create_service("cochrane").fetch(searchText)
    
def ovid_new_access():
    headers = {}
    ServiceFactory.create_service("ovid_new").fetch()