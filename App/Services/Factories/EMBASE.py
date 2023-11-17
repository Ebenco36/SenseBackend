import json
import requests
from retry import retry
from App.Services.Service import Service
from App.Request.ApiRequest import ApiRequest
from App.Utils.Helpers import create_directory_if_not_exists, save_json_to_csv, append_json_response_to_file, \
    json_to_dataframe_and_save, get_remainder_and_quotient, convert_json_to_List_of_dict

class EMBASE(Service):

    def __init__(self, pageSize = 200):
        self.execute_search2_data = None
        self.execute_applied_data = None
        self.execute_session_history_data = None
        self.execute_data = None # save retrieve records
        self.execute_search2_details_data = None
        self.execute_cited_by_data = None
        self.pageSize = pageSize
    
    def authenticate(self, headers):
        self.auth_headers = headers
        return self

    def fetch(self, headers:dict = {}, form_data:dict = {}):
        self.authenticate(headers).executeApply().executeSearchDetails().executeRetrieveData()

    """
        Process flow of retrieving records from EMBASE
        Stages are listed here.
        Auth: https://id.elsevier.com/as/authorization.oauth2?platSite=EM/embase&prompt=none&client_id=EMBASE-V1&redirect_uri=https://www.embase.com/id-plus-callback&response_type=code&scope=openid%20email%20profile%20els_auth_info%20urn:com:elsevier:idp:policy:product:indv_identity%20urn:com:elsevier:idp:policy:product:inst_assoc%20els_analytics_info&state=Hq3wI6
        Validate Query: https://www.embase.com/rest/searchresults/validateQuery
        1. Create prompt by accessing executeSearch2: https://www.embase.com/rest/spring/searchresults2/executeSearch2 [POST]
        2. Apply search by using this endpoint https://www.embase.com/facet/1/filter/applied [GET]
        3. Check Session History using the URL https://www.embase.com/rest/searchresults/sessionhistory [GET]
        4. Now to retrieve details, we need to chunk record. Steps are listed as follows:
            A. offset: 0, size: 10; https://www.embase.com/rest/searchresults/results?offset=0&size=10
            B. offset: 0, size: 10; https://www.embase.com/rest/searchresults/results?offset=10&size=10
            C. offset: 0, size: 10; https://www.embase.com/rest/searchresults/results?offset=20&size=5
        5. Retrieve search details using https://www.embase.com/rest/searchresults/details [GET]
        6. Get cited information using URL https://www.embase.com/rest/search/citedBy/items/L2024779469,L2024793833,L2024793842,L2025237334,L2025379467,L2025420903,L2026328836,L2026534380,L640379448,L641105167,L641199236,L641381626,L641479426,L641571848,L641924167,L641993429,L642006687,L642042055,L2018895705,L2021938550,L2021938552,L2016020459,L2018863277,L2018864577,L2018864578  [GET]
        7. Get next if there more records using https://www.embase.com/rest/spring/searchresults2/executeSearch2?page=2&pageSize=25&orderby=date&viewsearch=1
        8. Repeat 1 to 6 to get records for the next page.
    """

    """
        These are not necessary, But you might want to look into it.
    """
    def validateQuery(self, form_data = None):
        if not form_data:
            form_data = {
                "query": "#5 AND #17 AND [1-1-2018]/sd NOT [1-7-2019]/sd",
                "searchId": 19
            }
        url = "https://www.embase.com/rest/searchresults/validateQuery"
        conn = ApiRequest('json', url, headers=self.auth_headers)
        self.execute_search2_data = conn.send_data(form_data)
        print("validateQuery")
        print(self.execute_search2_data)
        return self
    
    """
        These are not necessary, But you might want to look into it.
    """
    def updateQuery(self, form_data = None):
        if not form_data:
            form_data = {
                "query": "#5 AND #17 AND [1-1-2018]/sd NOT [1-7-2019]/sd",
                "searchId": 19
            }
        url = "https://www.embase.com/rest/searchresults/updateQuery"
        conn = ApiRequest('json', url, headers=self.auth_headers)
        self.execute_search2_data = conn.send_data(form_data)
        print("updateQuery")
        print(self.execute_search2_data)
        return self
    
    """
        Run/Execute search using the executeSearch2 url from EMBASE.
    """
    def executeSearch2(self, page = 1, pageSize = 10, date = "date"):
        url = "https://www.embase.com/rest/spring/searchresults2/executeSearch2"
        url += "?page=" + str(page) + "&pageSize=" + str(pageSize) + "&orderby" + str(date) + "&viewsearch=19"
        conn = ApiRequest('json', url, headers=self.auth_headers)
        self.execute_search2_data = conn.fetch_records()
        return self
    
    """
        Apply changes to search as specified on EMBASE server.
    """
    def executeApply(self):
        url = 'https://www.embase.com/facet/1/filter/applied'
        filted_data = ApiRequest('json', url, headers=self.auth_headers)
        req = filted_data.fetch_records()
        if (req.get('status') and req.get('status') is True):
            self.execute_applied_data = filted_data.fetch_records()
        else:
            self.execute_applied_data = None
        print("Apply")
        print(self.execute_applied_data)
        return self

    
    """
        Get session history to know if we ae still inline
    """
    def executeSessionHistory(self):
        url = 'https://www.embase.com/rest/searchresults/sessionhistory'
        session_data = ApiRequest('json', url, headers=self.auth_headers)
        req = session_data.fetch_records()
        if (req.get('status') and req.get('status') is True):
            self.execute_session_history_data = session_data.fetch_records()
        else:
            self.execute_session_history_data = None
        print("session")
        print(self.execute_session_history_data)
        return self

    """
        Refresh each time a query is made to get records
    """
    def executeSearchDetails(self):
        url = 'https://www.embase.com/rest/searchresults/details'
        record_details_data = ApiRequest('json', url, headers=self.auth_headers)
        req = record_details_data.fetch_records()
        if (req.get('status') and req.get('status') is True):
            self.execute_search2_details_data = req.get("data").get('searchDetails')
        else:
            self.execute_search2_details_data = None

        return self
    
    """
        We are following the structure as described on EMBASE Server.
    """
    def executeRetrieveData(self):
        create_directory_if_not_exists("EMBASE/")
        # get record page by page for example 200 per page
        if (self.execute_search2_details_data):
            details = self.execute_search2_details_data
            hits = int(details.get("hits"))
            page_size = int(self.pageSize)
            if (hits > 0):
                responseList = []
                remainder, quotient, round_up_value = get_remainder_and_quotient(hits, page_size)
                for page in range(1, round_up_value + 1):
                    self.executeSearch2(page=page, pageSize=page_size)
                    self.executeSearchDetails()
                    """
                        For each page, we want to get 10 records
                    """
                    size = 10
                    print("Currently on page: " + str(page))
                    # Doing this to avoid problem on the last page seeding...
                    list_item_number = page_size
                    if(page == round_up_value):
                        if(remainder == page_size):
                            list_item_number = page_size
                        else:
                            list_item_number = remainder
                    else:
                        list_item_number = page_size
                    
                    for offset in range(0, list_item_number, size):
                        rec = self.retrieveRecord(offset=offset, size=size)
                        # pageList.append(rec.get("bibrecords")[0])
                        append_json_response_to_file(rec, "EMBASE/EMBASEexportPage_" + str(page) + "__" + str(offset) + ".json")
                convert_json_to_List_of_dict()

    # @retry(exceptions=requests.exceptions.ProxyError, tries=3, delay=2, backoff=2)                
    def retrieveRecord(self, offset = 0, size = 10):
        url = 'https://www.embase.com/rest/searchresults/results?offset=' + str(offset) + '&size=' + str(size)
        print(url)
        record_details_data = ApiRequest('json', url, headers=self.auth_headers)
        data = record_details_data.fetch_records()
        if (data.get('status') and data.get('status') is True):
            rec = record_details_data.fetch_records()
            data = rec.get('data')
        else:
            print(data)
            data = None
        return data


    def convert_to_csv(self, data_path, filename):
        with open(data_path, 'r') as json_file:
            json_data = json.load(json_file)
        json_to_dataframe_and_save(json_data, filename)
    