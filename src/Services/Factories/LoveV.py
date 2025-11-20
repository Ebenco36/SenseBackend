# import os
# import re
# import json
# import pandas as pd
# from glob import glob
# from src.Commands.DOIEnricher import DOIEnricher
# from src.Services.Service import Service
# from src.Request.ApiRequest import ApiRequest
# from src.Utils.Helpers import (
#     create_directory_if_not_exists,
#     check_file_existence, 
#     create_directory_if_not_exists,
#     getDOI,
#     process_data_valid, process_new_sheet
# )

# class LoveV(Service):

#     def __init__(self, pageSize = 500):
#         self.pageSize = pageSize


#     def authenticate(self, headers):
#         self.auth_headers = headers
#         return self

#     def fetch(self, headers):
#         return self.authenticate(headers).executeRetrieveData()

#     """
#         Process flow of retrieving records from L.ove
#         Stages are listed here.
#     """
    
                   
#     def retrieveRecord(self, page = 1):
#         # https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references?metadata_ids=5e7fce7e3d05156b5f5e032a,603b9fe03d05151f35cf13dc&classification_filter=systematic-review,primary-study&hide_excluded=true&page=1&sort_by=year&show_summary=true
#         # url = 'https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references?metadata_ids=5e7fce7e3d05156b5f5e032a,603b9fe03d05151f35cf13dc&classification_filter=systematic-review,primary-study&hide_excluded=true&page=' + str(page) + '&sort_by=year&show_summary=true'
#         url = "https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references"
#         # url = f"https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references?metadata_ids=5e7fce7e3d05156b5f5e032a,603b9fe03d05151f35cf13dc&classification_filter=systematic-review&hide_excluded=true&page={page}&size={self.pageSize}&sort_by=year&show_summary=true&year_from_filter=2011"
#         print(url)
#         """
#             Work on payload
#         """
#         # payload = {
#         #     "sort_by": "year",
#         #     "metadata_ids": [
#         #         "603b9fe03d05151f35cf13dc"
#         #     ],
#         #     "query": "",
#         #     "page": page,
#         #     "size": self.pageSize,
#         #     "classification_filter": "systematic-review",
#         #     "year_from_filter": 2011
#         # }
        
#         payload = {
#             "sort_by": "year",
#             "metadata_ids": [
#                 "603b9fe03d05151f35cf13dc"
#             ],
#             "query": "(review OR \"systematic review\" OR \"systematic overview\" OR \"quantitative review\"   OR \"quantitative overview\" OR \"quantitative synthesis\" OR \"methodologic review\"  OR \"integrative research review\" OR \"research integration\" OR meta-analysis   OR metaanalysis OR \"meta analysis\") AND (vaccine OR vaccination OR immunization OR immunisation OR immunize OR immunise   OR vaccinated OR immunized OR immunised OR immunizing OR immunising)",
#             "page": 1,
#             "size": 10,
#             "classification_filter": "primary-study,systematic-review,broad-synthesis",
#             "broad_synthesis_design_filter": "osr,guideline,pb,sr,fb,grade,not_grade",
#             "type_of_meta_analysis_filter": "lsr",
#             "study_design_filter": "rct,non-rct",
#             "publication_type_filter": "clinicaltrials,thesis,journal,book,report,conference,webpage",
#             "has_abstract": "all",
#             "year_from_filter": 2011
#         }
        
#         record_details_data = ApiRequest('json', url, headers=self.auth_headers)
#         rec = record_details_data.send_data(payload)
#         data = rec.get('data')
        
#         # record_details_data = ApiRequest('json', url, headers=self.auth_headers)
#         # rec = record_details_data.fetch_records() #.send_data(payload)
#         # data = rec.get('data')
#         return data
    
#     """
#         We are following the structure as described on EMBASE Server.
#     """

#     def executeRetrieveData(self, batch_size=10, save_interval=10, max_records=0):
#         # 10 is the number of record per page
#         max_records = self.pageSize * save_interval
#         # Save DataFrame to a CSV file
#         file_path = 'Data/L-OVE/Batches/'
#         file_path_processed = 'Data/L-OVE/Batches/Processed/'

#         create_directory_if_not_exists(file_path)

#         if(not check_file_existence(file_path, "batch_final.csv")):
#             # continue from where we stop to avoid starting all over again
#             result_max_num = get_max_batch_file(file_path)
#             page = 1

#             if (result_max_num and result_max_num > 0):
#                 # input and calculate continuation flow
#                 """
#                     if per page record = 10
#                     max_record is 2000 (save_interval * record per page)
#                     total record = max_records * batch_number
#                     page = ?
#                     Page = max_record / 10 == page number
#                 """
#                 page = int((max_records * result_max_num) / batch_size) + 1
#             all_records = []
#             while True:
#                 # Fetch records for the current page
#                 records = self.retrieveRecord(page)
                
#                 # Break the loop if no more records are returned
#                 if not records or len(records.get('items')) == 0:
#                     break

#                 # Append records to the list
#                 all_records.extend(records.get('items'))

#                 print(str(page) + '=====' + str((page % save_interval)) +'====='+ str(len(all_records)) +'====='+ str(max_records))
#                 # Check if it's time to save the records to a CSV file
#                 if page % save_interval == 0 and len(all_records) == max_records:
#                     # Convert the list of records to a pandas DataFrame
#                     df = pd.DataFrame(all_records)
#                     csv_filename = file_path + "batch_" + str(page/save_interval) + '.csv'
#                     df.to_csv(csv_filename, index=False, encoding='utf-8')

#                     print(f"Records spooled and saved to {csv_filename}.")
#                     # empty all_records = []
#                     all_records = []
#                 # Move to the next page
#                 page += 1

#             # Save any remaining records
#             if all_records:
#                 # Convert the list of records to a pandas DataFrame
#                 df = pd.DataFrame(all_records)
#                 csv_filename = file_path + "batch_final.csv"

#                 df.to_csv(csv_filename, index=False, encoding='utf-8')

#                 print(f"Records spooled and saved to {csv_filename}.")
#         # come back to this later
#         # process_csv_files(file_path, file_path_processed)  
#         print("All records saved successfully. Now merging files into one csv")
#         merge_files_by_pattern("Data/L-OVE/Batches", "batch_*", "Data/L-OVE/LOVE.csv")
#         print("Done merging the csv files.")

#         print("Starting enrichment for the combined file (LOVE-DB)...")
#         # enricher = DOIEnricher("Data/L-OVE/LOVE.csv")
#         # enricher.run(output_file="Data/L-OVE/LOVE_enriched.csv", key="doi")
#         # print("Done with enrich (LOVE-DB)...")
        
#         return self
    
# def get_max_batch_file(directory):
#     # Ensure the directory exists
#     if not os.path.exists(directory):
#         return None

#     # Get a list of CSV files in the directory
#     csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]

#     # If no CSV files are found, return None
#     if not csv_files:
#         return None

#     # Define a regular expression pattern to extract numbers from filenames
#     pattern = re.compile(r'batch_(\d+)')

#     # Initialize variables to store the maximum number and corresponding filename
#     max_number = 0
#     max_filename = None

#     # Iterate through the CSV files and find the maximum number
#     for csv_file in csv_files:
#         match = pattern.search(csv_file)
#         if match:
#             number = int(match.group(1))
#             if number > max_number:
#                 max_number = number
#                 max_filename = csv_file

#     # Return the filename with the maximum number
#     return max_number

# def merge_files_by_pattern(directory_path, pattern, output_file_path):
#     # Check if the directory exists
#     if not os.path.exists(directory_path):
#         raise FileNotFoundError(f"Directory not found: {directory_path}")

#     # Construct the full pattern for globbing
#     full_pattern = os.path.join(directory_path, pattern)

#     # Use glob to find files matching the pattern
#     matching_files = glob(full_pattern)

#     # Check if there are any matching files
#     if not matching_files:
#         print(f"No matching files found in '{directory_path}' with pattern '{pattern}'.")
#         return

#     # Initialize an empty DataFrame to store the merged data
#     merged_df = pd.DataFrame()

#     # Loop through each matching file and merge it into the DataFrame
#     for file_path in matching_files:
#         df = pd.read_csv(file_path)
#         merged_df = pd.concat([merged_df, df], ignore_index=True)

#     # Apply the filters
#     filtered_df = merged_df[
#         (merged_df['classification'] == 'systematic-review') &
#         (merged_df['year'] >= 2011) &
#         (merged_df['doi'].notna()) &
#         (merged_df['doi'].str.strip() != "")
#     ]

#     original_count = len(merged_df)

#     # Compute number of records removed
#     removed_count = original_count - len(filtered_df)

#     # Print the summary
#     print(f"Original count: {original_count}")
#     print(f"Filtered count: {len(filtered_df)}")
#     print(f"Records removed: {removed_count}")

#     # Save the merged DataFrame to the specified output file
#     filtered_df.to_csv(output_file_path, index=False)

#     print(f"Matching files in '{directory_path}' have been merged into '{output_file_path}'.")


# def process_csv_files(directory_path, file_path_processed):
#     # Use glob to find all CSV files in the directory
#     csv_files = glob(f"{directory_path}/*.csv")

#     # Check if the target directory exists, if not, create it
#     if not os.path.exists(file_path_processed):
#         os.makedirs(file_path_processed)

#     # Loop through each CSV file and perform DataFrame operation
#     for csv_file in csv_files:
#         # split file by separating ext from file name
#         file_name = csv_file.split("/")[-1].split(".csv")[0]
#         modified_csv_file = os.path.join(file_path_processed, f"{file_name}_modified.csv")
#         print(modified_csv_file)
#         if not os.path.exists(modified_csv_file):
#             file_name_with_extension = os.path.basename(csv_file)
            
#             # Save the modified DataFrame back to the CSV file or to a new file
#             furtherProcessiLoveValid(directory_path, file_name_with_extension, modified_csv_file)
            
#             print(f"Processed: {csv_file} -> {modified_csv_file}")
#         else:
#             print("Already processed...")
        


# def furtherProcessiLoveValid(dir, CSV_FILE, file_modified_name ):
#     data = pd.read_csv(dir+CSV_FILE)
#     result_dataframe = process_data_valid(data)
#     result_dataframe.to_csv(file_modified_name, index=False)
        
# def furtherProcessiLove(dir, CSV_FILE, file_modified_name ):
#     # check if file already exist to avoid start all over again.
#     create_directory_if_not_exists(dir)
#     # CSV_FILE = 'LOVEDB.csv'
#     if check_file_existence(dir, CSV_FILE):
#         print("We have a file so we do not need to download anything...")
#         # itemInfo_itemIdList_doi
#         df = pd.read_csv(dir+CSV_FILE)
#         # df =df.head(4)
#         # check if DF has 'full_text_URL', 'full_text_content_type'
#         if(not 'full_text_URL' in df.columns and not 'full_text_content_type' in df.columns):
#             result = df['doi'].apply(lambda row: getDOI(row))
#             # Create a new DataFrame with the results
#             result_df = pd.DataFrame(result.tolist(), columns=['full_text_URL', 'full_text_content_type'])
#             # Concatenate the new DataFrame with the original DataFrame
#             df = pd.concat([df, result_df], axis=1)
#         # Use a context manager to ensure proper file closure
#         df.to_csv(dir+CSV_FILE, index=False)
#         """Create our new dataframe and save it"""
#         process_new_sheet(df).to_csv(file_modified_name)
#     else:
#         # itemInfo_itemIdList_doi
#         df = pd.read_csv(dir+CSV_FILE)
#         result = df['doi'].apply(lambda row: getDOI(row))
#         # Create a new DataFrame with the results
#         result_df = pd.DataFrame(result.tolist(), columns=['full_text_URL', 'full_text_content_type'])
#         # Concatenate the new DataFrame with the original DataFrame
#         df = pd.concat([df, result_df], axis=1)
#         # Save DataFrame to a CSV file
#         df.to_csv(dir+CSV_FILE, index=False)
#         process_new_sheet(df).to_csv(file_modified_name)


import os
import re
import time
import math
import pandas as pd
from glob import glob
from datetime import datetime

from src.Commands.DOIEnricher import DOIEnricher  # optional, not invoked here
from src.Services.Service import Service
from src.Request.ApiRequest import ApiRequest
from src.Utils.Helpers import create_directory_if_not_exists

# -------------------- DOI helpers --------------------

_DOI_RE = re.compile(r'(10\.\d{4,9}/[-._;()/:A-Z0-9]+)', re.IGNORECASE)

def _extract_possible_doi(x):
    """
    Try hard to get a DOI string from various shapes:
    - plain doi in 'doi'
    - a URL containing a doi (e.g., https://doi.org/10.1234/abc)
    - nested dict/list in 'identifiers' or 'external_ids'
    Returns a cleaned DOI or None.
    """
    if pd.isna(x):
        return None

    def _from_string(s: str):
        s = s.strip()
        if not s:
            return None
        m = _DOI_RE.search(s)
        return m.group(1) if m else None

    # direct string
    if isinstance(x, str):
        return _from_string(x)

    # list of dicts/strings
    if isinstance(x, list):
        for item in x:
            if isinstance(item, str):
                v = _from_string(item)
                if v:
                    return v
            elif isinstance(item, dict):
                for k in ("doi", "value", "id", "url"):
                    if k in item and isinstance(item[k], str):
                        v = _from_string(item[k])
                        if v:
                            return v
        return None

    # dict
    if isinstance(x, dict):
        for k in ("doi", "value", "id", "url"):
            if k in x and isinstance(x[k], str):
                v = _from_string(x[k])
                if v:
                    return v
        for k in ("identifiers", "external_ids"):
            if k in x:
                return _extract_possible_doi(x[k])

    return None

def _normalize_year(df: pd.DataFrame) -> pd.Series:
    """
    Best-effort year extraction from any of these columns:
    - 'year' (numeric or string)
    - 'publication_year'
    - 'publicationDate' / 'publication_date'
    - 'date' / 'published' (ISO 8601 or yyyy-mm-dd)
    Returns an int Series (NaN for unknown).
    """
    candidates = []
    for col in ["year", "publication_year", "publicationDate",
                "publication_date", "date", "published"]:
        if col in df.columns:
            candidates.append(col)

    if not candidates:
        return pd.to_numeric(pd.Series([None]*len(df)), errors="coerce")

    year = pd.Series([None]*len(df))
    for col in candidates:
        s = df[col]
        if s.dtype == object:
            y = s.astype(str).str.extract(r'(^\d{4})')[0]
            y = pd.to_numeric(y, errors="coerce")
        else:
            y = pd.to_numeric(s, errors="coerce")
        mask = year.isna() & y.notna()
        year.loc[mask] = y.loc[mask]

    return pd.to_numeric(year, errors="coerce")

def _classification_contains_sr(val) -> bool:
    """
    True if classification contains 'systematic-review'.
    Works for exact string, comma-separated, list, or dicts.
    """
    if pd.isna(val):
        return False
    if isinstance(val, str):
        return 'systematic-review' in val.lower()
    if isinstance(val, list):
        return any(isinstance(x, str) and 'systematic-review' in x.lower() for x in val)
    if isinstance(val, dict):
        for k in ("type", "label", "name"):
            v = val.get(k)
            if isinstance(v, str) and 'systematic-review' in v.lower():
                return True
        for k in ("values", "labels"):
            v = val.get(k)
            if isinstance(v, list) and any(isinstance(x, str) and 'systematic-review' in x.lower() for x in v):
                return True
    return False

# -------------------- Main class --------------------

class LoveV(Service):
    def __init__(self, pageSize=500, max_retries=4, base_sleep=1.0, request_timeout=60):
        """
        pageSize:        items per API page
        max_retries:     max attempts per API request
        base_sleep:      base sleep seconds for backoff, grows exponentially
        request_timeout: seconds to wait for API before giving up (if ApiRequest supports it)
        """
        self.pageSize = int(pageSize)
        self.max_retries = int(max_retries)
        self.base_sleep = float(base_sleep)
        self.request_timeout = int(request_timeout)

        # Date-based snapshot directory (mirrors your Ovid approach)
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        self.data_dir = os.path.join("Data", "L-OVE", f"data_{self.date_str}")
        create_directory_if_not_exists(self.data_dir)

        # Final merged output (ALWAYS overwritten)
        self.merged_out = os.path.join("Data", "L-OVE", "LOVE.csv")
        create_directory_if_not_exists("Data/L-OVE")

        # API endpoint (adjust domain/path as needed)
        self.api_url = "https://api.iloveevidence.com/v2.1/loves/5e6fdb9669c00e4ac072701d/references"

        # Default payload “template” — mutate per-call with page/size
        self._payload_base = {
            "sort_by": "year",
            # Adjust this to your L·OVE collection/topic(s) as needed:
            "metadata_ids": ["603b9fe03d05151f35cf13dc"],
            "query": (
                '(review OR "systematic review" OR "systematic overview" OR "quantitative review" '
                ' OR "quantitative overview" OR "quantitative synthesis" OR "methodologic review" '
                ' OR "integrative research review" OR "research integration" OR meta-analysis '
                ' OR metaanalysis OR "meta analysis") '
                'AND '
                '(vaccine OR vaccines OR vaccination OR immunization OR immunisation OR immunize OR immunise '
                ' OR vaccinated OR immunized OR immunised OR immunizing OR immunising)'
            ),
            "classification_filter": "primary-study,systematic-review,broad-synthesis",
            "broad_synthesis_design_filter": "osr,guideline,pb,sr,fb,grade,not_grade",
            "type_of_meta_analysis_filter": "lsr",
            "study_design_filter": "rct,non-rct",
            "publication_type_filter": "clinicaltrials,thesis,journal,book,report,conference,webpage",
            "has_abstract": "all",
            "year_from_filter": 2011
            # If API supports language/OA filters, add them here once confirmed.
        }

    # --------------- plumbing ----------------

    def authenticate(self, headers):
        self.auth_headers = headers
        return self

    def fetch(self, headers):
        return self.authenticate(headers).executeRetrieveData()

    # --------------- API call with retries ----------------

    def _call_api(self, page: int):
        """
        Calls the L·OVE API with retries and exponential backoff.
        Returns the parsed 'data' dict or None.
        """
        payload = dict(self._payload_base)
        payload["page"] = int(page)
        payload["size"] = self.pageSize

        attempt = 0
        while True:
            attempt += 1
            try:
                req = ApiRequest('json', self.api_url, headers=getattr(self, "auth_headers", None))
                # If ApiRequest supports timeout, pass it; else handle inside ApiRequest
                resp = req.send_data(payload)
                if not isinstance(resp, dict):
                    raise ValueError("Unexpected API response type")

                data = resp.get("data")
                if data is None:
                    # Some APIs wrap directly under 'items'
                    if "items" in resp:
                        data = {"items": resp["items"]}
                    else:
                        data = {"items": []}
                return data

            except Exception as e:
                if attempt >= self.max_retries:
                    print(f"[ERROR] API call failed after {attempt} attempts on page={page}: {e}")
                    return None
                sleep_for = self.base_sleep * (2 ** (attempt - 1))
                print(f"[WARN] API call failed (attempt {attempt}/{self.max_retries}) on page={page}: {e}. "
                      f"Retrying in {sleep_for:.1f}s ...")
                time.sleep(sleep_for)

    # --------------- main driver ----------------

    def executeRetrieveData(self, max_pages=None):
        """
        Streams pages from the API, writes each page CSV to the date folder,
        then merges everything into Data/L-OVE/LOVE.csv (overwrites).
        """
        # Resume from last saved page in this date folder
        last_page = get_max_page_file(self.data_dir) or 0
        page = last_page + 1

        pages_written = 0
        while True:
            if max_pages and pages_written >= max_pages:
                print(f"Reached max_pages={max_pages}. Stopping.")
                break

            data = self._call_api(page)
            if not data:
                print("No data returned (API error or empty). Stopping.")
                break

            items = data.get("items") or []
            if len(items) == 0:
                print("No more items reported by API. Stopping.")
                break

            # Save page to CSV
            df = pd.DataFrame(items)
            page_path = os.path.join(self.data_dir, f"love_page_{page}.csv")
            df.to_csv(page_path, index=False, encoding="utf-8")
            print(f"Saved page {page} -> {page_path} ({len(df)} rows)")

            pages_written += 1
            page += 1

        # Merge all and overwrite the final file
        print("Merging snapshot into Data/L-OVE/LOVE.csv ...")
        merge_pages_and_overwrite(self.data_dir, self.merged_out)
        print(f"Done. Overwritten: {self.merged_out}")

        return self

# --------------- helpers ----------------

def get_max_page_file(directory):
    """
    Finds the highest page number among files named love_page_{n}.csv in `directory`.
    Returns an int or None if no pages exist.
    """
    if not os.path.exists(directory):
        return None
    files = glob(os.path.join(directory, "love_page_*.csv"))
    if not files:
        return None
    pat = re.compile(r"love_page_(\d+)\.csv$")
    max_n = 0
    for path in files:
        m = pat.search(os.path.basename(path))
        if m:
            n = int(m.group(1))
            if n > max_n:
                max_n = n
    return max_n if max_n > 0 else None

def merge_pages_and_overwrite(data_dir, output_file_path):
    """
    Robust merge:
      1) Concatenate all love_page_*.csv in the date folder
      2) Write a raw snapshot next to the pages (for auditing)
      3) Normalize classification/year/doi
      4) Filter to SR + year>=2011 + has DOI
      5) Overwrite Data/L-OVE/LOVE.csv
      6) Print diagnostics
    """
    files = sorted(glob(os.path.join(data_dir, "love_page_*.csv")))
    if not files:
        print(f"No page files found in {data_dir}. Writing empty output.")
        pd.DataFrame().to_csv(output_file_path, index=False, encoding="utf-8")
        return

    frames = []
    unreadable = 0
    for p in files:
        try:
            frames.append(pd.read_csv(p))
        except Exception as e:
            unreadable += 1
            print(f"[WARN] Skipping unreadable file: {p} ({e})")

    if not frames:
        print("[WARN] No readable CSVs. Writing empty output.")
        pd.DataFrame().to_csv(output_file_path, index=False, encoding="utf-8")
        return

    merged = pd.concat(frames, ignore_index=True)
    merged_out = os.path.join("Data", "L-OVE", "LOVE.csv")
    # raw_snapshot = os.path.join(data_dir, "LOVE_raw_snapshot.csv")
    merged.to_csv(merged_out, index=False, encoding="utf-8")
    print(f"Raw snapshot written: {merged_out} ({len(merged)} rows). Unreadable files: {unreadable}")

    df = merged.copy()

    # # ---- derive year ----
    # df["_year_norm"] = _normalize_year(df)

    # # ---- derive DOI (fill from multiple places) ----
    # if "doi" not in df.columns:
    #     df["doi"] = None
    # for col in ["doi", "identifier", "identifiers", "external_ids", "url", "fullTextUrl", "full_text_URL"]:
    #     if col in df.columns:
    #         missing = df["doi"].isna() | (df["doi"].astype(str).str.strip() == "")
    #         if missing.any():
    #             df.loc[missing, "doi"] = df.loc[missing, col].apply(_extract_possible_doi)
    # df["_has_doi"] = df["doi"].notna() & (df["doi"].astype(str).str.strip() != "")

    # # ---- is SR? ----
    # if "classification" in df.columns:
    #     df["_is_sr"] = df["classification"].apply(_classification_contains_sr)
    # else:
    #     if "study_type" in df.columns:
    #         df["_is_sr"] = df["study_type"].astype(str).str.contains("systematic", case=False, na=False)
    #     else:
    #         df["_is_sr"] = False

    # # ---- Diagnostics BEFORE filter ----
    # print("=== Diagnostics BEFORE filter ===")
    # print(f"Total rows: {len(df)}")
    # if "classification" in df.columns:
    #     try:
    #         print("classification head:", df["classification"].head(3).tolist())
    #     except Exception:
    #         pass
    # print("Count _is_sr=True:", int(df["_is_sr"].sum()))
    # print("Count _has_doi=True:", int(df["_has_doi"].sum()))
    # print("Year stats (non-null):", df["_year_norm"].dropna().describe() if df["_year_norm"].notna().any() else "none")

    # # ---- FILTERS ----
    # filtered = df[
    #     (df["_is_sr"] == True) &
    #     (df["_year_norm"] >= 2011) &
    #     (df["_has_doi"] == True)
    # ].copy()

    # # ---- Diagnostics AFTER filter ----
    # print("=== Diagnostics AFTER filter ===")
    # print(f"Rows kept: {len(filtered)}  |  Rows removed: {len(df) - len(filtered)}")
    # if len(filtered):
    #     try:
    #         print("Kept years (min→max):", (int(filtered["_year_norm"].min()), int(filtered["_year_norm"].max())))
    #     except Exception:
    #         pass

    # # Tidy columns
    # if "_year_norm" in filtered.columns:
    #     filtered.rename(columns={"_year_norm": "year_norm"}, inplace=True)
    # filtered.drop(columns=[c for c in filtered.columns if c.startswith("_")], inplace=True, errors="ignore")

    # # Overwrite final
    # # filtered.to_csv(output_file_path, index=False, encoding="utf-8")
    # print(f"Final written to: {output_file_path}  (raw: {len(df)} → filtered: {len(filtered)})")
