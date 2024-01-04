import requests
from src.Utils.Helpers import (
    check_file_existence, 
    create_directory_if_not_exists,
    getDOI, process_new_sheet,
    search_and_extract_html
)
from src.Utils.Reexpr import pattern_dict_regex
import pandas as pd

from src.api_utils import medline_access


medline_access()