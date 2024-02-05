import sys
import os
sys.path.append(os.getcwd())

from src.api_utils import (
    embase_access, 
    ilove_access, 
    cochrane_access, 
    medline_access
)

    
embase_access()
ilove_access()
medline_access()
cochrane_access()