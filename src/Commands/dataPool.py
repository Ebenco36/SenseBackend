import sys
import os
sys.path.append(os.getcwd())

from src.api_utils import (
    ilove_access, 
    cochrane_access, 
    medline_class_access,
    ovid_new_access
)

# ilove_access()
# medline_class_access()
# cochrane_access()
ovid_new_access()