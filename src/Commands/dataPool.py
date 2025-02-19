import sys
import os
sys.path.append(os.getcwd())

from src.api_utils import (
    ilove_access, 
    cochrane_access, 
    medline_class_access,
    ovid_new_access
)

print("Starting with LOVE DB...")
# ilove_access()
print("Done extracting content from LOVE DB.")

print("Proceed with Medline...")
# medline_class_access()
print("Done with Medline.")

print("Start pulling from Cochrane...")
# cochrane_access()
print("Done with Cochrane.")

print("Last but not the least OVID DB...")
ovid_new_access()
print("Done with OVID DB.")