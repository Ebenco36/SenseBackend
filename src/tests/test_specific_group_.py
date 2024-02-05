import unittest
import logging
from src.Utils.Helpers import create_columns_from_text
from src.Utils.Reexpr import searchRegEx

"""
    Test for population
"""
def test_specific_group_healthcare_workers_tagging():
    document = """
    Gastroenterologist are the ones with the best methods
    """
    tag = create_columns_from_text(document=document, searchRegEx=searchRegEx)
    # logging.info(tag)
    assert 'HealthcareWorkers' in tag.get("Population#SpecificGroup", "")
    
    
def test_specific_group_healthcare_workers_pregnant_tagging():
    document = """
    Gastroenterologist are the ones with the best methods pregnant
    """
    tag = create_columns_from_text(document=document, searchRegEx=searchRegEx)
    values_to_check = ['HealthcareWorkers', 'PregnantWomen']
    assert all(value in tag.get("Population#SpecificGroup", "") for value in values_to_check)