import unittest
import logging
from src.Utils.Helpers import create_columns_from_text
from src.Utils.Reexpr import searchRegEx

"""
    Test for population
"""
def test_new_born_tagging():
    document = """
    ages 0 - 1 years,
    """
    tag = create_columns_from_text(document=document, searchRegEx=searchRegEx)
    # logging.info(tag)
    assert 'Newborn_0-1' in tag.get("Population#AgeGroup", "")
    
def test_new_born_and_children_tagging():
    document = """
    greater then 20 years, and 0 to 3 years
    """
    tag = create_columns_from_text(document=document, searchRegEx=searchRegEx)
    values_to_check = ['Newborn_0-1', 'Adults_18-64']
    assert all(value in tag.get("Population#AgeGroup", "") for value in values_to_check)
    
    
def test_age_group_key_tagging():
    document = """
    greater then 20 years, newborn, babies, baby, infant
    """
    tag = create_columns_from_text(document=document, searchRegEx=searchRegEx)
    values_to_check = ['Newborn_0-1', 'Adults_18-64']
    assert all(value in tag.get("Population#AgeGroup", "") for value in values_to_check)
    
# test for adult without adding to document
def test_age_group_key_adult_tagging():
    document = """
    greater then 20 years, newborn, babies, baby, infant
    """
    tag = create_columns_from_text(document=document, searchRegEx=searchRegEx)
    values_to_check = ['Newborn_0-1', 'Adults_18-64', 'OlderAdults_65-10000']
    assert all(value in tag.get("Population#AgeGroup", "") for value in values_to_check)
    
    
# test for adult after adding adult
def test_age_group_key_adult_tagging():
    document = """
    greater then 20 years, newborn, babies, baby, infant, older adults
    """
    tag = create_columns_from_text(document=document, searchRegEx=searchRegEx)
    values_to_check = ['Newborn_0-1', 'Adults_18-64', 'OlderAdults_65-10000']
    assert all(value in tag.get("Population#AgeGroup", "") for value in values_to_check)
    
    
"""
    Add as much as you wish...
"""