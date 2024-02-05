import unittest
import logging
from src.Utils.Helpers import create_columns_from_text
from src.Utils.Reexpr import searchRegEx

"""
    Test for population
"""
def test_topics_tagging():
    document = """
    Gastroenterologist are the ones with the best methods efficacy
    """
    tag = create_columns_from_text(document=document, searchRegEx=searchRegEx)
    # logging.info(tag)
    assert 'Efficacy-Effectiveness' in tag.get("Topic#Efficacy-Effectiveness", "")
    
    
def test_topic_safety_effectiveness_tagging():
    document = """
    Gastroenterologist are the ones with the best methods pregnant, Safety, efficacy
    """
    tag = create_columns_from_text(document=document, searchRegEx=searchRegEx)
    assert 'Efficacy-Effectiveness' in tag.get("Topic#Efficacy-Effectiveness", "") and 'Safety' in tag.get("Topic#Safety", "")