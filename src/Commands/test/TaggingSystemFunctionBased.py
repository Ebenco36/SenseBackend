import re

from src.Utils.Reexpr import searchRegEx
from src.Utils.Helpers import create_columns_from_text

class TaggingSystemFunctionBased:
    def __init__(self):
        # Dictionary to store tag methods
        self.tag_methods = {}

    def register_tag_method(self, name, method):
        """
        Register a new tagging method.
        
        :param name: The name of the tagging method.
        :param method: The method to register. It should accept a text parameter and return results.
        """
        if not callable(method):
            raise ValueError("The method must be callable.")
        self.tag_methods[name] = method

    def apply_tags(self, text):
        """
        Apply all registered tagging methods to the text and return a dictionary of results.
        
        :param text: The text to apply tags to.
        :return: A dictionary of tag names and their corresponding results.
        """
        tags = {}
        for name, method in self.tag_methods.items():
            tags[name] = method(text)  # Apply each method and collect results
        return tags

    @staticmethod
    def tag_review_tags(text):
        # Example implementation
        return ["review"] if "review" in text.lower() else []

    @staticmethod
    def tag_number_of_studies_tags(text):
        # Example implementation
        number_keywords = ["number of study", "of studies"]
        found_tags = [keyword for keyword in number_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_population_specificGroup_healthcareWorkers_tags(text):
        # Example implementation
        specific_group_keywords = [
            "Physician", "Nurse", "Surgeon", "Dentist", "Pharmacist", "Physical Therapist",
            "Occupational Therapist", "Medical Laboratory Technologist", "Radiologist",
            "Dietitian/Nutritionist", "Respiratory Therapist", "Speech-Language Pathologist",
            "Physician Assistant", "Nurse Practitioner", "Certified Nursing Assistant (CNA)",
            "Medical Assistant", "Paramedic/EMT", "Midwife", "Psychologist",
            "Social Worker (Clinical)", "Hospital Administrator", "Medical Researcher",
            "Health Educator", "Orthopedic Technician", "Optometrist", "Podiatrist",
            "Anesthesiologist", "Neurologist", "Cardiologist", "Gastroenterologist"
        ]
        found_tags = [keyword for keyword in specific_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_population_specificGroup_pregnantWomen_tags(text):
        # Example implementation
        specific_group_keywords = ["pregnant", "pregnant women"]
        found_tags = [keyword for keyword in specific_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_population_specificGroup_parentsCaregivers_tags(text):
        # Example implementation
        specific_group_keywords = ["parents", "caregivers"]
        found_tags = [keyword for keyword in specific_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_population_OtherSpecificGroup_travellers_tags(text):
        # Example implementation
        other_specific_group_keywords = ["traveller"]
        found_tags = [keyword for keyword in other_specific_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_population_ageGroup_newborn_0to1_tags(text):
        # Example implementation
        age_group_keywords = [
            'newborn', 'babies', 'baby', 'infant', 'toddlers',
            'young ones', 'youngsters', 'small children'
        ]
        found_tags = [keyword for keyword in age_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_population_ageGroup_children_2to9_tags(text):
        # Example implementation
        age_group_keywords = ["child", "children"]
        found_tags = [keyword for keyword in age_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_population_ageGroup_adolescents_10to17_tags(text):
        # Example implementation
        age_group_keywords = ["adolescents", "adolescent", "young adults"]
        found_tags = [keyword for keyword in age_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_population_ageGroup_adults_18to64_tags(text):
        # Example implementation
        age_group_keywords = ["adults", "adult"]
        found_tags = [keyword for keyword in age_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_population_ageGroup_olderAdults_65to10000_tags(text):
        # Example implementation
        age_group_keywords = ["elderly", "older adults"]
        found_tags = [keyword for keyword in age_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_population_immuneStatus_immunocompromised_tags(text):
        # Example implementation
        immune_status_keywords = ["immune", "immunity", "immune status", "vaccinated", "immunocompromised"]
        found_tags = [keyword for keyword in immune_status_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_population_immuneStatus_healthy_tags(text):
        # Example implementation
        immune_status_keywords = ["healthy"]
        found_tags = [keyword for keyword in immune_status_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_intervention_vaccinePredictableDisease_covid_tags(text):
        # Example implementation
        keywords = ["COVID-19", "COVID", "COVID 19"]
        found_tags = [keyword for keyword in keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_intervention_vaccinePredictableDisease_influenza_tags(text):
        # Example implementation
        keywords = ["influenza"]
        found_tags = [keyword for keyword in keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_intervention_vaccinePredictableDisease_dengue_tags(text):
        # Example implementation
        keywords = ["dengue"]
        found_tags = [keyword for keyword in keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_intervention_vaccinePredictableDisease_rotavirus_tags(text):
        # Example implementation
        keywords = ["rotavirus"]
        found_tags = [keyword for keyword in keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_intervention_vaccineOptions_live_tags(text):
        # Example implementation
        keywords = ["live"]
        found_tags = [keyword for keyword in keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_intervention_vaccineOptions_nonLive_tags(text):
        # Example implementation
        keywords = ["non-live"]
        found_tags = [keyword for keyword in keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_intervention_vaccineOptions_adjuvants_tags(text):
        # Example implementation
        keywords = ["adjuvants"]
        found_tags = [keyword for keyword in keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_intervention_vaccineOptions_nonAdjuvants_tags(text):
        # Example implementation
        vaccine_options_keywords = ["non-adjuvanted", "non adjuvant"]
        found_tags = [keyword for keyword in vaccine_options_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_topic_acceptance_tags(text):
        # Example implementation
        acceptance_keywords = [
            "acceptance", "public opinion", "perception", "acceptance", 
            "Barrier", "vaccine barriers", 
            "knowledge", "vaccination willingness and intentions", 
            "HPV vaccine acceptability, acceptability", 
            "Awareness and knowledge", "Awareness", 
            "facilitators of and barriers", 
            "awareness,knowledge, acceptability, and intention", 
            "knowledge and acceptability", "knowledge and awareness", 
            "attitudes and beliefs", "Knowledge and Attitude",  
            "attitude", "knowledge, awareness, and attitude"
        ]
        found_tags = [keyword for keyword in acceptance_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_coverage_tags(text):
        # Example implementation
        coverage_keywords = ["coverage", "uptake", "the uptake", "actual uptake", "vaccine uptake"]
        found_tags = [keyword for keyword in coverage_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_economic_tags(text):
        # Example implementation
        economic_keywords = [
            "economic", "cost", "financial", "economic impact",
            "cost effectiveness", "cost-effectiveness", 
            "cost", "cost effectiveness", "economic evaluation", 
            "Cost-effectiveness of HPV vaccination strategies"
        ]
        found_tags = [keyword for keyword in economic_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_ethicalIssues_tags(text):
        # Example implementation
        ethical_keywords = [
            "ethical", "ethical issues", "morality",
             "racial", "ethnic", 
            "ethnic minority", "racial minority", 
            "racial/ethnic", "racial/ethnic minority", 
            "racial disparity", "ethnic disparity", 
            "minority", "minority population"
        ]
        found_tags = [keyword for keyword in ethical_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_administration_tags(text):
        # Example implementation
        administration_keywords = [
            "administration", "vaccine types", "dose schedules", 
            "vaccine types and dose schedules", "different dose schedules", 
            "Two doses of"
        ]
        found_tags = [keyword for keyword in administration_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_efficacyEffectiveness_tags(text):
        # Example implementation
        efficacy_keywords = ["effectiveness", "impact of", "effectiveness of", "efficacy"]
        found_tags = [keyword for keyword in efficacy_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_modeling_tags(text):
        # Example implementation
        immunogenicity_keywords = ["modeling"]
        found_tags = [keyword for keyword in immunogenicity_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_safety_tags(text):
        # Example implementation
        safety_keywords = ["safety", "adverse effects", "side effects"]
        found_tags = [keyword for keyword in safety_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags
    
    @staticmethod
    def tag_topic_risk_factor_tags(text):
        # Example implementation
        risk_keywords = ["risk factor", "risk"]
        found_tags = [keyword for keyword in risk_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_outcome_infection_tags(text):
        # Example implementation
        infection_keywords = ["infection", "infected", "disease"]
        found_tags = [keyword for keyword in infection_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_outcome_hospitalization_tags(text):
        # Example implementation
        hospitalization_keywords = ["hospitalization", "hospital", "admitted"]
        found_tags = [keyword for keyword in hospitalization_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_outcome_death_tags(text):
        # Example implementation
        death_keywords = [
            "death", "mortality", "deceased",
            "overall mortality", 
            "cancer related mortality", 
            "on overall and cancer mortality"
        ]
        found_tags = [keyword for keyword in death_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_outcome_ICU_tags(text):
        # Example implementation
        ICU_keywords = ["ICU", "intensive care", "intensive unit", "care unit"]
        found_tags = [keyword for keyword in ICU_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_all_with_old_implementation(text):
        return create_columns_from_text(text, searchRegEx)
        