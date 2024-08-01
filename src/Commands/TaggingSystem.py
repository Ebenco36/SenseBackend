import re

class TaggingSystem:
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
        number_keywords = ["study", "studies", "number", "sample size"]
        found_tags = [keyword for keyword in number_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_population_specificGroup_tags(text):
        # Example implementation
        specific_group_keywords = ["specific group", "target group", "study group"]
        found_tags = [keyword for keyword in specific_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_population_OtherSpecificGroup_tags(text):
        # Example implementation
        other_specific_group_keywords = ["other specific group", "alternative group"]
        found_tags = [keyword for keyword in other_specific_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_population_specificGroup_acronyms(text):
        # Example implementation
        acronyms = ["SG", "TG", "SGG"]  # Replace with actual acronyms
        found_tags = [acronym for acronym in acronyms if re.search(r'\b' + acronym + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_population_ageGroup_tags(text):
        # Example implementation
        age_group_keywords = ["child", "adult", "elderly", "age group"]
        found_tags = [keyword for keyword in age_group_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_population_immuneStatus_tags(text):
        # Example implementation
        immune_status_keywords = ["immune", "immunity", "immune status", "vaccinated"]
        found_tags = [keyword for keyword in immune_status_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_intervention_vaccinePredictableDisease_tags(text):
        # Example implementation
        vaccine_keywords = ["vaccine", "predictable disease", "preventable disease"]
        found_tags = [keyword for keyword in vaccine_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_intervention_vaccineOptions_tags(text):
        # Example implementation
        vaccine_options_keywords = ["vaccine options", "types of vaccine"]
        found_tags = [keyword for keyword in vaccine_options_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_acceptance_tags(text):
        # Example implementation
        acceptance_keywords = ["acceptance", "public opinion", "perception"]
        found_tags = [keyword for keyword in acceptance_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_coverage_tags(text):
        # Example implementation
        coverage_keywords = ["coverage", "extent", "scope"]
        found_tags = [keyword for keyword in coverage_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_economic_tags(text):
        # Example implementation
        economic_keywords = ["economic", "cost", "financial", "economic impact"]
        found_tags = [keyword for keyword in economic_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_ethicalIssues_tags(text):
        # Example implementation
        ethical_keywords = ["ethical", "ethical issues", "morality"]
        found_tags = [keyword for keyword in ethical_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_administration_tags(text):
        # Example implementation
        administration_keywords = ["administration", "management", "delivery"]
        found_tags = [keyword for keyword in administration_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_efficacyEffectiveness_tags(text):
        # Example implementation
        efficacy_keywords = ["efficacy", "effectiveness", "outcome"]
        found_tags = [keyword for keyword in efficacy_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_immunogenicity_tags(text):
        # Example implementation
        immunogenicity_keywords = ["immunogenicity", "immune response", "immunity"]
        found_tags = [keyword for keyword in immunogenicity_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_topic_safety_tags(text):
        # Example implementation
        safety_keywords = ["safety", "adverse effects", "side effects"]
        found_tags = [keyword for keyword in safety_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
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
        death_keywords = ["death", "mortality", "deceased"]
        found_tags = [keyword for keyword in death_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

    @staticmethod
    def tag_outcome_ICU_tags(text):
        # Example implementation
        ICU_keywords = ["ICU", "intensive care", "intensive unit"]
        found_tags = [keyword for keyword in ICU_keywords if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE)]
        return found_tags

# # Example usage
# tagging_system = TaggingSystem()

# # Register all the tagging methods
# tagging_system.register_tag_method("review_tags", TaggingSystem.tag_review_tags)
# tagging_system.register_tag_method("number_of_studies_tags", TaggingSystem.tag_number_of_studies_tags)
# tagging_system.register_tag_method("population_specificGroup_tags", TaggingSystem.tag_population_specificGroup_tags)
# tagging_system.register_tag_method("population_OtherSpecificGroup_tags", TaggingSystem.tag_population_OtherSpecificGroup_tags)
# tagging_system.register_tag_method("population_specificGroup_acronyms_", TaggingSystem.tag_population_specificGroup_acronyms)
# tagging_system.register_tag_method("population_ageGroup_tags", TaggingSystem.tag_population_ageGroup_tags)
# tagging_system.register_tag_method("population_immuneStatus_tags", TaggingSystem.tag_population_immuneStatus_tags)
# tagging_system.register_tag_method("intervention_vaccinePredictableDisease_tags", TaggingSystem.tag_intervention_vaccinePredictableDisease_tags)
# tagging_system.register_tag_method("intervention_vaccineOptions_tags", TaggingSystem.tag_intervention_vaccineOptions_tags)
# tagging_system.register_tag_method("topic_acceptance_tags", TaggingSystem.tag_topic_acceptance_tags)
# tagging_system.register_tag_method("topic_coverage_tags", TaggingSystem.tag_topic_coverage_tags)
# tagging_system.register_tag_method("topic_economic_tags", TaggingSystem.tag_topic_economic_tags)
# tagging_system.register_tag_method("topic_ethicalIssues_tags", TaggingSystem.tag_topic_ethicalIssues_tags)
# tagging_system.register_tag_method("topic_administration_tags", TaggingSystem.tag_topic_administration_tags)
# tagging_system.register_tag_method("topic_efficacyEffectiveness_tags", TaggingSystem.tag_topic_efficacyEffectiveness_tags)
# tagging_system.register_tag_method("topic_immunogenicity_tags", TaggingSystem.tag_topic_immunogenicity_tags)
# tagging_system.register_tag_method("topic_safety_tags", TaggingSystem.tag_topic_safety_tags)
# tagging_system.register_tag_method("outcome_infection_tags", TaggingSystem.tag_outcome_infection_tags)
# tagging_system.register_tag_method("outcome_hospitalization_tags", TaggingSystem.tag_outcome_hospitalization_tags)
# tagging_system.register_tag_method("outcome_death_tags", TaggingSystem.tag_outcome_death_tags)
# tagging_system.register_tag_method("outcome_ICU_tags", TaggingSystem.tag_outcome_ICU_tags)

# # Example text
# text = """This study reviews the impact of various vaccines on different populations. It includes data on infection rates, hospitalization, and mortality, while also addressing economic aspects and public acceptance."""

# # Apply tags to the text
# tags = tagging_system.apply_tags(text)
# print(tags)
