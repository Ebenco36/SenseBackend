
def preprocessResolvedData(resolved_result, ):
    review_tags, _, review_bag = resolved_result.get("review", ["", "", []])
    number_of_studies_tags, _, number_of_studies_bag = resolved_result.get("number_of_studies", ["", "", []])
    population_specificGroup_tags, population_specificGroup_acronyms_, population_specificGroup_bag = resolved_result.get("population_specificGroup", ["", "", []])
    population_ageGroup_tags, _, population_ageGroup_bag = resolved_result.get("population_ageGroup", ["", "", []])
    population_OtherSpecificGroup_tags, _, population_OtherSpecificGroup_bag = resolved_result.get("population_OtherSpecificGroup", ["", "", []])
    population_immuneStatus_tags, _, population_immuneStatus_bag = resolved_result.get("population_immuneStatus", ["", "", []])
    intervention_vaccinePredictableDisease_tags, _, intervention_vaccinePredictableDisease_bag = resolved_result.get("intervention_vaccinePredictableDisease", ["", "", []])
    intervention_vaccineOptions_tags, _, intervention_vaccineOptions_bag = resolved_result.get("intervention_vaccineOptions", ["", "", []])
    topic_acceptance_tags, _, topic_acceptance_bag = resolved_result.get("topic_acceptance", ["", "", []])
    topic_coverage_tags, _, topic_coverage_bag = resolved_result.get("topic_coverage", ["", "", []])
    topic_economic_tags, _, topic_economic_bag = resolved_result.get("topic_economic", ["", "", []])
    topic_ethicalIssues_tags, _, topic_ethicalIssues_bag = resolved_result.get("topic_ethicalIssues", ["", "", []])
    topic_administration_tags, _, topic_administration_bag = resolved_result.get("topic_administration", ["", "", []])
    topic_efficacyEffectiveness_tags, _, topic_efficacyEffectiveness_bag = resolved_result.get("topic_efficacyEffectiveness", ["", "", []])
    topic_immunogenicity_tags, _, topic_immunogenicity_bag = resolved_result.get("topic_immunogenicity", ["", "", []])
    topic_safety_tags, _, topic_safety_bag = resolved_result.get("topic_safety", ["", "", []])
    outcome_infection_tags, _, outcome_infection_bag = resolved_result.get("outcome_infection", ["", "", []])
    outcome_hospitalization_tags, _, outcome_hospitalization_bag = resolved_result.get("outcome_hospitalization", ["", "", []])
    outcome_death_tags, _, outcome_death_bag = resolved_result.get("outcome_death", ["", "", []])
    outcome_ICU_tags, _, outcome_ICU_bag = resolved_result.get("outcome_ICU", ["", "", []])
    openAccess_tags, _, openAccess_bag = resolved_result.get("openAccess", ["", "", []])
    
    response = (
        review_tags, review_bag,
        number_of_studies_tags, number_of_studies_bag,
        population_specificGroup_tags, population_specificGroup_acronyms_, population_specificGroup_bag,
        population_ageGroup_tags, population_ageGroup_bag,
        population_immuneStatus_tags, population_immuneStatus_bag,
        population_OtherSpecificGroup_tags, population_OtherSpecificGroup_bag,
        intervention_vaccinePredictableDisease_tags, intervention_vaccinePredictableDisease_bag,
        intervention_vaccineOptions_tags, intervention_vaccineOptions_bag,
        topic_acceptance_tags, topic_acceptance_bag,
        topic_coverage_tags, topic_coverage_bag,
        topic_economic_tags, topic_economic_bag,
        topic_ethicalIssues_tags, topic_ethicalIssues_bag,
        topic_administration_tags, topic_administration_bag,
        topic_efficacyEffectiveness_tags, topic_efficacyEffectiveness_bag,
        topic_immunogenicity_tags, topic_immunogenicity_bag,
        topic_safety_tags, topic_safety_bag ,
        outcome_infection_tags, outcome_infection_bag,
        outcome_hospitalization_tags, outcome_hospitalization_bag,
        outcome_death_tags, outcome_death_bag,
        outcome_ICU_tags, outcome_ICU_bag,
        openAccess_tags, openAccess_bag
    )

    return response

def ourColumns():
    columns = [
        "review_tags", "review_bag", "number_of_studies_tags", "number_of_studies_bag",
        "population_specificGroup_tags", "population_specificGroup_acronyms_", 
        "population_specificGroup_bag", "population_ageGroup_tags", "population_ageGroup_bag", 
        "population_immuneStatus_tags", "population_immuneStatus_bag",
        "population_OtherSpecificGroup_tags", "population_OtherSpecificGroup_bag",
        "intervention_vaccinePredictableDisease_tags", "intervention_vaccinePredictableDisease_bag",
        "intervention_vaccineOptions_tags", "intervention_vaccineOptions_bag", "topic_acceptance_tags", 
        "topic_acceptance_bag", "topic_coverage_tags", "topic_coverage_bag", "topic_economic_tags", 
        "topic_economic_bag", "topic_ethicalIssues_tags", "topic_ethicalIssues_bag",
        "topic_administration_tags", "topic_administration_bag", "topic_efficacyEffectiveness_tags", 
        "topic_efficacyEffectiveness_bag", "topic_immunogenicity_tags", "topic_immunogenicity_bag",
        "topic_safety_tags", "topic_safety_bag", "outcome_infection_tags", "outcome_infection_bag",
        "outcome_hospitalization_tags", "outcome_hospitalization_bag", "outcome_death_tags", "outcome_death_bag",
        "outcome_ICU_tags", "outcome_ICU_bag", "openAccess_tags", "openAccess_bag"
    ]

    return columns

