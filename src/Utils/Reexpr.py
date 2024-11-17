pattern_dict_regex = {
    "review": r'\b(?:systematic review|Literature Review|review|Meta-Analysis|Critical Review|Peer Review|Book Review|Editorial Review|Review Article)\b',
    "number_of_studies": r'\b(?:\d+ studies|\d+ study)\b',
    "population_specificGroup": r'\b(?:mother|mother of|father|father of|elderly|Older adults/elderly|men|woman|child|children|adolescent|adult|young|kids|newborn|baby|babies|Young Adults)\b',
    "population_OtherSpecificGroup": r'\b(?:travelers|caregivers|caregiver|parents|parents/caregivers|healthcare|healthcare workers|pregnant|pregnant women)\b',
    "population_ageGroup": r'\b(?:\d{1}-\d{1} years|\d{1}-\d{1} years|\d{2}-\d{2} years?|between \d{1}-\d{1} years|between \d{1}-\d{2} years|between \d{2}-\d{2} years?)\b',
    "population_immuneStatus": r'\b(?:immunocompromised|healthy)\b',
    "intervention_vaccinePredictableDisease": r'\b(?:COVID-19|COVID 19|COVID19|COVID|influenza|Dengue|rotavirus)\b',
    "intervention_vaccineOptions": r'\b(?:live|non-live|adjuvants|adjuvant|non-adjuvants|non-adjuvant)\b',
    "topic_acceptance": r'\b(?:Barriers|Barrier|vaccine barriers|knowledge|vaccination willingness and intentions|HPV vaccine acceptability, acceptability|Awareness and knowledge|Awareness|facilitators of and barriers|awareness, knowledge, acceptability, and intention|knowledge and acceptability|sociodemographics, knowledge, attitudes or other factors|knowledge and awareness|attitudes and beliefs|Knowledge and Attitude|attitude|knowledge, awareness, and attitude)\b',
    "topic_coverage": r'\b(?:Vaccine uptake|decreased vaccine initiation and completion|initiated vaccination but did not complete the vaccine series|Uptake|the uptake|actual uptake)\b',
    "topic_economic": r'\b(?:Cost Effectiveness|cost-effectiveness|cost|cost effectiveness|economic evaluation|Cost-effectiveness of HPV vaccination strategies)\b',
    "topic_ethicalIssues": r'\b(?:racial|ethnic|ethnic minorit(?:y)?|racial minorit(?:y)?|racial/ethnic|racial/ethnic minorit(?:y)?|racial disparit(?:y)?|ethnic disparit(?:y)?)\b',
    "topic_administration": r'\b(?:Comparison of different human papillomavirus \(HPV\) vaccine types and dose|schedules|different dose schedules|Two doses of HPV vaccine compared with three doses of HPV vaccine|Two doses of HPV vaccine with longer interval compared with two doses of HPV vaccine with shorter interval|Nonavalent HPV vaccine compared with quadrivalent HPV vaccine)\b',
    "topic_efficacyEffectiveness": r'\b(?:Efficacy|doses?|Three doses HPV vaccine compared with control|efficacy of a HPV catch-up vaccination|the effect|Effect of HPV vaccines on outcomes|HPV vaccination efficacy|vaccination efficacy|The Efficacy and Duration of Vaccine Protection|duration of protection following HPV vaccination|efficacy of vaccination|vaccine protection|Impact and Effectiveness|impact or effectiveness of 4vHPV vaccination|effectiveness|impact or effectiveness)\b',
    "topic_immunogenicity": r'\b(?:Immunogenicity|Antibody responses|Immunological outcomes \(geometric mean titre \(GMT\) and seropositivity)\)\b',
    "topic_safety": r'\b(?:Harm|local adverse events|injection-site adverse events|adverse effects|safety|serious adverse events|vaccination safety|serious adverse events|adverse events|serious adverse event|protective effect|safety of HPV vaccine|Ads|onset of autoimmune conditions related to HPV vaccine|events)\b',
    "outcome_infection": r'\b(?:incident infections|persistent infections|infection|HPV related diseases)\b',
    "outcome_hospitalization": r'\b(?:hospitalization)\b',
    "outcome_death": r'\b(?:mortality|overall mortality|cancer related mortality|on overall and cancer mortality)\b',
    "outcome_ICU": r'\b(?:ICU|Intensive care unit)\b',
    "openAccess": r'\b(?:open access)\b',
}


searchRegEx ={
    "Population": {
        "AgeGroup": {
            "Newborn_0-1": [
                'newborn',
                'babies',
                'baby',
                'infant',
                'toddlers',
                'young ones',
                'youngsters',
                'small children'
            ],
            "Children_2-9": [
                "child", "children"
            ],
            "Adolescents_10-17": [
                "adolescents", "adolescent", "young adults"
            ],
            "Adults_18-64": [
                "adults", "adult"
            ],
            "OlderAdults_65-10000": [
                "elderly", "older adults"
            ]
        },
        "SpecificGroup": {
            "HealthcareWorkers": [
                "Physician",
                "Nurse",
                "Surgeon",
                "Dentist",
                "Pharmacist",
                "Physical Therapist",
                "Occupational Therapist",
                "Medical Laboratory Technologist",
                "Radiologist",
                "Dietitian/Nutritionist",
                "Respiratory Therapist",
                "Speech-Language Pathologist",
                "Physician Assistant",
                "Nurse Practitioner",
                "Certified Nursing Assistant (CNA)",
                "Medical Assistant",
                "Paramedic/EMT",
                "Midwife",
                "Psychologist",
                "Social Worker (Clinical)",
                "Hospital Administrator",
                "Medical Researcher",
                "Health Educator",
                "Orthopedic Technician",
                "Optometrist",
                "Podiatrist",
                "Anesthesiologist",
                "Neurologist",
                "Cardiologist",
                "Gastroenterologist"
            ],
            "PregnantWomen": [
                "pregnant", "pregnant women"
            ],
            "Travellers": [
                "traveller"
            ],
            "ParentsCaregivers": [
                "parents", "caregivers"
            ]
        },
        "ImmuneStatus": {
            "Immunocompromised": [
                "immunocompromised"
            ],
            "Healthy": [
                "healthy"
            ]
        }
    },
    "Topic": {
        "Efficacy-Effectiveness": {
           "Efficacy-Effectiveness": [
               "effectiveness", "impact of", "effectiveness of", "efficacy"
           ] 
        },
        "Safety": {
           "Safety": [
               "safety", "adverse effects", "adverse events"
           ] 
        },
        "Risk-Factor": {
           "Risk-Factor": [
               "risk factor", "risk"
           ] 
        },
        "Coverage": {
           "Coverage": [
               "coverage", "uptake", "the uptake", "actual uptake", "vaccine uptake"
           ] 
        },
        "Acceptance": {
           "Acceptance": [
               "acceptance", "Barrier", "vaccine barriers", 
               "knowledge", "vaccination willingness and intentions", 
               "HPV vaccine acceptability, acceptability", 
               "Awareness and knowledge", "Awareness", 
               "facilitators of and barriers", 
               "awareness,knowledge, acceptability, and intention", 
               "knowledge and acceptability", "knowledge and awareness", 
               "attitudes and beliefs", "Knowledge and Attitude",  
               "attitude", "knowledge, awareness, and attitude"
           ] 
        },
        "Administration": {
           "Administration": [
               "administration", "vaccine types", "dose schedules", 
               "vaccine types and dose schedules", "different dose schedules", 
               "Two doses of"
           ] 
        },
        "Economic-Aspects": {
           "Economic-Aspects": [
                "economic", "cost", "financial", "economic impact",
                "cost effectiveness", "cost-effectiveness", 
                "cost", "cost effectiveness", "economic evaluation", 
                "Cost-effectiveness of HPV vaccination strategies"
           ] 
        },
        "Modeling": {
           "Modeling": [
               "modeling"
           ] 
        },
        "Ethical-Issues": {
           "Ethical-Issues": [
               "racial", "ethnic", 
               "ethnic minority", "racial minority", 
               "racial/ethnic", "racial/ethnic minority", 
               "racial disparity", "ethnic disparity", 
               "minority", "minority population"
           ] 
        },
    },
    "Outcome": {
        "Infection": {
            "Infection": [
                "infection"
            ],
        },
        "Hospitalization": {
            "Hospitalization": [
                "hospitalization"
            ]
        },
        "ICU": {
            "ICU": [
                "ICU", "intensive care unit", "intensive care"
            ]
        },
        "Death": {
            "Death": [
                "death", "mortality", "overall mortality", 
                "cancer related mortality", "on overall and cancer mortality"
            ],
        }
    },
    "Reviews": {
        "Reviews": {
            "review": [
                "systematic review", "Literature Review", "review", 
                "Meta-Analysis", "Critical Review", "Peer Review", 
                "Book Review", "Editorial Review", "Review Article"
            ]
        }
    },
    "Studies": {
        "NoOfStudies": {
            "number_of_studies": [
                "studies", "studies"
            ]
        }
    },
    "Intervention": {
        "Vaccine-preventable-disease": {
            "COVID-19": [
                "COVID-19", "COVID", "COVID 19"
            ],
            "Influenza": [
                "influenza"
            ],
            "Dengue": [
                "dengue"
            ],
            "Rotavirus": [
                "rotavirus"
            ]
        },
        "Vaccine-Options": {
            "Live": [
                "live"
            ],
            "Non-Live": [
                "non-live"
            ],
            "Adjuvants": [
                "adjuvants"
            ],
            "Non-Adjuvanted": [
                "non-adjuvanted"
            ]
        },
    },
}