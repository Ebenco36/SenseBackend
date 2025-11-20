searchRegEx ={
    "popu": {
        "age__group": {
            "nb_0__1": [
                ('newborn', 'nb'),
                ('babies', 'nb'),
                ('baby', 'nb'),
                ('infant', 'nb'),
                ('toddlers', 'nb'),
                ('young ones', 'nb'),
                ('youngsters', 'nb'),
                ('small children', 'nb')
            ],
            "chi_2__9": [
                ("child", "chi"), ("children", "chi")
            ],
            "ado_10__17": [
                ("adolescents", "ado"), ("adolescent", "ado"), ("young adults", "ado")
            ],
            "adu_18__64": [
                ("adults", "adu"), ("adult", "adu")
            ],
            "eld_65__10000": [
                ("elderly", "eld"), ("older adults", "eld")
            ]
        },
        "specific__group": {
            "hcw": [
                ("Physician", "hcw"),
                ("Nurse", "hcw"),
                ("Surgeon", "hcw"),
                ("Dentist", "hcw"),
                ("Pharmacist", "hcw"),
                ("Physical Therapist", "hcw"),
                ("Occupational Therapist", "hcw"),
                ("Medical Laboratory Technologist", "hcw"),
                ("Radiologist", "hcw"),
                ("Dietitian/Nutritionist", "hcw"),
                ("Respiratory Therapist", "hcw"),
                ("Speech-Language Pathologist", "hcw"),
                ("Physician Assistant", "hcw"),
                ("Nurse Practitioner", "hcw"),
                ("Certified Nursing Assistant (CNA)", "hcw"),
                ("Medical Assistant", "hcw"),
                ("Paramedic/EMT", "hcw"),
                ("Midwife", "hcw"),
                ("Psychologist", "hcw"),
                ("Social Worker (Clinical)", "hcw"),
                ("Hospital Administrator", "hcw"),
                ("Medical Researcher", "hcw"),
                ("Health Educator", "hcw"),
                ("Orthopedic Technician", "hcw"),
                ("Optometrist", "hcw"),
                ("Podiatrist", "hcw"),
                ("Anesthesiologist", "hcw"),
                ("Neurologist", "hcw"),
                ("Cardiologist", "hcw"),
                ("Gastroenterologist", "hcw")
            ],
            "pw": [
                ("pregnant", "pw"), ("pregnant women", "pw")
            ],
            "tra": [
                ("traveller", "tra")
            ],
            "pcg": [
                ("parents", "cg"), ("caregivers", "cg")
            ]
        },
        "immune__status": {
            "imu": [
                ("immunocompromised", "imu")
            ],
            "hty": [
                ("healthy", "hty")
            ]
        }
    },
    "topic": {
        "eff": {
           "eff": [
                ("effectiveness", "eff"), ("impact of", "eff"), ("effectiveness of", "eff"), ("efficacy", "eff"),
                ("VE", "eff"), ("CI", "eff"), ("RR", "eff"), ("OR", "eff"), ("RD", "eff"), ("rate difference", "eff"),
                ("odds ratios", "eff"), ("odds ratio (OR)", "eff"), ("odds ratios (ORs)", "eff"),
                ("IRR", "eff"), ("relative risks(RR)", "eff"), ("relative risks", "eff"),
                ("efficacy rate", "eff"), ("effectiveness rate", "eff"), ("vaccine efficacy", "eff"),
                ("hazard ratio", "eff"), ("HR", "eff"), ("risk ratio", "eff"), ("rate ratio", "eff"),
                ("adjusted", "eff"), ("propensity score", "eff"),
                ("did not effectively", "eff"), ("no effect", "eff"), ("not effective", "eff"),
                ("pooled", "eff")
            ]
        },
        "safety": {
           "saf": [
               ("safety", "saf"), ("adverse effects", "saf"), ("adverse events", "saf")
           ] 
        },
        "risk__factor": {
           "rf": [
               ("risk factor", "rf"), ("risk", "rf")
           ] 
        },
        "coverage": {
           "cov": [
               ("coverage", "cov"), ("uptake", "cov"), ("the uptake", "cov"),
               ("actual uptake", "cov"), ("vaccine uptake", "cov")
           ] 
        },
        "acceptance": {
           "kaa": [
               ("acceptance", "kaa"), ("Barrier", "kaa"), ("vaccine barriers", "kaa"), 
               ("knowledge", "kaa"), ("vaccination willingness and intentions", "kaa"), 
               ("HPV vaccine acceptability", "kaa"), 
               ("Awareness and knowledge", "kaa"), ("Awareness", "kaa"), 
               ("facilitators of and barriers", "kaa"), 
               ("awareness,knowledge, acceptability, and intention", "kaa"), 
               ("knowledge and acceptability", "kaa"), ("knowledge and awareness", "kaa"), 
               ("attitudes and beliefs", "kaa"), ("Knowledge and Attitude", "kaa"),  
               ("attitude", "kaa"), ("knowledge, awareness, and attitude", "kaa")
           ] 
        },
        "adm": {
           "adm": [
               ("administration", "adm"), ("vaccine types", "adm"), ("dose schedules", "adm"), 
               ("vaccine types and dose schedules", "adm"), ("different dose schedules", "adm"), 
               ("Two doses of", "adm")
           ] 
        },
        "eco": {
           "eco": [
                ("economic", "eco"), ("cost", "eco"), ("financial", "eco"), ("economic impact", "eco"),
                ("cost effectiveness", "eco"), ("cost-effectiveness", "eco"), 
                ("economic evaluation", "eco"), 
                ("Cost-effectiveness of HPV vaccination strategies", "eco")
           ] 
        },
        "modeling": {
           "mod": [
               ("modeling", "mod")
           ] 
        },
        "ethical__issues": {
           "eth": [
               ("racial", "eth"), ("ethnic", "eth"), 
               ("ethnic minority", "eth"), ("racial minority", "eth"), 
               ("racial/ethnic", "eth"), ("racial/ethnic minority", "eth"), 
               ("racial disparity", "eth"), ("ethnic disparity", "eth"), 
               ("minority", "eth"), ("minority population", "eth")
           ] 
        },
    },
    "outcome": {
        "infection": {
            "inf": [
                ("infection", "inf")
            ],
        },
        "hospital": {
            "hos": [
                ("hospitalization", "hos")
            ]
        },
        "icu": {
            "icu": [
                ("ICU", "icu"), ("intensive care unit", "icu"), ("intensive care", "icu")
            ]
        },
        "death": {
            "dea": [
                ("death", "dea"), ("mortality", "dea"), ("overall mortality", "dea"), 
                ("cancer related mortality", "dea"), ("on overall and cancer mortality", "dea")
            ],
        }
    },
    "intervention": {
        "vpd": {
            "covid": [
                ("COVID-19", "covid"), ("COVID", "covid"), ("COVID 19", "covid"), ("SARS-CoV-2", "covid")
            ],
            "aden": [
                ("Adenovirus", "aden")
            ],
            "anth": [
                ("anthrax", "anth")
            ],
            "camp": [
                ("Campylobacter", "camp")
            ],
            "chol": [
                ("Cholera", "chol"), ("Vibrio cholerae", "chol"),
                ("cholerae", "chol")
            ],
            "Q__fever": [
                ("Q fever", "coxi"), ("Coxiella burnetii", "coxi"),
                ("Q-fever", "coxi")
            ],
            "diph": [
                ("Diphtheria", "diph")
            ],
            "ebol": [
                ("Ebola", "ebol")
            ],
            "ente": [
                ("Enterovirus", "ente")
            ],
            "esch" : [
                ("Escherichia coli (ETEC)", "esch")
            ],
            "hib": [
                ("Haemophilus influenzae type b", "hib")
            ],
            "ha": [
                ("Hepatitis A", "ha")
            ],
            "hb": [
                ("Hepatitis B", "hb")
            ],
            "hc": [
                ("Hepatitis C", "hc")
            ],
            "he": [
                ("Hepatitis E", "he")
            ],
            "hs": [
                ("Herpes simplex", "hs")
            ],
            "hz": [
                ("Shingles, Herpes zoster", "hz")
            ],
            "hiv": [
                ("HIV / AIDS", "hiv"), ("HIV", "hiv"), ("AIDS", "hiv"),
                ("HIV/AIDS", "hiv")
            ],
            "hpv": [
                ("Human papillomavirus", "hpv"), ("HPV", "hpv")
            ],
            "je": [
                ("Japanese encephalitis", "je")
            ],
            "leis": [
                ("Leishmaniasis", "leis"), ("Leishmania", "leis")
            ],
            "lyme": [
                ("Lyme disease, Borreliosis", "lyme"), ("Borreliosis", "lyme"), 
                ("Lyme disease", "lyme")
            ],
            "mala": [
                ("Malaria", "mala"), ("Plasmodia", "mala")
            ],
            "meas": [
                ("Measles", "meas")
            ],
            "meni": [
                ("Meningococcal", "meni"), ("Neisseria", "meni")
            ],
            "mump": [
                ("Mumps", "mump")
            ],
            "leprosy": [
                ("Leprosy", "myle"), ("Mycobacterium leprae", "myle")
            ],
            "myva": [
                ("Mycobacterium vaccae", "myva")
            ],
            "pert": [
                ("Pertussis", "pert"), ("Bordetella", "pert")
            ],
            "plag": [
                ("Plague", "plag")
            ],
            "pneu": [
                ("Pneumococcal", "pneu")
            ],
            "poli": [
                ("Poliovirus", "poli"), ("Poliomyelitis", "poli")
            ],
            "pseu": [
                ("Pseudomonas aeruginosa", "pseu")
            ],
            "rabies": [
                ("Rabies", "rabi")
            ],
            "rsv": [
                ("Respiratory syncytial virus", "rsv"), ("rsv", "rsv")
            ],
            "rubella": [
                ("Rubella", "rube")
            ],
            "salm": [
                ("Salmonella", "salm")
            ],
            "shig": [
                ("Shigella", "shig")
            ],
            "smal": [
                ("Smallpox", "smal"), ("Variola", "smal")
            ],
            "strb": [
                ("Streptococcus group B", "strb")
            ],
            "tetanus": [
                ("Tetanus", "tt")
            ],
            "tbe": [
                ("Tick-borne encephalitis", "tbe")
            ],
            "tb": [
                ("Tuberculosis", "tb")
            ],
            "typh": [
                ("Typhoid", "typh")
            ],
            "vari": [
                ("Varicella, Chickenpox", "vari")
            ],
            "yf": [
                ("Yellow fever", "yf")
            ],
            "zika": [
                ("Zika", "zika")
            ],
            "infl": [
                ("influenza", "infl")
            ],
            "deng": [
                ("dengue", "deng")
            ],
            "rota": [
                ("rotavirus", "rota")
            ]
        },
        "vaccine__options": {
            "live": [
                ("live", "live")
            ],
            "non__live": [
                ("non-live", "nonlive")
            ],
            "adjuvants": [
                ("adjuvants", "adjuvants")
            ],
            "non__djuvanted": [
                ("non-adjuvanted", "nonadjuvants")
            ],
            "quad": [
                ("quadrivalent", "quad"), ("4vHPV", "4vHPV")
            ],
            "biva": [
                ("bivalent", "biva"), ("2vHPV", "2vHPV")
            ],
        },
    },
    "studies": {
        "studie__no": {
            "sty": [
                # === Study Inclusion Signals ===
                ("met inclusion criteria", "sty"),
                ("met eligibility criteria", "sty"),

                # === RCTs ===
                ("RCT", "rct"), ("RCTs", "rct"),

                # === NRSI ===
                ("observational study", "nrsi"), ("observational studies", "nrsi"),

                # === Mixed Methods ===
                ("mixed methods", "mmtd"), ("mixed-methods", "mmtd"),

                # === Qualitative ===
                ("qualitative study", "quanti"), ("qualitative studies", "quanti"),
                ("interviews", "quanti"), ("focus group", "quanti"), ("focus groups", "quanti"),
            ]
        },
    },
    "gender": {
        "group": {
            "sex": [
                ("male", "male"), ("female", "female"), ("divers", "divers"), 
                ("other", "other"), ("non-binary", "nonbinary"), 
                ("transgender", "transgender"), ("cisgender", "cisgender"),
                ("intersex", "intersex"), ("genderqueer", "genderqueer"), 
                ("genderfluid", "genderfluid"), ("prefer not to say", "pnts"), 
                ("unspecified", "unspecified"), ("unknown", "unknown"), ("girl", "girl"), 
                ("boy", "boy"), ("women", "women"), ("men", "men")
            ]
        }
    },
    "particip": {
        "group": {
            "members": [
                ("N - population", "npopu"), ("population size", "popus"), ("sample size", "samsi"), 
                ("number of participants", "numpa"), ("subjects", "subj"), ("study participants", "studyp"), 
                ("participant", "participant"), ("students", "students"), ("parents", "parents"), ("women", "women"),
                ("men", "men"), ("mothers", "mothers"), ("daughters", "daughters"), ("girls", "girls"), ("guardians", "guardians"), 
                ("adolescents", "adolescents"), ("young adult", "youngadult"), ("male", "male"), ("female", "female")
            ]
        }
    },
    "lit_search_dates": {
        "dates": {
            "dates": [
            ]
        }
    },
    "study_country": {
        "countries": {
            "countries": [
            ]
        }, 
        # "study_count": {
        #     "count": [
        #     ]
        # }
    },
    "title_popu": {
        "title_pop": {
            "title": [
                "Physician", "Nurse", "Surgeon", "Dentist", "Pharmacist", "Physical Therapist", 
                "Occupational Therapist", "Medical Laboratory Technologist", "Radiologist",
                "Dietitian/Nutritionist", "Respiratory Therapist","Speech-Language Pathologist", 
                "Physician Assistant", "Nurse Practitioner", "Certified Nursing Assistant (CNA)", 
                "Medical Assistant", "Paramedic/EMT", "Midwife", "Psychologist", "Social Worker (Clinical)",
                "Hospital Administrator", "Medical Researcher", "Health Educator", "Orthopedic Technician",
                "Optometrist", "Podiatrist", "Anesthesiologist", "Neurologist", "Cardiologist",
                "Gastroenterologist", "pregnant women", "pregnant", "traveller","parents","caregivers",
                "male", "female", "females and males", "female and male"
            ]
        }
    }
}