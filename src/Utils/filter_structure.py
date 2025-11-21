"""
filter_structure.py - Complete Filter Structure with Column Mappings
Each filter now includes a 'column' field indicating the database column to query
"""

FILTER_STRUCTURE = {
    "others": {
        "AMSTAR 2 Rating": {
            "column": "amstar_label",
            "values": ["High", "Moderate", "Low", "Critically Low"]
        },
        "Country": {
            "column": "country",
            "values": [
                "Argentina", "Australia", "Austria", "Bangladesh", "Belgium",
                "Bosnia and Herzegovina", "Brazil", "Bulgaria", "Canada", "Chile",
                "China", "Colombia", "Croatia", "Cuba", "Czech Republic", "Czechia",
                "Denmark", "Egypt", "England", "Estonia", "Ethiopia", "Finland",
                "France", "Germany", "Ghana", "Greece", "Hong Kong", "Hungary",
                "India", "Indonesia", "Iran", "Iran, Islamic Republic of", "Ireland",
                "Israel", "Italy", "Japan", "Kenya", "Korea (South)", "Kyrgyztan",
                "Lebanon", "Libyan Arab Jamahiriya", "Malaysia", "Mexico", "Nepal",
                "Netherlands", "New Zealand", "Nigeria", "North Macedonia", "Norway",
                "Oman", "Pakistan", "Peru", "Philippines", "Poland", "Portugal",
                "Puerto Rico", "Qatar", "Romania", "Russia (Federation)",
                "Russian Federation", "Saudi Arabia", "Scotland", "Serbia", "Singapore",
                "Slovakia", "Slovenia", "South Africa", "South Korea", "Spain", "Sweden",
                "Switzerland", "Taiwan (Republic of China)", "Tanzania", "Thailand",
                "Tunisia", "Turkey", "Uganda", "United Arab Emirates", "United Kingdom",
                "United States", "Venezuela", "Vietnam"
            ]
        },
        "Language": {
            "column": "language",
            "values": ["Chinese", "English", "French", "German", "Polish", "Portuguese", "Russian", "Spanish", "Turkish", "en"]
        },
        "Region": {
            "column": "study_country__hash__countries__hash__region",
            "values": ["Africa", "Americas", "Europe"]
        },
        "Year": {
            "column": "year",
            "values": [2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011]
        }
    },
    "tag_filters": {
        "intervention": {
            "column": "intervention",
            "vaccine__options": {
                "column": "vaccine_options",
                "adjuvants": {
                    "column": "intervention__hash__vaccine__options__hash__adjuvants",
                    "additional_context": "None",
                    "display": "adjuvants",
                    "synonyms": ["adjuvants:adjuvants", "adjuvants"]
                },
                "biva": {
                    "column": "intervention__hash__vaccine__options__hash__biva",
                    "additional_context": "None",
                    "display": "biva",
                    "synonyms": ["biva", "2vHPV:2vHPV", "bivalent:biva"]
                },
                "live": {
                    "column": "intervention__hash__vaccine__options__hash__live",
                    "additional_context": "None",
                    "display": "live",
                    "synonyms": ["live:live", "live"]
                },
                "non__djuvanted": {
                    "column": "intervention__hash__vaccine__options__hash__non_adjuvants",
                    "additional_context": "None",
                    "display": "non__djuvanted",
                    "synonyms": ["non-adjuvanted:nonadjuvants", "non__djuvanted"]
                },
                "quad": {
                    "column": "intervention__hash__vaccine__options__hash__quad",
                    "additional_context": "None",
                    "display": "quad",
                    "synonyms": ["4vHPV:4vHPV", "quad", "quadrivalent:quad"]
                }
            },
            "vpd": {
                "column": "vaccine_preventable_disease",
                "aden": {
                    "column": "intervention__hash__vpd__hash__aden",
                    "additional_context": "None",
                    "display": "aden",
                    "synonyms": ["Adenovirus:aden", "aden"]
                },
                "covid": {
                    "column": "intervention__hash__vpd__hash__cov",
                    "additional_context": "None",
                    "display": "covid",
                    "synonyms": ["covid", "SARS-CoV-2:covid", "COVID:covid", "COVID-19:covid", "COVID 19:covid"]
                },
                "deng": {
                    "column": "intervention__hash__vpd__hash__deng",
                    "additional_context": "None",
                    "display": "deng",
                    "synonyms": ["deng", "dengue:deng"]
                },
                "diph": {
                    "column": "intervention__hash__vpd__hash__diph",
                    "additional_context": "None",
                    "display": "diph",
                    "synonyms": ["diph", "Diphtheria:diph"]
                },
                "ente": {
                    "column": "intervention__hash__vpd__hash__ente",
                    "additional_context": "None",
                    "display": "ente",
                    "synonyms": ["ente", "Enterovirus:ente"]
                },
                "ha": {
                    "column": "intervention__hash__vpd__hash__ha",
                    "additional_context": "None",
                    "display": "ha",
                    "synonyms": ["ha", "Hepatitis A:ha"]
                },
                "hb": {
                    "column": "intervention__hash__vpd__hash__hb",
                    "additional_context": "None",
                    "display": "hb",
                    "synonyms": ["Hepatitis B:hb", "hb"]
                },
                "he": {
                    "column": "intervention__hash__vpd__hash__he",
                    "additional_context": "None",
                    "display": "he",
                    "synonyms": ["Hepatitis E:he", "he"]
                },
                "hiv": {
                    "column": "intervention__hash__vpd__hash__hiv",
                    "additional_context": "None",
                    "display": "hiv",
                    "synonyms": ["HIV / AIDS:hiv", "hiv", "AIDS:hiv", "HIV:hiv", "HIV/AIDS:hiv"]
                },
                "hpv": {
                    "column": "intervention__hash__vpd__hash__hpv",
                    "additional_context": "None",
                    "display": "hpv",
                    "synonyms": ["hpv", "Human papillomavirus:hpv", "HPV:hpv"]
                },
                "hs": {
                    "column": "intervention__hash__vpd__hash__hs",
                    "additional_context": "None",
                    "display": "hs",
                    "synonyms": ["Herpes simplex:hs", "hs"]
                },
                "infl": {
                    "column": "intervention__hash__vpd__hash__infl",
                    "additional_context": "None",
                    "display": "infl",
                    "synonyms": ["infl", "influenza:infl"]
                },
                "meas": {
                    "column": "intervention__hash__vpd__hash__meas",
                    "additional_context": "None",
                    "display": "meas",
                    "synonyms": ["meas", "Measles:meas"]
                },
                "meni": {
                    "column": "intervention__hash__vpd__hash__meni",
                    "additional_context": "None",
                    "display": "meni",
                    "synonyms": ["Meningococcal:meni", "meni", "Neisseria:meni"]
                },
                "pert": {
                    "column": "intervention__hash__vpd__hash__pert",
                    "additional_context": "None",
                    "display": "pert",
                    "synonyms": ["Bordetella:pert", "Pertussis:pert", "pert"]
                },
                "pneu": {
                    "column": "intervention__hash__vpd__hash__pneu",
                    "additional_context": "None",
                    "display": "pneu",
                    "synonyms": ["Pneumococcal:pneu", "pneu"]
                },
                "rota": {
                    "column": "intervention__hash__vpd__hash__rota",
                    "additional_context": "None",
                    "display": "rota",
                    "synonyms": ["rotavirus:rota", "rota"]
                },
                "rsv": {
                    "column": "intervention__hash__vpd__hash__rsv",
                    "additional_context": "None",
                    "display": "rsv",
                    "synonyms": ["rsv:rsv", "Respiratory syncytial virus:rsv", "rsv"]
                },
                "rubella": {
                    "column": "intervention__hash__vpd__hash__rube",
                    "additional_context": "None",
                    "display": "rubella",
                    "synonyms": ["rubella", "Rubella:rube"]
                },
                "salm": {
                    "column": "intervention__hash__vpd__hash__salm",
                    "additional_context": "None",
                    "display": "salm",
                    "synonyms": ["Salmonella:salm", "salm"]
                },
                "tbe": {
                    "column": "intervention__hash__vpd__hash__tbe",
                    "additional_context": "None",
                    "display": "tbe",
                    "synonyms": ["tbe", "Tick-borne encephalitis:tbe"]
                },
                "tetanus": {
                    "column": "intervention__hash__vpd__hash__tt",
                    "additional_context": "None",
                    "display": "tetanus",
                    "synonyms": ["tetanus", "Tetanus:tt"]
                }
            }
        },
        "lit_search_dates": {
            "column": "lit_search_dates",
            "dates": {
                "column": "lit_search_dates",
                "dates": {
                    "column": "lit_search_dates",
                    "additional_context": "None",
                    "display": "dates",
                    "synonyms": ["dates"]
                }
            }
        },
        "open_acc": {
            "column": "open_access",
            "opn_access": {
                "column": "open_access",
                "op_ac": {
                    "column": "open_access",
                    "additional_context": "None",
                    "display": "op_ac",
                    "synonyms": ["free full text:oa", "creative commons:oa", "open research:oa", "unrestricted access:oa", "op_ac", "open access:oa", "oa license:oa", "oa:oa", "fully open:oa", "free access:oa", "gold open access:oa"]
                }
            }
        },
        "outcome": {
            "column": "outcome",
            "death": {
                "column": "outcome_death",
                "dea": {
                    "column": "outcome__hash__death__hash__dea",
                    "additional_context": "None",
                    "display": "dea",
                    "synonyms": ["overall mortality:dea", "cancer related mortality:dea", "mortality:dea", "death:dea", "dea"]
                }
            },
            "hospital": {
                "column": "outcome_hospitalization",
                "hos": {
                    "column": "outcome__hash__hospital__hash__hos",
                    "additional_context": "None",
                    "display": "hos",
                    "synonyms": ["hos", "hospitalization:hos"]
                }
            },
            "icu": {
                "column": "outcome_icu",
                "icu": {
                    "column": "outcome__hash__icu__hash__icu",
                    "additional_context": "None",
                    "display": "icu",
                    "synonyms": ["intensive care:icu", "intensive care unit:icu", "icu", "ICU:icu"]
                }
            },
            "infection": {
                "column": "outcome_infection",
                "inf": {
                    "column": "outcome__hash__infection__hash__inf",
                    "additional_context": "None",
                    "display": "inf",
                    "synonyms": ["inf", "infection:inf"]
                }
            }
        },
        "particip": {
            "column": "participants",
            "group": {
                "column": "participant_group",
                "members": {
                    "column": "participant_group",
                    "additional_context": "None",
                    "display": "members",
                    "synonyms": ["female:female", "study participants:studyp", "subjects:subj", "girls:girls", "number of participants:numpa", "students:students", "women:women", "mothers:mothers", "members", "adolescents:adolescents", "participant:participant", "male:male", "daughters:daughters", "parents:parents", "men:men", "N - population:npopu", "population size:popus", "sample size:samsi", "young adult:youngadult", "guardians:guardians"]
                }
            }
        },
        "popu": {
            "column": "population",
            "age__group": {
                "column": "age_group",
                "nb_0__1": {
                    "column": "popu__hash__age__group__hash__nb_0__1",
                    "additional_context": "None",
                    "display": "nb_0__1",
                    "synonyms": ["infant:nb", "toddlers:nb", "youngsters:nb", "nb_0__1", "young ones:nb", "babies:nb", "small children:nb", "newborn:nb", "baby:nb"]
                },
                "chi_2__9": {
                    "column": "popu__hash__age__group__hash__chi_2__9",
                    "additional_context": "None",
                    "display": "chi_2__9",
                    "synonyms": ["children:chi", "chi_2__9", "child:chi"]
                },
                "ado_10__17": {
                    "column": "popu__hash__age__group__hash__ado_10__17",
                    "additional_context": "None",
                    "display": "ado_10__17",
                    "synonyms": ["adolescent:ado", "young adults:ado", "ado_10__17", "adolescents:ado"]
                },
                "adu_18__64": {
                    "column": "popu__hash__age__group__hash__adu_18__64",
                    "additional_context": "None",
                    "display": "adu_18__64",
                    "synonyms": ["adu_18__64", "adult:adu", "adults:adu"]
                },
                "eld_65__10000": {
                    "column": "popu__hash__age__group__hash__eld_65__10000",
                    "additional_context": "None",
                    "display": "eld_65__10000",
                    "synonyms": ["eld_65__10000", "older adults:eld", "elderly:eld"]
                }
            },
            "immune__status": {
                "column": "immune_status",
                "hty": {
                    "column": "popu__hash__immune__status__hash__hty",
                    "additional_context": "None",
                    "display": "hty",
                    "synonyms": ["healthy:hty", "hty"]
                },
                "imu": {
                    "column": "popu__hash__immune__status__hash__imu",
                    "additional_context": "None",
                    "display": "imu",
                    "synonyms": ["immunocompromised:imu", "imu"]
                }
            },
            "specific__group": {
                "column": "specific_group",
                "hcw": {
                    "column": "popu__hash__specific__group__hash__hcw",
                    "additional_context": "None",
                    "display": "hcw",
                    "synonyms": ["Gastroenterologist:hcw", "Neurologist:hcw", "Certified Nursing Assistant (CNA):hcw", "Dentist:hcw", "Physician:hcw", "hcw"]
                },
                "pcg": {
                    "column": "popu__hash__specific__group__hash__cg",
                    "additional_context": "None",
                    "display": "pcg",
                    "synonyms": ["pcg", "parents:cg", "caregivers:cg"]
                },
                "pw": {
                    "column": "popu__hash__specific__group__hash__pw",
                    "additional_context": "None",
                    "display": "pw",
                    "synonyms": ["pregnant:pw", "pregnant women:pw", "pw"]
                },
                "tra": {
                    "column": "popu__hash__specific__group__hash__tra",
                    "additional_context": "None",
                    "display": "tra",
                    "synonyms": ["traveller:tra"]
                }
            }
        },
        "reviews": {
            "column": "review_type",
            "review": {
                "column": "review_type",
                "rev": {
                    "column": "review_type",
                    "additional_context": "None",
                    "display": "rev",
                    "synonyms": ["Critical Review:rev", "Meta-Analysis:rev", "Book Review:rev", "Literature Review:rev", "Peer Review:rev", "rev", "Review Article:rev", "systematic review:rev", "Editorial Review:rev", "review:rev"]
                }
            }
        },
        "studies": {
            "column": "study_type",
            "studie__no": {
                "column": "study_type",
                "sty": {
                    "column": "study_type",
                    "additional_context": "None",
                    "display": "sty",
                    "synonyms": ["natural experiment:nrsi", "mixed methods:mmtd", "qualitative study:quanti", "double-blind study:rct", "studies:sty", "cohort-study:nrsi", "mixed-methods:mmtd", "quasi-experimental:nrsi", "NRSI:nrsi", "randomized trial:rct", "cohort study:nrsi", "non-randomized controlled study:nrsi", "randomised controlled trial:rct", "randomised trial:rct", "RCT:rct"]
                }
            }
        },
        "study_country": {
            "column": "study_country",
            "countries": {
                "column": "study_country",
                "countries": {
                    "column": "study_country",
                    "additional_context": "None",
                    "display": "countries",
                    "synonyms": ["countries"]
                }
            },
            "study_count": {
                "column": "study_count",
                "count": {
                    "column": "study_count",
                    "additional_context": "None",
                    "display": "count",
                    "synonyms": ["count"]
                }
            }
        },
        "title_popu": {
            "column": "title_population",
            "title_pop": {
                "column": "title_population",
                "title": {
                    "column": "title_population",
                    "additional_context": "None",
                    "display": "title",
                    "synonyms": ["title"]
                }
            }
        },
        "topic": {
            "column": "topic",
            "acceptance": {
                "column": "topic_acceptance",
                "kaa": {
                    "column": "topic__hash__acceptance__hash__kaa",
                    "additional_context": "None",
                    "display": "kaa",
                    "synonyms": ["attitudes and beliefs:kaa", "awareness,knowledge, acceptability, and intention:kaa", "Knowledge and Attitude:kaa", "vaccine barriers:kaa", "knowledge, awareness, and attitude:kaa", "attitude:kaa", "facilitators of and barriers:kaa", "kaa", "vaccination willingness and intentions:kaa", "knowledge and acceptability:kaa", "knowledge:kaa", "Awareness:kaa", "HPV vaccine acceptability:kaa", "acceptance:kaa", "knowledge and awareness:kaa", "Awareness and knowledge:kaa", "Barrier:kaa"]
                }
            },
            "adm": {
                "column": "topic_administration",
                "adm": {
                    "column": "topic__hash__adm__hash__adm",
                    "additional_context": "None",
                    "display": "adm",
                    "synonyms": ["dose schedules:adm", "vaccine types and dose schedules:adm", "Two doses of:adm", "administration:adm", "vaccine types:adm", "different dose schedules:adm", "adm"]
                }
            },
            "coverage": {
                "column": "topic_coverage",
                "cov": {
                    "column": "topic__hash__coverage__hash__cov",
                    "additional_context": "None",
                    "display": "cov",
                    "synonyms": ["vaccine uptake:cov", "uptake:cov", "cov", "the uptake:cov", "coverage:cov", "actual uptake:cov"]
                }
            },
            "eco": {
                "column": "topic_economic",
                "eco": {
                    "column": "topic__hash__eco__hash__eco",
                    "additional_context": "None",
                    "display": "eco",
                    "synonyms": ["cost-effectiveness:eco", "Cost-effectiveness of HPV vaccination strategies:eco", "eco", "economic:eco", "economic evaluation:eco", "cost:eco", "financial:eco", "cost effectiveness:eco", "economic impact:eco"]
                }
            },
            "ethical__issues": {
                "column": "topic_ethical",
                "eth": {
                    "column": "topic__hash__ethical__issues__hash__eth",
                    "additional_context": "None",
                    "display": "eth",
                    "synonyms": ["eth", "racial:eth", "ethnic:eth", "racial/ethnic:eth", "racial disparity:eth", "ethnic disparity:eth", "racial/ethnic minority:eth", "minority population:eth", "minority:eth", "ethnic minority:eth", "racial minority:eth"]
                }
            },
            "modeling": {
                "column": "topic_modeling",
                "mod": {
                    "column": "topic__hash__mod__hash__mod",
                    "additional_context": "None",
                    "display": "mod",
                    "synonyms": ["mod", "modeling:mod"]
                }
            },
            "risk__factor": {
                "column": "topic_risk_factor",
                "rf": {
                    "column": "topic__hash__risk__factor__hash__rf",
                    "additional_context": "None",
                    "display": "rf",
                    "synonyms": ["risk factor:rf", "rf", "risk:rf"]
                }
            },
            "safety": {
                "column": "topic_safety",
                "saf": {
                    "column": "topic__hash__safety__hash__saf",
                    "additional_context": "None",
                    "display": "saf",
                    "synonyms": ["saf", "adverse events:saf", "adverse effects:saf", "safety:saf"]
                }
            }
        }
    }
}