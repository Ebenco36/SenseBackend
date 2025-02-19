import json


class APIResponseNormalizer:
    """
    A class to normalize API responses, ensuring that missing keys
    are filled with default values from a reference dataset.
    """

    def __init__(self):
        """
        Initializes the APIResponseNormalizer with reference data.

        :param reference_data: The full expected JSON structure.
        """
        self.reference_data = {
            "popu": {
                "age__group": {
                    "adu_18__64": {
                        "display": "adu_18__64",
                        "synonyms": ["adults:adu", "adult:adu", "adu_18__64"],
                        "additional_context": "None",
                    },
                    "eld_65__10000": {
                        "display": "eld_65__10000",
                        "synonyms": [
                            "elderly:eld",
                            "older adults:eld",
                            "eld_65__10000",
                        ],
                        "additional_context": "None",
                    },
                    "chi_2__9": {
                        "display": "chi_2__9",
                        "synonyms": ["child:chi", "children:chi", "chi_2__9"],
                        "additional_context": "None",
                    },
                    "nb_0__1": {
                        "display": "nb_0__1",
                        "synonyms": [
                            "newborn:nb",
                            "babies:nb",
                            "baby:nb",
                            "infant:nb",
                            "toddlers:nb",
                            "young ones:nb",
                            "youngsters:nb",
                            "small children:nb",
                            "nb_0__1",
                        ],
                        "additional_context": "None",
                    },
                    "ado_10__17": {
                        "display": "ado_10__17",
                        "synonyms": [
                            "adolescents:ado",
                            "adolescent:ado",
                            "young adults:ado",
                            "ado_10__17",
                        ],
                        "additional_context": "None",
                    },
                },
                "immune__status": {
                    "hty": {
                        "display": "hty",
                        "synonyms": ["healthy:hty", "hty"],
                        "additional_context": "None",
                    },
                    "imu": {
                        "display": "imu",
                        "synonyms": ["immunocompromised:imu", "imu"],
                        "additional_context": "None",
                    },
                },
                "specific__group": {
                    "pcg": {
                        "display": "pcg",
                        "synonyms": ["parents:cg", "caregivers:cg", "pcg"],
                        "additional_context": "None",
                    },
                    "pw": {
                        "display": "pw",
                        "synonyms": ["pregnant:pw", "pregnant women:pw", "pw"],
                        "additional_context": "None",
                    },
                    "tra": {
                        "display": "tra",
                        "synonyms": ["traveller:tra"],
                        "additional_context": "None",
                    },
                    "hcw": {
                        "display": "hcw",
                        "synonyms": [
                            "Physician:hcw",
                            "Nurse:hcw",
                            "Surgeon:hcw",
                            "Dentist:hcw",
                            "Pharmacist:hcw",
                            "Physical Therapist:hcw",
                            "Occupational Therapist:hcw",
                            "Medical Laboratory Technologist:hcw",
                            "Radiologist:hcw",
                            "Dietitian/Nutritionist:hcw",
                            "Respiratory Therapist:hcw",
                            "Speech-Language Pathologist:hcw",
                            "Physician Assistant:hcw",
                            "Nurse Practitioner:hcw",
                            "Certified Nursing Assistant (CNA):hcw",
                            "Medical Assistant:hcw",
                            "Paramedic/EMT:hcw",
                            "Midwife:hcw",
                            "Psychologist:hcw",
                            "Social Worker (Clinical):hcw",
                            "Hospital Administrator:hcw",
                            "Medical Researcher:hcw",
                            "Health Educator:hcw",
                            "Orthopedic Technician:hcw",
                            "Optometrist:hcw",
                            "Podiatrist:hcw",
                            "Anesthesiologist:hcw",
                            "Neurologist:hcw",
                            "Cardiologist:hcw",
                            "Gastroenterologist:hcw",
                            "hcw",
                        ],
                        "additional_context": "None",
                    },
                },
            },
            "topic": {
                "risk__factor": {
                    "rf": {
                        "display": "rf",
                        "synonyms": ["risk factor:rf", "risk:rf", "rf"],
                        "additional_context": "None",
                    }
                },
                "adm": {
                    "adm": {
                        "display": "adm",
                        "synonyms": [
                            "administration:adm",
                            "vaccine types:adm",
                            "dose schedules:adm",
                            "vaccine types and dose schedules:adm",
                            "different dose schedules:adm",
                            "Two doses of:adm",
                            "adm",
                        ],
                        "additional_context": "None",
                    }
                },
                "coverage": {
                    "cov": {
                        "display": "cov",
                        "synonyms": [
                            "coverage:cov",
                            "uptake:cov",
                            "the uptake:cov",
                            "actual uptake:cov",
                            "vaccine uptake:cov",
                            "cov",
                        ],
                        "additional_context": "None",
                    }
                },
                "acceptance": {
                    "kaa": {
                        "display": "kaa",
                        "synonyms": [
                            "acceptance:kaa",
                            "Barrier:kaa",
                            "vaccine barriers:kaa",
                            "knowledge:kaa",
                            "vaccination willingness and intentions:kaa",
                            "HPV vaccine acceptability:kaa",
                            "Awareness and knowledge:kaa",
                            "Awareness:kaa",
                            "facilitators of and barriers:kaa",
                            "awareness,knowledge, acceptability, and intention:kaa",
                            "knowledge and acceptability:kaa",
                            "knowledge and awareness:kaa",
                            "attitudes and beliefs:kaa",
                            "Knowledge and Attitude:kaa",
                            "attitude:kaa",
                            "knowledge, awareness, and attitude:kaa",
                            "kaa",
                        ],
                        "additional_context": "None",
                    }
                },
                "eco": {
                    "eco": {
                        "display": "eco",
                        "synonyms": [
                            "economic:eco",
                            "cost:eco",
                            "financial:eco",
                            "economic impact:eco",
                            "cost effectiveness:eco",
                            "cost-effectiveness:eco",
                            "economic evaluation:eco",
                            "Cost-effectiveness of HPV vaccination strategies:eco",
                            "eco",
                        ],
                        "additional_context": "None",
                    }
                },
                "safety": {
                    "saf": {
                        "display": "saf",
                        "synonyms": [
                            "safety:saf",
                            "adverse effects:saf",
                            "adverse events:saf",
                            "saf",
                        ],
                        "additional_context": "None",
                    }
                },
                "ethical__issues": {
                    "eth": {
                        "display": "eth",
                        "synonyms": [
                            "racial:eth",
                            "ethnic:eth",
                            "ethnic minority:eth",
                            "racial minority:eth",
                            "racial/ethnic:eth",
                            "racial/ethnic minority:eth",
                            "racial disparity:eth",
                            "ethnic disparity:eth",
                            "minority:eth",
                            "minority population:eth",
                            "eth",
                        ],
                        "additional_context": "None",
                    }
                },
                "modeling": {
                    "mod": {
                        "display": "mod",
                        "synonyms": ["modeling:mod", "mod"],
                        "additional_context": "None",
                    }
                },
            },
            "outcome": {
                "infection": {
                    "inf": {
                        "display": "inf",
                        "synonyms": ["infection:inf", "inf"],
                        "additional_context": "None",
                    }
                },
                "death": {
                    "dea": {
                        "display": "dea",
                        "synonyms": [
                            "death:dea",
                            "mortality:dea",
                            "overall mortality:dea",
                            "cancer related mortality:dea",
                            "on overall and cancer mortality:dea",
                            "dea",
                        ],
                        "additional_context": "None",
                    }
                },
                "hospital": {
                    "hos": {
                        "display": "hos",
                        "synonyms": ["hospitalization:hos", "hos"],
                        "additional_context": "None",
                    }
                },
                "icu": {
                    "icu": {
                        "display": "icu",
                        "synonyms": [
                            "ICU:icu",
                            "intensive care unit:icu",
                            "intensive care:icu",
                            "icu",
                        ],
                        "additional_context": "None",
                    }
                }
            },
            "reviews": {
                "review": {
                    "rev": {
                        "display": "rev",
                        "synonyms": [
                            "systematic review:rev",
                            "Literature Review:rev",
                            "review:rev",
                            "Meta-Analysis:rev",
                            "Critical Review:rev",
                            "Peer Review:rev",
                            "Book Review:rev",
                            "Editorial Review:rev",
                            "Review Article:rev",
                            "rev",
                        ],
                        "additional_context": "None",
                    }
                }
            },
            "studies": {
                "studie__no": {
                    "sty": {
                        "display": "sty",
                        "synonyms": [
                            "study:sty",
                            "studies:sty",
                            "RCT:rct",
                            "randomized controlled trial:rct",
                            "randomised controlled trial:rct",
                            "randomized trial:rct",
                            "randomised trial:rct",
                            "clinical trial:rct",
                            "double-blind study:rct",
                            "placebo-controlled:rct",
                            "randomised comparative:rct",
                            "NRSI:nrsi",
                            "non-randomized studies of interventions:nrsi",
                            "observational studies:nrsi",
                            "quasi-experimental:nrsi",
                            "non-randomized controlled study:nrsi",
                            "non-randomised studies:nrsi",
                            "natural experiment:nrsi",
                            "test-negative designs:nrsi",
                            "cross-sectional:nrsi",
                            "cross sectional:nrsi",
                            "controlled clinical:nrsi",
                            "cohort study:nrsi",
                            "cohort-study:nrsi",
                            "prospective study:nrsi",
                            "retrospective study:nrsi",
                            "longitudinal study:nrsi",
                            "case-control study:nrsi",
                            "pre-post studies:nrsi",
                            "interrupted time series:nrsi",
                            "case reports:nrsi",
                            "case series:nrsi",
                            "mixed methods:mmtd",
                            "mixed-methods:mmtd",
                            "convergent design:mmtd",
                            "explanatory sequential design:mmtd",
                            "qualitative study:quanti",
                            "sty",
                        ],
                        "additional_context": "None",
                    }
                }
            },
            "intervention": {
                "vpd": {
                    "infl": {
                        "display": "infl",
                        "synonyms": ["influenza:infl", "infl"],
                        "additional_context": "None",
                    },
                    "pneu": {
                        "display": "pneu",
                        "synonyms": ["Pneumococcal:pneu", "pneu"],
                        "additional_context": "None",
                    },
                    "hiv": {
                        "display": "hiv",
                        "synonyms": [
                            "HIV / AIDS:hiv",
                            "HIV:hiv",
                            "AIDS:hiv",
                            "HIV/AIDS:hiv",
                            "hiv",
                        ],
                        "additional_context": "None",
                    },
                    "rsv": {
                        "display": "rsv",
                        "synonyms": [
                            "Respiratory syncytial virus:rsv",
                            "rsv:rsv",
                            "rsv",
                        ],
                        "additional_context": "None",
                    },
                    "covid": {
                        "display": "covid",
                        "synonyms": [
                            "COVID-19:covid",
                            "COVID:covid",
                            "COVID 19:covid",
                            "SARS-CoV-2:covid",
                            "covid",
                        ],
                        "additional_context": "None",
                    },
                    "meni": {
                        "display": "meni",
                        "synonyms": ["Meningococcal:meni", "Neisseria:meni", "meni"],
                        "additional_context": "None",
                    },
                    "hpv": {
                        "display": "hpv",
                        "synonyms": ["Human papillomavirus:hpv", "HPV:hpv", "hpv"],
                        "additional_context": "None",
                    },
                    "hb": {
                        "display": "hb",
                        "synonyms": ["Hepatitis B:hb", "hb"],
                        "additional_context": "None",
                    },
                    "meas": {
                        "display": "meas",
                        "synonyms": ["Measles:meas", "meas"],
                        "additional_context": "None",
                    },
                    "pert": {
                        "display": "pert",
                        "synonyms": ["Pertussis:pert", "Bordetella:pert", "pert"],
                        "additional_context": "None",
                    },
                    "tetanus": {
                        "display": "tetanus",
                        "synonyms": ["Tetanus:tt", "tetanus"],
                        "additional_context": "None",
                    },
                    "ha": {
                        "display": "ha",
                        "synonyms": ["Hepatitis A:ha", "ha"],
                        "additional_context": "None",
                    },
                    "he": {
                        "display": "he",
                        "synonyms": ["Hepatitis E:he", "he"],
                        "additional_context": "None",
                    },
                    "rubella": {
                        "display": "rubella",
                        "synonyms": ["Rubella:rube", "rubella"],
                        "additional_context": "None",
                    },
                    "aden": {
                        "display": "aden",
                        "synonyms": ["Adenovirus:aden", "aden"],
                        "additional_context": "None",
                    },
                    "ente": {
                        "display": "ente",
                        "synonyms": ["Enterovirus:ente", "ente"],
                        "additional_context": "None",
                    },
                    "hs": {
                        "display": "hs",
                        "synonyms": ["Herpes simplex:hs", "hs"],
                        "additional_context": "None",
                    },
                    "salm": {
                        "display": "salm",
                        "synonyms": ["Salmonella:salm", "salm"],
                        "additional_context": "None",
                    },
                    "rota": {
                        "display": "rota",
                        "synonyms": ["rotavirus:rota", "rota"],
                        "additional_context": "None",
                    },
                    "tbe": {
                        "display": "tbe",
                        "synonyms": ["Tick-borne encephalitis:tbe", "tbe"],
                        "additional_context": "None",
                    },
                    "deng": {
                        "display": "deng",
                        "synonyms": ["dengue:deng", "deng"],
                        "additional_context": "None",
                    },
                    "diph": {
                        "display": "diph",
                        "synonyms": ["Diphtheria:diph", "diph"],
                        "additional_context": "None",
                    },
                },
                "vaccine__options": {
                    "live": {
                        "display": "live",
                        "synonyms": ["live:live", "live"],
                        "additional_context": "None",
                    },
                    "non__djuvanted": {
                        "display": "non__djuvanted",
                        "synonyms": ["non-adjuvanted:nonadjuvants", "non__djuvanted"],
                        "additional_context": "None",
                    },
                    "adjuvants": {
                        "display": "adjuvants",
                        "synonyms": ["adjuvants:adjuvants", "adjuvants"],
                        "additional_context": "None",
                    },
                    "quad": {
                        "display": "quad",
                        "synonyms": ["quadrivalent:quad", "4vHPV:4vHPV", "quad"],
                        "additional_context": "None",
                    },
                    "biva": {
                        "display": "biva",
                        "synonyms": ["bivalent:biva", "2vHPV:2vHPV", "biva"],
                        "additional_context": "None",
                    },
                },
            },
            "particip": {
                "group": {
                    "members": {
                        "display": "members",
                        "synonyms": [
                            "N - population:npopu",
                            "population size:popus",
                            "sample size:samsi",
                            "number of participants:numpa",
                            "subjects:subj",
                            "study participants:studyp",
                            "participant:participant",
                            "students:students",
                            "parents:parents",
                            "women:women",
                            "men:men",
                            "mothers:mothers",
                            "daughters:daughters",
                            "girls:girls",
                            "guardians:guardians",
                            "adolescents:adolescents",
                            "young adult:youngadult",
                            "male:male",
                            "female:female",
                            "members",
                        ],
                        "additional_context": "None",
                    }
                }
            },
            "open_acc": {
                "opn_access": {
                    "op_ac": {
                        "display": "op_ac",
                        "synonyms": [
                            "open access:oa",
                            "oa:oa",
                            "free access:oa",
                            "gold open access:oa",
                            "creative commons:oa",
                            "open research:oa",
                            "fully open:oa",
                            "free full text:oa",
                            "oa license:oa",
                            "unrestricted access:oa",
                            "op_ac",
                        ],
                        "additional_context": "None",
                    }
                }
            },
            "study_country": {
                "countries": {
                    "countries": {
                        "display": "countries",
                        "synonyms": ["countries"],
                        "additional_context": "None",
                    }
                },
                "study_count": {
                    "count": {
                        "display": "count",
                        "synonyms": ["count"],
                        "additional_context": "None",
                    }
                },
            },
            "title_popu": {
                "title_pop": {
                    "title": {
                        "display": "title",
                        "synonyms": ["title"],
                        "additional_context": "None",
                    }
                }
            },
            "lit_search_dates": {
                "dates": {
                    "dates": {
                        "display": "dates",
                        "synonyms": ["dates"],
                        "additional_context": "None",
                    }
                }
            },
        }

    def normalize_response(self, response_data):
        """
        Ensures the response data has all required fields from reference_data.
        
        :param response_data: The actual API response (may have missing keys).
        :return: A fully structured response with missing fields and synonyms added.
        """
        return self._update_missing(response_data.copy(), self.reference_data)

    def _update_missing(self, response_part, reference_part):
        """
        Recursively updates missing fields in the response with reference data.
        
        :param response_part: A part of the API response (dict).
        :param reference_part: The corresponding part in reference_data.
        :return: Updated response part.
        """
        updated_part = response_part.copy() if isinstance(response_part, dict) else response_part

        if isinstance(reference_part, dict):
            for key, value in reference_part.items():
                if key not in updated_part:
                    updated_part[key] = self._convert_to_serializable(value)  # Fill missing key
                elif isinstance(value, dict):
                    updated_part[key] = self._update_missing(updated_part[key], value)
                elif key == "synonyms" and isinstance(value, list):
                    # Merge and deduplicate synonyms
                    updated_part[key] = self._merge_and_deduplicate_lists(updated_part.get(key, []), value)

        return updated_part

    def _convert_to_serializable(self, value):
        """
        Converts sets to lists and deduplicates lists for JSON serialization.
        
        :param value: The value to be converted.
        :return: Converted value (if it's a set, returns a list).
        """
        if isinstance(value, set):
            return list(value)  # Convert set to list
        elif isinstance(value, list):
            return list(set(value))  # Remove duplicates from lists
        elif isinstance(value, dict):
            return {k: self._convert_to_serializable(v) for k, v in value.items()}
        return value

    def _merge_and_deduplicate_lists(self, original_list, reference_list):
        """
        Merges two lists and removes duplicates.
        
        :param original_list: The list from the API response.
        :param reference_list: The list from the reference data.
        :return: A merged and deduplicated list.
        """
        merged_list = list(set(original_list + reference_list))
        return merged_list
