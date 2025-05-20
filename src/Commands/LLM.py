from typing import Dict, Optional
from transformers import pipeline, AutoModelForQuestionAnswering, AutoTokenizer
import re
import json
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

class MyExtractor:
    def __init__(self, qa_pipeline, database_list: List[str], score_threshold: float = 0.1):
        self.qa_pipeline = qa_pipeline
        self.database_list = database_list
        self.score_threshold = score_threshold

    def extract_database_names(self, text: str) -> list:
        # Known common aliases
        aliases = {
            "MEDLINE": ["MEDLINE via Ovid", "MEDLINE/PubMed"],
            "CENTRAL": ["Cochrane Central Register of Controlled Trials"],
            "Cochrane": ["Cochrane Library"],
            "PubMed": ["PubMed Central"],
            "PsycINFO": ["Psychological Abstracts"]
        }

        # Normalize alias map: reverse mapping from alias to canonical name
        alias_map = {alias: canonical for canonical, alias_list in aliases.items() for alias in alias_list}
        all_variants = set(self.database_list + list(alias_map.keys()))

        # Regex: match whole words or phrase boundaries (e.g., "PubMed", "Cochrane Library")
        pattern = re.compile(r'\b(?:' + '|'.join(re.escape(db) for db in all_variants) + r')\b', re.IGNORECASE)

        matches = pattern.findall(text)
        matched_databases = set()

        for match in matches:
            # Normalize via alias mapping if needed
            canonical = alias_map.get(match, match)
            # Title-case everything for consistency
            matched_databases.add(canonical.title())

        return sorted(matched_databases)
    
    def extract_all(self, title: str, main_content: str, abstract: str,
                    methods: str, results: str, document: str
                   ) -> Dict[str, Any]:
        # 1) Build contexts
        contexts = {
            "last_search_date": main_content,
            "population_count": document,
            "country_mentions": results,
            "ve_info": " ".join([abstract, methods, results]),
            "location_in_title": title,
            "race_ethnicity_in_title": title,
            "target_population_in_title": title,
            "topic_in_title": title,
            "num_databases": document,
            "duration_of_intervention": document,
            "dosage": document,
            "comparator": document,
            "country_participant_counts": document,
            "review_type": document,
            # bias/funding share one context
            "bias_and_funding": document,

            "total_studies": document,
            "rct": document,
            "nrsi": document,
            "mixed_methods": document,
            "qualitative": document,
        }

        # 2) Define all questions
        questions_map: Dict[str, List[str]] = {
            "last_search_date": [
                "When was the last literature search conducted?",
                "Until what date were studies retrieved?",
                "What was the final search date?",
                "When was the last literature search conducted?"
                "What was the date of the final search?",
                "On what date did the authors perform the final database search?",
                "Until what date were studies retrieved?",
                "Up to which date does the search cover?",
                "When was the database search last run?",
                "What is the date of the last automatic search?",
                "When did the literature search conclude?",
                "When was the search strategy most recently updated?",
                "What is the date of the final update to the search?",
                "On what date were the retrievals implemented?",
                "When was the PubMed search last performed?",
                "When was the EMBASE search last carried out?",
                "When was the Cochrane Database of Systematic Reviews search up to?",
                "On what date was the systematic search completed?",
            ],
            "population_count": [
                "What is the total number of participants?",
                "How many people were included in the study?",
                "What is the population size?"
            ],
            "country_mentions": [
                "Which countries were included in the study?",
                "What countries were mentioned in the paper?",
                "Which nations were studied?"
            ],
            "ve_info": [
                "What is the reported vaccine effectiveness?",
                "What is the efficacy of the vaccine?",
                "What are the confidence intervals for vaccine effectiveness?"
            ],
            "location_in_title": ["Which country is mentioned in the title?"],
            "race_ethnicity_in_title": ["What race or ethnicity is mentioned in the title?"],
            "target_population_in_title": ["What population is targeted in the title?"],
            "topic_in_title": ["What is the main topic of the article title?"],
            "num_databases": [
                "How many databases were searched?",
                "What is the total number of databases included in the literature search?",
                "How many electronic sources were searched?"
            ],
            "duration_of_intervention": ["What was the duration of the intervention?"],
            "dosage": ["What was the dosage used in the intervention?"],
            "comparator": ["What comparator was used in the study?"],
            "country_participant_counts": ["How many participants were included from each country?"],
            "review_type": [
                "What type of review is this paper?",
                "What kind of review was conducted?",
                "Is this a systematic review, meta-analysis, or another type of review?",
                "What review methodology was used in the study?"
            ],
            "bias_and_funding": [
                "Was the risk of bias in individual studies assessed?",
                "Did the authors evaluate risk of bias for the included studies?",
                "Was any method used to assess study bias?",
                "Was the risk of bias considered when interpreting the results?",
                "Did the authors take bias into account when discussing the findings?",
                "Was bias mentioned in interpretation of the study findings?",
                "Did the review disclose sources of funding for the included studies?",
                "Was funding information reported in the systematic review?",
                "Was study funding declared in the paper? If information does not exist say funding was not disclosed."
            ],
            "total_studies": [
                # general
                "How many studies were included in the review?",
                "What is the total number of studies analyzed?",
                # inclusion-signal variants
                "How many studies met inclusion criteria?",
                "How many records were included in the analysis?",
                "How many studies were deemed relevant?",
                "How many studies were considered eligible?",
                "How many studies yielded data from the search?",
                "How many studies, leaving a total of, were included?",
            ],
            "rct": [
                "How many randomized controlled trials (RCTs) were included?",
                "What is the count of RCTs in the paper?",
                # synonyms
                "How many randomized trials were analyzed?",
                "What is the number of placebo-controlled or double-blind studies?",
                "How many controlled clinical trials were part of the review?",
                "How many multicenter or randomized comparative trials were included?",
            ],
            "nrsi": [
                "How many non-randomized studies of interventions (NRSIs) were reported?",
                "How many observational studies were included?",
                # specific designs
                "What is the count of cohort and case-control studies?",
                "How many retrospective or prospective studies were analyzed?",
                "How many interrupted time-series, cross-sectional or quasi-experimental studies appeared?",
                "How many case series or case reports were included?",
            ],
            "mixed_methods": [
                "How many mixed-methods studies were included?",
                "What is the number of convergent design studies?",
                "How many explanatory sequential design studies were part of the review?",
                "How many studies used a mixed methods approach?",
            ],
            "qualitative": [
                "How many qualitative studies were included?",
                "What is the count of focus group or interview-based studies?",
                "How many studies assessed data via interviews or focus groups?",
            ]
        }

        # 3) Flatten into one batch
        batch: List[Dict[str, str]] = []
        field_seq: List[str] = []
        for field, qs in questions_map.items():
            for q in qs:
                batch.append({"question": q, "context": contexts[field]})
                field_seq.append(field)

        # 4) Single pipeline call
        raw_answers = self.qa_pipeline(batch)

        # 5) Group by field and filter by score
        answers_by_field: Dict[str, List[str]] = defaultdict(list)
        for field, out in zip(field_seq, raw_answers):
            if out and out.get("score", 0) > self.score_threshold and out.get("answer"):
                answers_by_field[field].append(out["answer"].strip())

        # 6) Post-process each field
        results: Dict[str, Any] = {}

        # === New: extract counts for added fields ===
        def extract_count(field_name: str) -> Optional[int]:
            for ans in answers_by_field.get(field_name, []):
                m = re.search(r"\d+", ans)
                if m:
                    return int(m.group())
            return None

        for fld in ["total_studies", "rct", "nrsi", "mixed_methods", "qualitative"]:
            results[fld] = extract_count(fld)
        # === End of new fields ===
        # rct, nrsi, mixed_methods, qualitative
        # last_search_date
        date_pat = re.compile(r"\b([A-Z][a-z]+)\s+\d{4}\b")
        for ans in answers_by_field.get("last_search_date", []):
            m = date_pat.search(ans)
            if m:
                results["last_search_date"] = m.group(0)
                break
        else:
            results["last_search_date"] = None

        # population_count
        pop = None
        for ans in answers_by_field.get("population_count", []):
            m = re.search(r"\d+", ans)
            if m:
                pop = int(m.group(0))
                break
        results["population_count"] = pop

        # country_mentions
        countries = set()
        for ans in answers_by_field.get("country_mentions", []):
            found = re.findall(r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*", ans)
            countries.update(found)
        results["country_mentions"] = sorted(countries)

        # ve_info
        ve_list = []
        for ans in answers_by_field.get("ve_info", []):
            if re.search(r"(\d{1,3}%|\(.*?CI.*?\))", ans):
                ve_list.append(ans)
        results["ve_info"] = ve_list

        # title extractions
        for key in ["location_in_title", "race_ethnicity_in_title",
                    "target_population_in_title", "topic_in_title"]:
            vals = answers_by_field.get(key, [])
            results[key] = vals[0] if vals else None

        # database names (regex + alias fallback)
        results["database_names"] = self.extract_database_names(contexts["num_databases"])
        # num_databases: try QA count else len(database_names)
        num_db = None
        for ans in answers_by_field.get("num_databases", []):
            m = re.search(r"\d+", ans)
            if m:
                num_db = int(m.group(0))
                break
        results["num_databases"] = num_db if num_db is not None else len(results["database_names"])

        # duration, dosage, comparator
        for key in ["duration_of_intervention", "dosage", "comparator"]:
            vals = answers_by_field.get(key, [])
            results[key] = vals[0] if vals else None

        # country_participant_counts
        raw = answers_by_field.get("country_participant_counts", [""])[0]
        counts: Dict[str,int] = {}
        for country, cnt in re.findall(r"([A-Z][a-zA-Z ]+)\s*\((\d+)\)", raw):
            counts[country.strip()] = counts.get(country.strip(), 0) + int(cnt)
        formatted = ", ".join(f"{c}({v})" for c, v in counts.items())
        results["country_participant_counts"] = {
            "per_country": formatted or None,
            "total_count": sum(counts.values())
        }

        # review_type
        vals = answers_by_field.get("review_type", [])
        results["review_type"] = vals[0] if vals else None

        # bias & funding
        bias_qs = answers_by_field.get("bias_and_funding", [])
        # we know the first 3 questions were bias_assessed, next 3 bias_considered, last 3 funding
        results["bias_and_funding_info"] = json.dumps({
            "risk_of_bias_assessed":   (bias_qs[0].capitalize() if len(bias_qs) > 0 else None),
            "bias_considered":         (bias_qs[3].capitalize() if len(bias_qs) > 3 else None),
            "funding_disclosed":       (bias_qs[6].capitalize() if len(bias_qs) > 6 else None),
        })

        return results
