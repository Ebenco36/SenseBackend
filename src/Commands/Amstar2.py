import re
import json
import hashlib
from datetime import datetime
from transformers import pipeline

class Amstar2:
    def __init__(self, review_date: str = "1900-01-01"):
        self.qa_pipeline = pipeline("question-answering", model="distilbert-base-uncased-distilled-squad", device=-1)
        self.publication_bias_qa = pipeline("question-answering", model="bert-large-uncased-whole-word-masking-finetuned-squad")
        self.review_date = review_date

    def contains_keywords(self, text: str, keywords: list[str]) -> bool:
        return any(re.search(keyword, text, re.IGNORECASE) for keyword in keywords)

    def evaluate_prisma_and_exclusions(self, text: str) -> str:
        prisma_keywords = [
            "flowchart", "PRISMA", "study selection", "retrieval flow", "flow diagram",
            "screening flow", "selection of studies", "flow of literature"
        ]
        excluded_section_patterns = [
            r"(excluded studies|studies excluded).*?(table|supplementary|appendix|listed|provided|shown|detailed|included)",
            r"(supplementary\s*(table|material).*?excluded)",
            r"(table\s*\d+\s*[:\-]\s*excluded studies)",
            r"(list of excluded studies)",
            r"(excluded.*?(reasons|criteria|with reason|with justification))"
        ]

        prisma_present = self.contains_keywords(text, prisma_keywords)
        excluded_detailed = any(re.search(pat, text, re.IGNORECASE | re.DOTALL) for pat in excluded_section_patterns)

        if prisma_present and excluded_detailed:
            return "Yes"
        elif excluded_detailed:
            return "Partial Yes"
        return "No"

    def check_prospero_registration(self, text: str) -> str:
        return "YES" if re.search(r'\bPROSPERO\b', text, re.IGNORECASE) and re.search(r'\bCRD\d{6,}\b', text, re.IGNORECASE) else "NO"

    def rule_based_check(self, text, keywords, db_check=False):
        if db_check:
            count = sum(1 for kw in keywords if kw.lower() in text.lower())
            return "Yes" if count >= 2 else "No"
        return "Yes" if any(kw.lower() in text.lower() for kw in keywords) else None

    def ai_based_check(self, text, question):
        answer = self.qa_pipeline(question=question, context=text)
        return "Yes" if answer and answer["score"] > 0.3 else "No"

    def check_within_24_months(self, text: str) -> str:
        year_matches = re.findall(r'\b\d{4}\b', text)
        if not year_matches:
            return "No"
        latest_year = max([int(y) for y in year_matches])
        try:
            latest_date = datetime.strptime(str(latest_year), '%Y')
            review_dt = datetime.strptime(self.review_date, '%Y-%m-%d')
        except ValueError:
            return "No"
        delta_days = (review_dt - latest_date).days
        return "Yes" if 0 <= delta_days <= 730 else "No"

    def evaluate_item4(self, text: str):
        questions_keywords = [
            ("Did the authors search at least 2 databases?", ["MEDLINE", "EMBASE", "Cochrane"], True),
            ("Did the authors provide keyword and/or search strategy?", ["search strategy", "detailed", "umbrealla", "described", "search strategies", "grey literature", "search terms", "Influenza", "Vaccine", "Observational studies", "vaccin", "immuni", "flu", "FLUAD", "test-negative"]),
            ("Did they mention any restrictions at the study selection or eligibility screening, if available?", ["No restrictions", "restriction on", "limited to the English language", "did not include", "no language restrictions", "did not restrict"]),
            ("Did they search the reference lists/ bibliographies of included studies?", ["Citations of included studies were also reviewed", "references", "cross references"]),
            ("Did they search trial/study registries?", ["the Cochrane Central Register of Controlled Trials", "CENTRAL"]),
            ("Did they include / consult content experts in the field", ["language restrictions"]),
        ]
        results = []
        for question, keywords, *extra in questions_keywords:
            db_check = extra[0] if extra else False
            result = self.rule_based_check(text, keywords, db_check)
            if result is None:
                result = self.ai_based_check(text, question)
            results.append((question, result))
        date_result = self.check_within_24_months(text)
        results.append(("Did they conduct search within 24 months of completion of the review?", date_result))
        core = [r for _, r in results[:3]]
        extended = [r for _, r in results[3:]]
        final = "Yes" if all(r == "Yes" for r in extended) else "Partial Yes" if all(r == "Yes" for r in core) else "No"
        return final, results

    def check_publication_bias_with_qa(self, text: str) -> str:
        q1 = "Does the text mention publication bias, funnel plot or Egger's test?"
        q2 = "Does the text justify how publication bias was addressed?"
        a1 = self.publication_bias_qa(question=q1, context=text)
        a2 = self.publication_bias_qa(question=q2, context=text)
        return "YES" if a1['score'] > 0.1 and a2['score'] > 0.1 else "NO"

    def evaluate_pico_components(self, text: str) -> str:
        pico_keywords = {
            "P": ["population", "patients", "participants", "subjects", "children", "adults", "elderly", "adolescents", "infants", "newborns", "pregnant women", "men", "women", "males", "females", "individuals", "cohort", "residents", "seniors", "school-aged", "community-dwelling", "hospitalized", "nursing home", "clinical sample", "healthy volunteers"],
            "I": ["intervention", "treatment", "therapy", "vaccine", "vaccination", "drug", "exposure", "program", "strategy", "supplementation", "medication", "behavioral therapy", "psychological intervention"],
            "C": ["comparison", "control", "placebo", "standard care", "usual care", "no intervention", "waitlist", "alternative treatment"],
            "O": ["outcome", "mortality", "recovery", "efficacy", "effectiveness", "incidence", "rate", "risk", "benefit", "response", "progression", "adverse effects", "side effects", "hospitalization", "length of stay", "quality of life"]
        }
        for group in pico_keywords.values():
            if not any(kw.lower() in text.lower() for kw in group):
                return "No"
        return "Yes"

    def check_if_rct_or_nrsi_present(self, text: str) -> str:
        rct_terms = ["randomized controlled trial", "randomised controlled trial", "rct", "randomized trial", "randomised trial", "clinical trial"]
        nrsi_terms = ["observational study", "observational studies", "cohort", "case-control", "case control", "cross-sectional", "cross sectional", "test-negative", "test negative", "real-world"]
        found_rct = any(t in text.lower() for t in rct_terms)
        found_nrsi = any(t in text.lower() for t in nrsi_terms)
        return "Yes" if found_rct or found_nrsi else "No"

    def evaluate_item6_data_extraction_duplicate(self, text: str) -> str:
        duplicate = ["data extraction in duplicate", "extracted in duplicate", "two reviewers independently extracted", "independently extracted by two reviewers", "two authors independently extracted", "duplicate data extraction", "two observers independently extracted"]
        consensus = ["discrepancies were resolved by consensus", "resolved by discussion", "differences were resolved by discussion", "checked by a second reviewer", "disagreements resolved by consensus", "agreement was reached", "consensus of both investigators", "achieved consensus", "≥80%", "greater than 80%", "good agreement"]
        if any(p in text.lower() for p in duplicate) or any(p in text.lower() for p in consensus):
            return "Yes"
        return "No"

    def evaluate_item8(self, text: str) -> str:
        table_keywords = ["Table 1", "Characteristics and Outcomes of Included Studies", "Table of Characteristics", "Characteristics of included studies", "study characteristics", "study details"]
        detailed_studies_keywords = ["detailed description", "described in detail", "individual studies", "study results", "included studies described"]
        table_found = any(re.search(keyword, text, re.IGNORECASE) for keyword in table_keywords)
        detailed_description_found = any(re.search(keyword, text, re.IGNORECASE) for keyword in detailed_studies_keywords)
        if table_found or detailed_description_found:
            return "Yes"
        else:
            return "No"
        
    def determine_study_types(self, text: str) -> list[str]:
        types = []
        if any(term in text.lower() for term in ["randomized controlled trial", "rct", "randomised"]):
            types.append("RCT")
        if any(term in text.lower() for term in ["observational study", "cohort", "case-control", "cross-sectional", "test-negative"]):
            types.append("NRSI")
        return types    

    def evaluate_item9(self, text: str, study_types: list[str]) -> str:
        rct_tools = ["Cochrane risk-of-bias tool", "RoB 2"]
        nrsi_tools = ["Newcastle-Ottawa", "ROBINS-I", "SIGN", "MMAT"]
        rct_ok = "Yes" if "RCT" in study_types else None
        nrsi_ok = "Yes" if "NRSI" in study_types else None

        if "RCT" in study_types:
            rct_tool_found = any(tool.lower() in text.lower() for tool in rct_tools)
            rct_table_fig_present = bool(re.search(r"(Table|Fig(?:ure)?)\\s?\\d+", text, re.IGNORECASE))
            rct_detailed_results = bool(re.search(r"each (individual )?study|shown in Table|Figure \\d+", text, re.IGNORECASE))
            if not (rct_tool_found and rct_table_fig_present and rct_detailed_results):
                rct_ok = "No"

        if "NRSI" in study_types:
            nrsi_tool_found = any(tool.lower() in text.lower() for tool in nrsi_tools)
            nrsi_table_fig_present = bool(re.search(r"(Table|Fig(?:ure)?)\\s?\\d+", text, re.IGNORECASE))
            nrsi_detailed_results = bool(re.search(r"each (individual )?study|shown in Table|Figure \\d+", text, re.IGNORECASE))
            if not (nrsi_tool_found and nrsi_table_fig_present and nrsi_detailed_results):
                nrsi_ok = "No"

        if study_types == ["RCT"] and rct_ok == "Yes":
            return "Yes"
        if study_types == ["NRSI"] and nrsi_ok == "Yes":
            return "Yes"
        if "RCT" in study_types and "NRSI" in study_types and rct_ok == "Yes" and nrsi_ok == "Yes":
            return "Yes"
        return "No"

    def evaluate_item11(self, text: str) -> str:
        meta_terms = ["meta-analysis", "meta analysis", "meta-analyses", "performed a meta-analysis", "we conducted a meta analysis", "meta-analytic", "meta analytic", "pooled estimate", "forest plot"]
        model_keywords = ["random-effects model", "fixed-effects model", "mixed-effects", "mantel-haenszel"]
        model_reasons = ["due to heterogeneity", "because of heterogeneity", "expected heterogeneity", "model was chosen", "justified using"]
        heterogeneity_keywords = ["i²", "i2", "heterogeneity was assessed", "chi²", "q test", "cochrane q", "p < 0.1", "p<0.1"]
        exploration_keywords = ["investigated heterogeneity", "explored heterogeneity", "investigated the cause of heterogeneity", "subgroup analysis", "sensitivity analysis", "meta-regression", "looked into heterogeneity", "explored sources of heterogeneity"]
        no_result_keywords = ["no clear cause found", "did not find anything", "no explanation found"]
        text = text.lower()
        if not any(term in text for term in meta_terms):
            return "Not applicable"
        if all(term in text for term in model_keywords + model_reasons + heterogeneity_keywords) and (any(k in text for k in exploration_keywords) or any(k in text for k in no_result_keywords)):
            return "Yes"
        return "No"

    def evaluate_item13(self, text: str) -> str:
        text = text.lower()
        criterion1 = [r"\bonly (low|lower) risk of bias\b", r"\bonly studies with (low|lower) risk of bias\b", r"\bonly (high quality|low risk) studies\b", r"\bonly included studies scoring.*(high|≥[ ]?x)\b", r"\bwe included only.*(low risk|high quality)"]
        if any(re.search(pat, text) for pat in criterion1):
            return "Yes"
        rob_terms = ["risk of bias", "high risk of bias", "moderate risk of bias", "selection bias", "reporting bias", "study quality", "potential bias", "bias may have affected", "interpret with caution", "limitations due to bias", "unclear risk of bias"]
        interpretation_terms = ["discussion", "limitations", "conclusion", "interpretation", "caution"]
        if any(term in text for term in rob_terms) and any(term in text for term in interpretation_terms):
            return "Yes"
        return "No"

    def evaluate_all(self, text: str) -> dict:
        i4_result, i4_details = self.evaluate_item4(text)
        study_types = self.determine_study_types(text)
        item8_result = self.evaluate_item8(text)
        item9_result = self.evaluate_item9(text,study_types)
        item11_result = self.evaluate_item11(text)
        item13_result = self.evaluate_item13(text)
        return {
            "Item 1 - PICO Components Present": self.evaluate_pico_components(text),
            "Item 2 - Protocol / PROSPERO": self.check_prospero_registration(text),
            "Item 3 - Explanation on the Selection of the Study Design (RCT/NRSI)": self.check_if_rct_or_nrsi_present(text),
            "Item 4 - Comprehensive Literatue Search strategy": i4_result,
            "Item 4 - Details": i4_details,
            "Item 6 - Duplicate Data Extraction": self.evaluate_item6_data_extraction_duplicate(text),
            "Item 7 - List of Excluded Studies with Justification": self.evaluate_prisma_and_exclusions(text),
            "Item 8 - Description of Included Studies in Adequate Detail": item8_result,
            "Item 9 - Satisfactory Technique for RoB (RCT + NRSI)": item9_result,
            "Item 11 - Use of Appropriate Methods for Statistical Combination of Results": item11_result,
            "Item 13 - Risk of Bias Considered": item13_result,
            "Item 15 - Publication Bias Investigation": self.check_publication_bias_with_qa(text),
        }

    def amstar_label_and_flaws(self, results):
        critical_keys = {
            "Item 2 - Protocol / PROSPERO",
            "Item 4 - Comprehensive Literatue Search strategy",
            "Item 7 - List of Excluded Studies with Justification",
            "Item 9 - Satisfactory Technique for RoB (RCT + NRSI)",
            "Item 11 - Use of Appropriate Methods for Statistical Combination of Results",
            "Item 13 - Risk of Bias Considered",
            "Item 15 - Publication Bias Investigation"
        }

        secondary_keys = {
            "Item 1 - PICO Components Present",
            "Item 3 - Explanation on the Selection of the Study Design (RCT/NRSI)",
            "Item 6 - Duplicate Data Extraction",
            "Item 8 - Description of Included Studies in Adequate Detail"
        }

        flaws = []

        critical_flaws = [key for key in critical_keys if results.get(key, "").strip().lower() == "no"]
        secondary_flaws = [key for key in secondary_keys if results.get(key, "").strip().lower() == "no"]

        # Flaws listesine notasyon ekle
        flaws.extend([f"# {f}" for f in critical_flaws])
        flaws.extend([f"* {f}" for f in secondary_flaws])

        # Yeni kurala göre label belirleme
        if len(critical_flaws) >= 2:
            label = "Critically Low"
        elif len(critical_flaws) == 1:
            label = "Low"
        else:
            if len(secondary_flaws) >= 2:
                label = "Moderate"
            else:
                label = "High"

        return label, flaws
    
    # ------------------- AMSTAR Class + Evaluation Functions -------------------
    # This assumes the `amstar2` and `amstar_label_and_flaws` functions you already have
    # (Not repeating here for brevity, but they should be present above this code block.)

    # ------------------- Normalization for PostgreSQL Columns -------------------

    def normalize_key_for_column(self, name: str, max_len: int = 63) -> str:
        """
        Normalize long keys to snake_case column names. Truncates and hashes if needed.
        """
        key = re.sub(r'[^a-zA-Z0-9]+', '_', name).strip('_').lower()
        if len(key) > max_len:
            hash_suffix = hashlib.md5(key.encode()).hexdigest()[:8]
            key = key[:max_len - 9] + "_" + hash_suffix
        return key

    def prepare_amstar_update_dict(self, results: dict, summary: tuple) -> dict:
        output = {}

        for key, value in results.items():
            column = self.normalize_key_for_column(key)
            if isinstance(value, list):
                nested_dict = {q.strip(): a.strip() for q, a in value}
                output[column] = json.dumps(nested_dict)
            else:
                output[column] = value.strip() if isinstance(value, str) else value

        # Unpack label + flaws
        label, flaws = summary
        output["amstar_label"] = label
        output["amstar_flaws"] = flaws

        return output


