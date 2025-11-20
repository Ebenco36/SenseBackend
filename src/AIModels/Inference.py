import re
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

import numpy as np
from dateutil.parser import parse
from sentence_transformers import SentenceTransformer, util

# Optional numeric parsers (best-effort if available)
try:
    from number_parser import parse_number as _np_parse_number  # pip install number-parser
except Exception:
    _np_parse_number = None

try:
    from word2number import w2n  # pip install word2number
except Exception:
    w2n = None

from collections import OrderedDict
from typing import Any, Dict


def _add_term(term_map: Dict[str, str], term: str, code: str, limit: int = 150):
    """Helper: store matched phrase → code (preserve first-seen casing), cap size for safety."""
    if term and len(term_map) < limit and term not in term_map:
        term_map[term] = code


# ------------------------- Small utilities -------------------------
def _prune_empty(x: Any) -> Any:
    """
    Recursively drop empty dicts/lists/None. Keep falsy scalars like 0 or "".
    """
    if x is None:
        return None
    if isinstance(x, dict):
        pruned = {k: _prune_empty(v) for k, v in x.items()}
        pruned = {k: v for k, v in pruned.items() if v not in (None, {}, [])}
        return pruned
    if isinstance(x, list):
        pruned_list = [ _prune_empty(v) for v in x ]
        pruned_list = [ v for v in pruned_list if v not in (None, {}, []) ]
        return pruned_list
    return x  # scalar

def extract_valuable_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only whitelisted, structured fields and prune empties recursively.
    Also preserves a logical, readable key order.
    """
    if not isinstance(data, dict):
        raise ValueError("Input must be a dictionary.")

    # whitelist now includes term maps and articles
    valuable_keys_ordered = [
        "lit_search_date",
        "articles",                  # article/study final counts (your new extractor)
        "studies",                   # legacy total + per-design from earlier path
        "design_counts",
        "topics",
        "topics_terms",              # NEW: exact phrases -> topic code
        "outcomes",
        "interventions",
        "intervention_terms",        # NEW: exact phrases -> disease/vaccine option codes
        "age_groups",
        "age_group_terms",           # NEW: exact phrases/ranges -> short age code
        "specific_groups",
        "specific_group_terms",      # NEW: exact phrases -> cg/pw/hcw/...
        "immune_status",
        "countries",
        "regions",
        "databases",
        "treatment",
    ]

    out = OrderedDict()
    for k in valuable_keys_ordered:
        if k in data:
            pruned = _prune_empty(data[k])
            if pruned not in (None, {}, []):
                out[k] = pruned

    return out

_MONTHS = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"

def _split_sentences(text:str)->List[str]:
    parts = re.split(r'(?<=[\.\?!])\s+', text or "")
    return [p.strip() for p in parts if p and p.strip()]

def _strip_commas_int(s: str) -> Optional[int]:
    s = (s or "").strip()
    if not s: return None
    m = re.search(r"\d[\d,]*", s)
    if not m: return None
    try:
        return int(m.group(0).replace(",", ""))
    except Exception:
        return None

_WORDMAP = {
    "zero":0,"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,"ten":10,
    "eleven":11,"twelve":12,"thirteen":13,"fourteen":14,"fifteen":15,"sixteen":16,"seventeen":17,"eighteen":18,
    "nineteen":19,"twenty":20,"thirty":30,"forty":40,"fifty":50,"sixty":60,"seventy":70,"eighty":80,"ninety":90,
    "hundred":100,"thousand":1000,"million":1_000_000
}

def _int_from_strnum(s: str) -> Optional[int]:
    if not s or not isinstance(s, str): return None
    s_norm = (s or "").replace("–","-").replace("—","-").replace("−","-").strip()
    d = _strip_commas_int(s_norm)
    if d is not None: return d
    if _np_parse_number is not None:
        try:
            val = _np_parse_number(s_norm)
            if isinstance(val, (int, float)): return int(val)
        except Exception:
            pass
    if w2n is not None:
        try:
            val = w2n.word_to_num(s_norm.lower().replace("-", " "))
            if isinstance(val, int): return val
        except Exception:
            pass
    total, cur = 0, 0
    had_any = False
    for w in re.split(r"[-\s]+", s_norm.lower()):
        if w in ("hundred","thousand","million"):
            cur = max(1,cur) * _WORDMAP[w]; total += cur; cur = 0; had_any = True
        elif w in _WORDMAP and _WORDMAP[w] < 100:
            cur += _WORDMAP[w]; had_any = True
        elif re.fullmatch(r"\d+", w):
            total += int(w); cur = 0; had_any = True
    total += cur
    return total if had_any and total>0 else None

def _int_from_words_or_digits(s: str) -> Optional[int]:
    if not s or not isinstance(s, str): return None
    d = _strip_commas_int(s)
    if d is not None: return d
    s = (s or "").lower().replace("–","-").replace("—","-").replace("−","-").replace("-", " ")
    total, cur, seen = 0, 0, False
    for w in s.split():
        if w in ("hundred","thousand","million"):
            cur = max(1, cur) * _WORDMAP[w]; total += cur; cur = 0; seen = True
        elif w in _WORDMAP and _WORDMAP[w] < 100:
            cur += _WORDMAP[w]; seen = True
        elif re.fullmatch(r"\d+", w):
            total += int(w); cur = 0; seen = True
    total += cur
    return total if seen and total > 0 else None

def _latest_date_from_candidates(cands: List[str]) -> Optional[str]:
    best = None
    for x in set(cands or []):
        try:
            dt = parse(re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', x), fuzzy=True)
            if best is None or dt > best[0]: best = (dt, x)
        except Exception:
            continue
    return best[1] if best else None


# ------------------------- QA Head (numeric) -------------------------
class QAHead:
    """
    Lightweight numeric QA head:
      - Accepts a question string and a list of short contexts (sentences/clauses).
      - Uses keyword bags + regex to find numbers near question-specific labels.
      - Returns (int_value_or_None, evidence_dict).
    """
    def __init__(self):
        # Define canonical question → keyword bag + compiled regex (at runtime per ask)
        self.q2labels = {
            "total": [
                r"(?:final|overall)?\s*(?:number|count)?\s*(?:of\s+)?(?:articles?|stud(?:y|ies))\s*(?:included|in\s+this\s+review|in\s+review)?",
                r"(?:in\s+total|overall|altogether)\s*(?:,|\s+)?(?:we\s+)?(?:included|analyzed|synthesized)"
            ],
            "rct": [r"randomi[sz]ed\s+controlled\s+trials?", r"\bRCTs?\b"],
            "cohort": [r"cohort\s+stud(?:y|ies)"],
            "case_control": [r"case[-\s]?control\s+stud(?:y|ies)"],
            "cross_sectional": [r"cross[-\s]?sectional\s+stud(?:y|ies)"],
            "nrsi": [r"(?:non[-\s]?randomi[sz]ed|observational)\s+stud(?:y|ies)"],
        }

        # A general numeric pattern (words or digits) followed/preceded by a label
        self.num_pat = r"([A-Za-z][A-Za-z\- ]+|\d[\d,]*)"

    def _compile_label_regex(self, labels: List[str], direction: str = "forward") -> re.Pattern:
        """
        direction: 'forward' → [NUM] + LABEL; 'back' → LABEL + [NUM]
        """
        label_union = "(?:" + "|".join(labels) + ")"
        if direction == "forward":
            patt = rf"\b{self.num_pat}\s+{label_union}\b"
        else:
            patt = rf"\b{label_union}\b[^.{{0,120}}]{{0,120}}?\b{self.num_pat}\b"
        return re.compile(patt, flags=re.I)

    def _score(self, sent: str, label: str) -> int:
        s = sent.lower()
        score = 0
        if "in this review" in s or "included" in s: score -= 2
        if any(x in s for x in ("final","overall")): score -= 2
        if "representing" in s and "unique" in s: score -= 1
        if any(x in s for x in ("excluded","screened","not included","eligibility","removed")): score += 5
        if label in ("rct","cohort","case_control","cross_sectional","nrsi"): score -= 1
        return score

    def ask_int(self, question_key: str, contexts: List[str], fallback_text: Optional[str]=None) -> Tuple[Optional[int], Dict]:
        """
        question_key: one of self.q2labels keys
        """
        labels = self.q2labels.get(question_key, [])
        if not labels: return None, {}

        patt_fwd = self._compile_label_regex(labels, "forward")
        patt_back = self._compile_label_regex(labels, "back")

        cands: List[Tuple[int,int,str,str]] = []  # (score, length, match_str, sent)
        scan_blocks = list(contexts or [])
        if fallback_text: scan_blocks.append(fallback_text)

        for s in scan_blocks:
            # forward: [NUM] label
            for m in patt_fwd.finditer(s):
                n = _int_from_words_or_digits(m.group(1))
                if n is None: continue
                sc = self._score(s, question_key)
                cands.append((sc, len(m.group(0)), m.group(0), s))
            # backward: label ... [NUM]
            for m in patt_back.finditer(s):
                n = _int_from_words_or_digits(m.group(1))
                if n is None: continue
                sc = self._score(s, question_key)
                cands.append((sc, len(m.group(0)), m.group(0), s))

        if not cands:
            return None, {}

        # pick best (lowest score, then shortest span)
        cands.sort(key=lambda x: (x[0], x[1]))
        best_span = cands[0][2]
        best_sent = cands[0][3]

        # pull the integer back out of the best span (robust)
        mnum = re.search(r"([A-Za-z][A-Za-z\- ]+|\d[\d,]*)", best_span)
        val = _int_from_words_or_digits(mnum.group(1)) if mnum else None
        return val, {"context": best_sent, "match": best_span, "score": cands[0][0]}


# ------------------------- SR Predictor -------------------------
class SRPredictor:
    """
    SentenceTransformer retriever + regex extractors + QA head for numeric answers.
    """

    def __init__(self, model_path: str, device: Optional[str] = None, top_k: int = 10):
        import torch
        if device is None:
            device = "cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu")
        self.model = SentenceTransformer(model_path, device=device)
        self.device = device
        self.top_k = top_k

        # Queries
        self.queries = {
            "studies": "How many studies or reviews were included?",
            "articles": "How many articles or studies or reviews were included in the review?",
            "date":    "What is the last literature search date?",
            "country": "Give country counts or sample sizes by country.",
            "designs": "List and count study designs in the included studies.",
            "outcomes":"What outcomes were studied (infection, hospitalization, ICU, death)?",
            "topics":  "What topics are discussed (effectiveness, safety, risk factors, coverage, acceptance, administration, economics, modeling, ethics)?",
            "interv":  "Which diseases and vaccine-preventable interventions are targeted?",
            "ages":    "What age groups or specific populations are mentioned?",
            "regions": "Which world regions are mentioned in relation to studies or samples?",
            "dbs": "Which bibliographic databases and sources were searched?",
            "tx_duration": "What was the intervention duration or follow-up duration?",
            "tx_dose": "What dosage or dosing schedule was used?",
            "tx_comp": "What was the comparator (placebo, control, unvaccinated, standard care)?"
        }

        # QA head + question presets
        self.qa = QAHead()
        self.q_total = "total"
        self.q_rct = "rct"
        self.q_coh = "cohort"
        self.q_cc  = "case_control"
        self.q_cs  = "cross_sectional"
        self.q_nrsi= "nrsi"

        # Regions
        self.regions = [
            "Africa","Asia","Europe","North America","South America","Oceania","Middle East","Sub-Saharan Africa",
            "Southeast Asia","East Asia","South Asia","Central Asia","Western Europe","Eastern Europe","Latin America",
            "Caribbean","Pacific Islands"
        ]

        # Country aliases
        self.country_alias = {
            "US":"United States","U.S.":"United States","U.S":"United States","USA":"United States",
            "UK":"United Kingdom","U.K.":"United Kingdom","U.K":"United Kingdom",
            "UAE":"United Arab Emirates","Korea":"South Korea"
        }

        # VPD map
        self.vpd_map = {
            "covid":["COVID-19","COVID","COVID 19","SARS-CoV-2"],
            "aden":["Adenovirus"], "anth":["anthrax"], "camp":["Campylobacter"],
            "chol":["Cholera","Vibrio cholerae","cholerae"], "coxi":["Q fever","Coxiella burnetii","Q-fever"],
            "diph":["Diphtheria"], "ebol":["Ebola"], "ente":["Enterovirus"],
            "esch":["Escherichia coli (ETEC)"], "hib":["Haemophilus influenzae type b"],
            "ha":["Hepatitis A"], "hb":["Hepatitis B"], "hc":["Hepatitis C"], "he":["Hepatitis E"],
            "hs":["Herpes simplex"], "hz":["Shingles","Herpes zoster"], "hiv":["HIV / AIDS","HIV","AIDS","HIV/AIDS"],
            "hpv":["Human papillomavirus","HPV"], "je":["Japanese encephalitis"],
            "leis":["Leishmaniasis","Leishmania"], "lyme":["Lyme disease, Borreliosis","Borreliosis","Lyme disease"],
            "mala":["Malaria","Plasmodia"], "meas":["Measles"], "meni":["Meningococcal","Neisseria"],
            "mump":["Mumps"], "myle":["Leprosy","Mycobacterium leprae"], "myva":["Mycobacterium vaccae"],
            "pert":["Pertussis","Bordetella"], "plag":["Plague"], "pneu":["Pneumococcal"],
            "poli":["Poliovirus","Poliomyelitis"], "pseu":["Pseudomonas aeruginosa"], "rabi":["Rabies"],
            "rsv":["Respiratory syncytial virus","rsv"], "rube":["Rubella"],
            "salm":["Salmonella"], "shig":["Shigella"], "smal":["Smallpox","Variola"],
            "strb":["Streptococcus group B"], "tt":["Tetanus"], "tbe":["Tick-borne encephalitis"], "tb":["Tuberculosis"],
            "typh":["Typhoid"], "vari":["Varicella, Chickenpox","Varicella","Chickenpox"], "yf":["Yellow fever"],
            "zika":["Zika"], "infl":["influenza"], "deng":["dengue"], "rota":["rotavirus"]
        }

        # Vaccine options
        self.vaccine_opts = {
            "live":["live"], "nonlive":["non-live","nonlive"], "adjuvants":["adjuvants","adjuvant"],
            "nonadjuvanted":["non-adjuvanted","nonadjuvanted"], "quad":["quadrivalent","4vHPV"], "biva":["bivalent","2vHPV"]
        }

        # Topics
        self.topic_map = {
            "eff":["effectiveness","impact of","effectiveness of","efficacy","VE","CI","RR","OR","RD","rate difference","odds ratios","odds ratio (OR)","odds ratios (ORs)","IRR","relative risks(RR)","relative risks","efficacy rate","effectiveness rate","vaccine efficacy","hazard ratio","HR","risk ratio","rate ratio","adjusted","propensity score","did not effectively","no effect","not effective","pooled"],
            "saf":["safety","adverse effects","adverse events"],
            "rf":["risk factor","risk"],
            "cov":["coverage","uptake","the uptake","actual uptake","vaccine uptake"],
            "kaa":["acceptance","Barrier","vaccine barriers","knowledge","vaccination willingness and intentions","HPV vaccine acceptability","Awareness and knowledge","Awareness","facilitators of and barriers","awareness,knowledge, acceptability, and intention","knowledge and acceptability","knowledge and awareness","attitudes and beliefs","Knowledge and Attitude","attitude","knowledge, awareness, and attitude"],
            "adm":["administration","vaccine types","dose schedules","vaccine types and dose schedules","different dose schedules","Two doses of"],
            "eco":["economic","cost","financial","economic impact","cost effectiveness","cost-effectiveness","economic evaluation","Cost-effectiveness of HPV vaccination strategies"],
            "mod":["modeling"],
            "eth":["racial","ethnic","ethnic minority","racial minority","racial/ethnic","racial/ethnic minority","racial disparity","ethnic disparity","minority","minority population"],
        }

        # Outcomes
        self.outcomes = {
            "infection":["infection","infections"],
            "hospital":["hospitalization","hospitalisation"],
            "icu":["ICU","intensive care unit","intensive care"],
            "death":["death","mortality","overall mortality","cancer related mortality","on overall and cancer mortality"]
        }

        # Age/groups/immune
        self.age_groups = {
            "nb_0__1":["newborn","babies","baby","infant","toddlers","young ones","youngsters","small children"],
            "chi_2__9":["child","children"],
            "ado_10__17":["adolescents","adolescent","young adults"],
            "adu_18__64":["adults","adult"],
            "eld_65__10000":["elderly","older adults"],
        }
        self.specific_groups = {
            "hcw":["Physician","Nurse","Surgeon","Dentist","Pharmacist","Physical Therapist","Occupational Therapist","Medical Laboratory Technologist","Radiologist","Dietitian/Nutritionist","Respiratory Therapist","Speech-Language Pathologist","Physician Assistant","Nurse Practitioner","Certified Nursing Assistant (CNA)","Medical Assistant","Paramedic/EMT","Midwife","Psychologist","Social Worker (Clinical)","Hospital Administrator","Medical Researcher","Health Educator","Orthopedic Technician","Optometrist","Podiatrist","Anesthesiologist","Neurologist","Cardiologist","Gastroenterologist"],
            "pw":["pregnant","pregnant women"],
            "tra":["traveller","traveler"],
            "pcg":["parents","caregivers"],
        }
        self.immune_status = {"imu":["immunocompromised"], "hty":["healthy"]}

        # Screens/guards
        self._inclusion_verbs = re.compile(
            r"\b(included|were included|was included|retained|synthesi[sz]ed|meta-?analy[sz]ed|analy[sz]ed|used for analysis|for data extraction|in the review)\b",
            flags=re.I
        )
        self._decoy = re.compile(r"\b(excluded|screened|removed|not included|missing data|non-eligible|eligibility)\b", re.I)
        self._pat_date = re.compile(
            rf"{_MONTHS}\s+\d{{1,2}},?\s+\d{{4}}|{_MONTHS}\s+\d{{4}}|\d{{4}}-\d{{2}}-\d{{2}}",
            flags=re.I
        )

        # Database vocabulary (canonical → aliases)
        self.database_vocab = {
            "PubMed":["PubMed"],
            "MEDLINE":["MEDLINE","Ovid MEDLINE","MEDLINE via Ovid","MEDLINE Complete"],
            "Embase":["Embase","EMBASE","Embase via Ovid"],
            "Web of Science":["Web of Science","Science Citation Index Expanded","Conference Proceedings Citation Index"],
            "Scopus":["Scopus"],
            "CINAHL":["CINAHL","CINAHL Complete"],
            "Cochrane Library":["Cochrane Library","CENTRAL","Cochrane Central Register of Controlled Trials"],
            "PubMed Central":["PubMed Central"],
            "LILACS":["LILACS"],
            "Google Scholar":["Google Scholar"],
            "ProQuest":["ProQuest","ProQuest Dissertations & Theses Global","ProQuest News & Newspapers"],
            "EBSCO":["EBSCO","EBSCOhost","Academic Search Complete","Business Source Complete","SocINDEX","SPORTDiscus"],
            "Ovid":["Ovid"],
            "PsycINFO":["PsycINFO","PsycARTICLES","PsycBOOKS"],
            "AMED":["AMED"],
            "ClinicalTrials.gov":["ClinicalTrials.gov"],
            "BIOSIS":["BIOSIS"],
            "TOXLINE":["TOXLINE"],
            "CANCERLIT":["CANCERLIT"],
            "HMIC":["HMIC"],
            "POPLINE":["POPLINE"],
            "Global Health":["Global Health"],
            "CAB Abstracts":["CAB Abstracts"],
            "AGRICOLA":["AGRICOLA"],
            "GeoRef":["GeoRef"],
            "ASSIA":["ASSIA"],
            "Social Services Abstracts":["Social Services Abstracts"],
            "Sociological Abstracts":["Sociological Abstracts"],
            "EconLit":["EconLit"],
            "ERIC":["ERIC"],
            "PAIS Index":["PAIS Index"],
            "IBSS":["IBSS"],
            "UpToDate":["UpToDate"],
            "DynaMed":["DynaMed"],
            "Clinical Key":["Clinical Key","ClinicalKey"],
            "BMJ Best Practice":["BMJ Best Practice"],
            "Cochrane Clinical Answers":["Cochrane Clinical Answers"],
            "TRIP Database":["TRIP Database","TRIP"],
            "NICE Evidence Search":["NICE Evidence Search"],
            "DrugBank":["DrugBank"],
            "PharmGKB":["PharmGKB"],
            "RxList":["RxList"],
            "Martindale":["Martindale"],
            "AHFS Drug Information":["AHFS Drug Information"],
            "OMIM":["OMIM"],
            "GenBank":["GenBank"],
            "Gene":["Gene"],
            "GEO":["GEO"],
            "UniProt":["UniProt"],
            "EU Clinical Trials Register":["EU Clinical Trials Register","EUCTR"],
            "ISRCTN Registry":["ISRCTN Registry","ISRCTN"],
            "WHO ICTRP":["WHO ICTRP","ICTRP"],
            "ANZCTR":["Australian New Zealand Clinical Trials Registry","ANZCTR"],
            "Epistemonikos":["Epistemonikos"],
            "Health Evidence":["Health Evidence"],
            "Campbell Collaboration Library":["Campbell Collaboration Library","Campbell Library"],
            "3ie Database of Systematic Reviews":["3ie Database of Systematic Reviews","3ie"],
            "OpenGrey":["OpenGrey"],
            "GreyNet":["GreyNet"],
            "NTIS":["NTIS"],
            "CORDIS":["CORDIS"],
            "EThOS":["EThOS"],
            "DART-Europe":["DART-Europe"],
            "IEEE Xplore":["IEEE Xplore"],
            "Google Patents":["Google Patents"],
            "Espacenet":["Espacenet"],
            "USPTO":["USPTO"],
            "DOAJ":["Directory of Open Access Journals","Directory of Open Access Journals (DOAJ)","DOAJ"],
            "PLoS":["PLoS"],
            "BioMed Central":["BioMed Central","BMC"],
            "arXiv":["arXiv"],
            "medRxiv":["medRxiv"],
            "bioRxiv":["bioRxiv"],
            "OpenDOAR":["OpenDOAR"],
            "BASE":["BASE"],
            "Wiley Online Library":["Wiley Online Library"],
            "ScienceDirect":["ScienceDirect"],
            "SpringerLink":["SpringerLink"],
            "JSTOR":["JSTOR"],
            "Taylor & Francis Online":["Taylor & Francis Online"],
            "Sage Journals":["Sage Journals"],
            "Oxford Academic":["Oxford Academic"],
            "Cambridge Core":["Cambridge Core"],
            "Nature.com":["Nature.com"],
            "Science Magazine":["Science Magazine"],
            "CDC Stacks":["CDC Stacks"],
            "NIH.gov":["NIH.gov"],
            "WHO.int":["WHO.int"],
            "WorldBank.org":["WorldBank.org"],
            "UN iLibrary":["UN iLibrary"],
            "ABI/INFORM":["ABI/INFORM"],
            "Factiva":["Factiva"],
            "LexisNexis":["LexisNexis","Nexis Uni"],
            "Westlaw":["Westlaw"],
            "HeinOnline":["HeinOnline"],
            "Project MUSE":["Project MUSE"],
            "Physiotherapy Evidence":["Physiotherapy Evidence","PEDro","OTseeker","SpeechBITE"],
        }
        self._db_flat = []
        for canon, aliases in self.database_vocab.items():
            for a in aliases:
                self._db_flat.append((canon, re.compile(rf"\b{re.escape(a)}\b", re.I)))

        # Treatment regexes
        self._rx_duration = re.compile(
            r"\b(?:for|over|during|with|median follow[-\s]?up of|follow[-\s]?up of|followup of)\s+"
            r"(\d{1,3}\s*(?:day|days|week|weeks|month|months|year|years))\b"
            r"|(\d{1,3}\s*(?:day|days|week|weeks|month|months|year|years))\s+(?:follow[-\s]?up|treatment|intervention)\b",
            re.I
        )
        self._rx_dose_units = re.compile(r"\b(\d+(?:\.\d+)?)\s*(mg|g|mcg|μg|ug|IU|mL|ml)\b", re.I)
        self._rx_dose_schedule = re.compile(
            r"\b(?:\d+[-\s]?dose|two[-\s]?dose|three[-\s]?dose|4[-\s]?dose|3[-\s]?dose)\s+(?:series|schedule)\b"
            r"|(?:at|on)\s+(?:0\s*,\s*1\s*,\s*6|0\s*and\s*6|0\s*,\s*2\s*,\s*6)\s*(?:months?|mo)\b"
            r"|\b(?:dose(?:s)?\s+at\s+\d+\s*(?:weeks?|months?))\b",
            re.I
        )
        self._rx_comparator = re.compile(
            r"\b(?:versus|vs\.?|compared\s+(?:to|with)|against)\s+(placebo|control|standard care|usual care|no treatment|unvaccinated)\b"
            r"|\b(placebo|control|standard care|usual care|no treatment|unvaccinated)\b\s+(?:group|arm)\b",
            re.I
        )

    # ------------------------- Core retrieval -------------------------
    def _top_sentences(self, sentences: List[str], query: str, k: Optional[int]=None) -> List[str]:
        if not sentences: return []
        k = k or self.top_k
        s_clean = [s for s in sentences if s and s.strip()]
        if not s_clean: return []
        embs = self.model.encode(s_clean, convert_to_tensor=True, normalize_embeddings=True)
        qemb = self.model.encode([query], convert_to_tensor=True, normalize_embeddings=True)[0]
        sims = util.cos_sim(embs, qemb).cpu().numpy().reshape(-1)
        idx = np.argsort(-sims)[:k]
        return [s_clean[int(i)] for i in idx]

    def _looks_included(self, s: str) -> bool:
        if self._decoy.search(s): return False
        return bool(self._inclusion_verbs.search(s) or re.search(r"\b(included|eligible)\s+stud(?:y|ies)\b", s, flags=re.I))

    # Wrapper to QA head
    def _ask_int(self, question_key: str, contexts: List[str], fallback_text: Optional[str]=None) -> Tuple[Optional[int], Dict]:
        return self.qa.ask_int(question_key, contexts, fallback_text=fallback_text)

    # ------------------------- Slots -------------------------
    def predict_lit_date(self, text: str) -> Tuple[Optional[str], str]:
        sents = _split_sentences(text)
        top = self._top_sentences(sents, self.queries["date"])
        cands = []
        evid = ""
        for s in top:
            ds = self._pat_date.findall(s)
            if ds:
                cands.extend(ds)
                if not evid: evid = s
        if not cands:
            cands = self._pat_date.findall(text)
            evid = cands and "global-scan" or ""
        return _latest_date_from_candidates(cands), evid

    def extract_article_study_counts(self, text: str) -> Tuple[Dict, List[Dict]]:
        sents = _split_sentences(text)
        cand = self._top_sentences(sents, self.queries["articles"], k=max(18, self.top_k))
        contexts = cand[:]  # list[str]
        evid: List[Dict] = []

        # High-priority patterns
        pat_final = re.compile(
            r"\b(?:final|overall)\s+(?:number|count)\s+of\s+(?:articles?|stud(?:y|ies))\s+(?:included|includ(?:ed|ing))"
            r"(?:\s+in\s+this\s+review)?\s*(?:is|was|were|:)?\s*([A-Za-z][A-Za-z\-\s]*|\d[\d,]*)"
            r"(?:\s*(?:representing|corresponding\s+to)\s+([A-Za-z][A-Za-z\-\s]*|\d[\d,]*)\s+unique\s+stud(?:y|ies))?",
            re.I
        )
        pat_in_review = re.compile(
            r"\b([A-Za-z][A-Za-z\-\s]*|\d[\d,]*)\s+(?:articles?|stud(?:y|ies))\s+were?\s+included\s+in\s+this\s+review"
            r"(?:[^.]{0,160}?\b(?:representing|corresponding\s+to)\s+([A-Za-z][A-Za-z\-\s]*|\d[\d,]*)\s+unique\s+stud(?:y|ies))?",
            re.I
        )
        pat_meeting_incl = re.compile(r"\b([A-Za-z][A-Za-z\-\s]*|\d[\d,]*)\s+(?:met|meeting)\s+inclusion\s+criteria\b", re.I)
        pat_generic = re.compile(
            r"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+(?:stud(?:y|ies)|articles?|trials?)\b[^.]{0,160}?\b"
            r"(?:included|analy[sz]ed|synthesi[sz]ed|meta-?analy[sz]ed|used for analysis|for data extraction|in the review)\b",
            re.I
        )
        pat_generic_rev = re.compile(
            r"\b(?:included|analy[sz]ed|synthesi[sz]ed|meta-?analy[sz]ed|used for analysis|for data extraction|in the review)\b"
            r"[^.]{0,160}?\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+(?:stud(?:y|ies)|articles?|trials?)\b",
            re.I
        )

        # Typed designs via regex (then QA fallback)
        def typed_from_regex(kind: str, labels: List[str]) -> Optional[int]:
            pat = re.compile(rf"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+(?:{'|'.join(labels)})\b", re.I)
            for s in contexts[:16]:
                if not self._looks_included(s) and "stud" not in s.lower():
                    continue
                m = pat.search(s)
                if m:
                    n = _int_from_words_or_digits(m.group(1))
                    if n is not None:
                        evid.append({"field": f"studies.{kind}", "text": s})
                        return n
            return None

        def score_clause(clause: str) -> int:
            clause_l = clause.lower()
            score = 0
            if re.search(r"\b(final|overall)\b", clause_l): score -= 6
            if re.search(r"\bin this review\b", clause_l): score -= 5
            if re.search(r"\brepresenting\s+\d", clause_l): score -= 3
            if re.search(r"\bmet(?:ting)? inclusion criteria\b", clause_l): score += 2
            if self._decoy.search(clause): score += 6
            return score

        article_total = None
        study_total = None

        candidates = []
        scan_blocks = contexts + [text]
        for s in scan_blocks:
            for m in pat_final.finditer(s):
                art = _int_from_words_or_digits(m.group(1))
                stud = _int_from_words_or_digits(m.group(2)) if m.group(2) else None
                if art: candidates.append(("final", art, stud, m.group(0), s))
            for m in pat_in_review.finditer(s):
                art = _int_from_words_or_digits(m.group(1))
                stud = _int_from_words_or_digits(m.group(2)) if m.group(2) else None
                if art: candidates.append(("in_review", art, stud, m.group(0), s))

        if candidates:
            candidates.sort(key=lambda x: (score_clause(x[4]), len(x[3])))
            _, art, stud, frag, sent = candidates[0]
            article_total = art
            study_total = stud
            evid.append({"field":"studies.final", "text": sent})

        if article_total is None and study_total is None:
            meet_cands = []
            for s in contexts:
                for m in pat_meeting_incl.finditer(s):
                    n = _int_from_words_or_digits(m.group(1))
                    if n: meet_cands.append((n, s))
            if meet_cands:
                meet_cands.sort(key=lambda x: score_clause(x[1]))
                article_total = meet_cands[0][0]
                evid.append({"field":"studies.inclusion_criteria", "text": meet_cands[0][1]})

        if article_total is None and study_total is None:
            gen_cands = []
            for s in contexts:
                if self._decoy.search(s): continue
                m = pat_generic.search(s) or pat_generic_rev.search(s)
                if m:
                    n = _int_from_words_or_digits(m.group(1))
                    if n: gen_cands.append((n, s))
            if gen_cands:
                gen_cands.sort(key=lambda x: score_clause(x[1]))
                article_total = gen_cands[0][0]
                evid.append({"field":"studies.generic", "text": gen_cands[0][1]})

        # QA backstop for total (articles if not explicit)
        if article_total is None and study_total is None:
            total_qa, ev_total = self._ask_int(self.q_total, contexts[:10], fallback_text=text)
            if total_qa:
                article_total = total_qa
                if ev_total.get("context"):
                    evid.append({"field": "studies.total_qa", "text": ev_total["context"]})

        total = study_total if (study_total and study_total <= (article_total or study_total)) else (article_total or 0)

        # typed design counts: regex → QA fallback
        rct_regex = typed_from_regex("rct", ["randomi[sz]ed\\s+controlled\\s+trials?", "rcts?"])
        coh_regex = typed_from_regex("cohort", ["cohort\\s+stud(?:y|ies)"])
        cc_regex  = typed_from_regex("case_control", ["case[-\\s]?control\\s+stud(?:y|ies)"])
        cs_regex  = typed_from_regex("cross_sectional", ["cross[-\\s]?sectional\\s+stud(?:y|ies)"])
        nrsi_regex= typed_from_regex("nrsi", ["non[-\\s]?randomi[sz]ed\\s+stud(?:y|ies)", "observational\\s+stud(?:y|ies)"])

        def qa_backstop(cur_val: Optional[int], qkey: str, field: str) -> int:
            if cur_val is not None: return cur_val
            v, ev = self._ask_int(qkey, contexts[:8], fallback_text=text)
            if ev.get("context"):
                evid.append({"field": f"studies.{field}", "text": ev["context"]})
            return v or 0

        rct = qa_backstop(rct_regex, self.q_rct, "rct")
        coh = qa_backstop(coh_regex, self.q_coh, "cohort")
        cc  = qa_backstop(cc_regex,  self.q_cc,  "case_control")
        cs  = qa_backstop(cs_regex,  self.q_cs,  "cross_sectional")
        nrsi= qa_backstop(nrsi_regex,self.q_nrsi,"nrsi")

        parts = {
            "articles_included": article_total or 0,
            "unique_studies": study_total or 0,
            "total": total or 0,
            "rct": rct or 0,
            "cohort": coh or 0,
            "case_control": cc or 0,
            "cross_sectional": cs or 0,
            "nrsi": nrsi or 0
        }

        # reconcile if typed sum > total
        typed_sum = sum(parts[k] for k in ("rct","cohort","case_control","cross_sectional","nrsi"))
        if parts["total"] and typed_sum > parts["total"]:
            factor = parts["total"] / typed_sum if typed_sum else 1.0
            for k in ("rct","cohort","case_control","cross_sectional","nrsi"):
                parts[k] = int(round((parts[k] or 0) * factor))

        # optional: number of countries included
        pat_c = re.compile(r"\b(?:span|from)\s+([A-Za-z][A-Za-z\-\s]*|\d[\d,]*)\s+countries?\b", re.I)
        for s in contexts[:12]:
            m = pat_c.search(s)
            if m:
                nc = _int_from_words_or_digits(m.group(1))
                if nc:
                    parts["num_countries_included"] = nc
                    evid.append({"field": "studies.num_countries_included", "text": s})
                    break

        return parts, evid

    def predict_study_counts(self, text: str) -> Tuple[Dict, List[str]]:
        sents = _split_sentences(text)
        top = self._top_sentences(sents, self.queries["studies"], k=max(14, self.top_k))
        evid = []

        pat_a = re.compile(
            r"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+(?:stud(?:y|ies)|articles?|trials?)\b[^.]{0,120}?\b"
            r"(?:included|analy[sz]ed|synthesi[sz]ed|meta-?analy[sz]ed|used for analysis|for data extraction|in the review)\b", re.I)
        pat_b = re.compile(
            r"\b(?:included|analy[sz]ed|synthesi[sz]ed|meta-?analy[sz]ed|used for analysis|for data extraction|in the review)\b"
            r"[^.]{0,120}?\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+(?:stud(?:y|ies)|articles?|trials?)\b", re.I)

        total = None
        for s in top[:12]:
            if not self._looks_included(s): continue
            m = pat_a.search(s) or pat_b.search(s)
            if m:
                n = _int_from_strnum(m.group(1))
                if n is not None:
                    total = n; evid.append(s); break

        def typed(label_regex: str) -> Optional[int]:
            pat = re.compile(rf"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+{label_regex}\b", re.I)
            for s in top[:16]:
                if not self._looks_included(s):
                    if "stud" not in s.lower(): continue
                m = pat.search(s)
                if m:
                    n = _int_from_strnum(m.group(1))
                    if n is not None:
                        evid.append(s); return n
            return None

        parts = {
            "rct": typed(r"(?:randomi[sz]ed\s+controlled\s+trials?|rcts?)") or 0,
            "cohort": typed(r"cohort\s+stud(?:y|ies)") or 0,
            "case_control": typed(r"case[-\s]?control\s+stud(?:y|ies)") or 0,
            "cross_sectional": typed(r"cross[-\s]?sectional\s+stud(?:y|ies)") or 0,
            "nrsi": typed(r"(?:non[-\s]?randomi[sz]ed|observational)\s+stud(?:y|ies)") or 0
        }

        sum_parts = sum(parts.values())
        if total and sum_parts > total:
            factor = total / sum_parts
            for k in parts:
                parts[k] = int(round(parts[k] * factor))

        out = {"total": total or 0, **parts}
        return out, evid[:8]

    def predict_countries_regions(self, text: str) -> Tuple[Dict, List[str]]:
        evid = []

        def norm_country(name: str) -> Optional[str]:
            n = (name or "").strip()
            if n in self.country_alias: n = self.country_alias[n]
            if n.lower().startswith("the "): n = n[4:]
            if not re.match(r"^[A-Z][A-Za-z]+(?:\s[A-Z][A-Za-z]+)*$", n): return None
            return n

        pat_counts = re.compile(r"\b([A-Z][A-Za-z]*(?:\s[A-Z][A-Za-z]*)*)\s*\(\s*(\d{1,4})\s*\)")
        pat_samples = re.compile(r"\b([A-Z][A-Za-z]*(?:\s[A-Z][A-Za-z]*)*)\s*\(\s*[nN]\s*=\s*([\d\s,]+)\s*\)")
        pat_participants_from = re.compile(
            r"\b([\d][\d,\s]*)\s+(?:participants|subjects)\s+(?:from|in)\s+([A-Z][A-Za-z]*(?:\s[A-Z][A-Za-z]*)*)", re.I)
        pat_ctry_had_participants = re.compile(
            r"\b([A-Z][A-Za-z]*(?:\s[A-Z][A-Za-z]*)*)\b[^.]{0,40}?\b(?:had|with)\s+([\d][\d,\s]*)\s+(?:participants|subjects)\b", re.I)

        study_counts: Dict[str, int] = defaultdict(int)
        sample_sizes: Dict[str, int] = defaultdict(int)

        for m in pat_samples.finditer(text):
            c, n = norm_country(m.group(1)), _strip_commas_int(m.group(2).replace(" ", ""))
            if c and n is not None:
                sample_sizes[c] = max(sample_sizes[c], n); evid.append(m.group(0))
        for m in pat_participants_from.finditer(text):
            n, c = _strip_commas_int(m.group(1).replace(" ", "")), norm_country(m.group(2))
            if c and n is not None:
                sample_sizes[c] = max(sample_sizes[c], n); evid.append(m.group(0))
        for m in pat_ctry_had_participants.finditer(text):
            c, n = norm_country(m.group(1)), _strip_commas_int(m.group(2).replace(" ", ""))
            if c and n is not None:
                sample_sizes[c] = max(sample_sizes[c], n); evid.append(m.group(0))
        for m in pat_counts.finditer(text):
            c, n = norm_country(m.group(1)), _int_from_strnum(m.group(2))
            if c and n is not None:
                study_counts[c] += n; evid.append(m.group(0))

        regions_found = {}
        for reg in self.regions:
            if re.search(rf"\b{re.escape(reg)}\b", text): regions_found[reg] = True

        study_counts = dict(sorted(study_counts.items(), key=lambda x: (-x[1], x[0])))
        sample_sizes = dict(sorted(sample_sizes.items(), key=lambda x: (-x[1], x[0])))

        return {"countries": {"study_counts": study_counts, "sample_sizes": sample_sizes},
                "regions": regions_found}, evid[:20]

    def predict_designs(self, text: str) -> Tuple[Dict, List[str]]:
        sents = _split_sentences(text)
        top = self._top_sentences(sents, self.queries["designs"], k=max(16, self.top_k))
        evid = []
        patterns = {
            "rct": re.compile(r"\brandomi[sz]ed\s+controlled\s+trial(s)?\b|\bRCTs?\b", re.I),
            "cohort": re.compile(r"\bcohort\s+stud(?:y|ies)\b", re.I),
            "case_control": re.compile(r"\bcase[-\s]?control\s+stud(?:y|ies)\b", re.I),
            "cross_sectional": re.compile(r"\bcross[-\s]?sectional\s+stud(?:y|ies)\b", re.I),
            "nrsi": re.compile(r"\b(non[-\s]?randomi[sz]ed|observational)\s+stud(?:y|ies)\b", re.I),
        }
        counts = {k:0 for k in patterns}
        for s in top:
            for k, rg in patterns.items():
                if rg.search(s):
                    m = re.search(rf"([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+{rg.pattern}", s)
                    n = _int_from_strnum(m.group(1)) if m else None
                    counts[k] += n if n else 1
                    if len(evid) < 12: evid.append(s)
        return counts, evid

    def predict_topics_outcomes(self, text: str) -> Tuple[Dict, Dict, List[str], Dict[str, str]]:
        """
        Returns:
        topics_counts: Dict[str,int]
        outcomes_counts: Dict[str,int]
        evidence: List[str]
        topics_terms: Dict[matched_phrase -> topic_code]
        """
        sents = _split_sentences(text)
        top_t = self._top_sentences(sents, self.queries["topics"])
        top_o = self._top_sentences(sents, self.queries["outcomes"])
        evid = []
        topics = defaultdict(int)
        outcomes = defaultdict(int)

        # capture exact phrases for topics
        topics_terms: Dict[str, str] = OrderedDict()

        for s in top_t:
            for code, toks in self.topic_map.items():
                for t in toks:
                    m = re.search(rf"\b{re.escape(t)}\b", s, flags=re.I)
                    if m:
                        topics[code] += 1
                        _add_term(topics_terms, m.group(0), code)
                        if len(evid) < 20:
                            evid.append(s)
                        break  # avoid double-counting same code in same sentence

        # outcomes (kept as counts only)
        for s in top_o:
            for code, toks in self.outcomes.items():
                for t in toks:
                    if re.search(rf"\b{re.escape(t)}\b", s, flags=re.I):
                        outcomes[code] += 1
                        if len(evid) < 20:
                            evid.append(s)
                        break

        return dict(topics), dict(outcomes), evid, dict(topics_terms)


    def predict_interventions(self, text: str) -> Tuple[Dict, Dict, List[str], Dict[str, str]]:
        """
        Returns:
        diseases_counts: Dict[str,int]  (e.g., {'hpv':8,'infl':1})
        vaccine_option_counts: Dict[str,int] (e.g., {'quad':2})
        evidence: List[str]
        intervention_terms: Dict[matched_phrase -> code]  (merged: diseases + options)
        """
        sents = _split_sentences(text)
        top = self._top_sentences(sents, self.queries["interv"])
        evid = []
        diseases = defaultdict(int)
        vopts = defaultdict(int)

        terms: Dict[str, str] = OrderedDict()

        for s in top:
            # diseases / VPDs
            for code, names in self.vpd_map.items():
                for n in names:
                    m = re.search(rf"\b{re.escape(n)}\b", s, flags=re.I)
                    if m:
                        diseases[code] += 1
                        _add_term(terms, m.group(0), code)
                        if len(evid) < 20:
                            evid.append(s)
                        break  # once per code per sentence
            # vaccine options
            for code, names in self.vaccine_opts.items():
                for n in names:
                    m = re.search(rf"\b{re.escape(n)}\b", s, flags=re.I)
                    if m:
                        vopts[code] += 1
                        _add_term(terms, m.group(0), code)
                        if len(evid) < 20:
                            evid.append(s)
                        break

        return dict(diseases), dict(vopts), evid, dict(terms)


    def predict_ages_groups(self, text: str) -> Tuple[Dict, Dict, Dict, List[str], Dict[str, str], Dict[str, str]]:
        """
        Returns:
        ages_counts: Dict[str,int]             (legacy counts, keys: nb_0__1, chi_2__9, ado_10__17, adu_18__64, eld_65__10000, range_*, age_*)
        groups_counts: Dict[str,int]           (legacy counts: hcw, pw, tra, pcg)
        immune_counts: Dict[str,int]
        evidence: List[str]
        age_group_terms: Dict[matched_phrase -> short_code]     (ado, adu, chi, nb, eld; numeric ranges → ado/chi/adu/eld where inferable OR 'range')
        specific_group_terms: Dict[matched_phrase -> short_code] (cg for parents/caregivers; pw, hcw, tra, etc.)
        """
        sents = _split_sentences(text)
        top = self._top_sentences(sents, self.queries["ages"])
        evid = []
        ages = defaultdict(int)
        groups = defaultdict(int)
        immune = defaultdict(int)

        # map long keys → short codes for nice terms
        age_short = {
            "nb_0__1": "nb",
            "chi_2__9": "chi",
            "ado_10__17": "ado",
            "adu_18__64": "adu",
            "eld_65__10000": "eld",
        }
        # alias pcg → cg in terms list (keep legacy key for counts)
        sg_alias = {"pcg": "cg"}

        age_group_terms: Dict[str, str] = OrderedDict()
        special_group_terms: Dict[str, str] = OrderedDict()

        # numeric ranges
        pat_age_range = re.compile(
            r"\b(\d{1,2})\s*(?:to|-|–|—)\s*(\d{1,2})\s*(?:years?|yrs?)\b|\b(\d{1,2})\s*(?:years?|yrs?)\b",
            flags=re.I
        )

        def infer_age_short(a: int) -> str:
            if a <= 1: return "nb"
            if 2 <= a <= 9: return "chi"
            if 10 <= a <= 17: return "ado"
            if 18 <= a <= 64: return "adu"
            return "eld"

        for s in top:
            # dictionary hits (ages)
            for long_key, toks in self.age_groups.items():
                for t in toks:
                    m = re.search(rf"\b{re.escape(t)}\b", s, flags=re.I)
                    if m:
                        ages[long_key] += 1
                        _add_term(age_group_terms, m.group(0), age_short.get(long_key, long_key))
                        if len(evid) < 20: evid.append(s)
                        break

            # dictionary hits (specific groups)
            for sg_key, toks in self.specific_groups.items():
                for t in toks:
                    m = re.search(rf"\b{re.escape(t)}\b", s)
                    if m:
                        groups[sg_key] += 1
                        _add_term(special_group_terms, m.group(0), sg_alias.get(sg_key, sg_key))
                        if len(evid) < 20: evid.append(s)
                        break

            # immune
            for im_key, toks in self.immune_status.items():
                for t in toks:
                    if re.search(rf"\b{re.escape(t)}\b", s, flags=re.I):
                        immune[im_key] += 1
                        if len(evid) < 20: evid.append(s)
                        break

            # numeric ranges → infer short code if possible
            for m in pat_age_range.finditer(s):
                if m.group(1) and m.group(2):
                    a1, a2 = int(m.group(1)), int(m.group(2))
                    # choose bucket by mid-point
                    mid = (a1 + a2) // 2
                    short = infer_age_short(mid)
                    ages[f"range_{a1}_{a2}"] += 1
                    _add_term(age_group_terms, f"{m.group(1)}–{m.group(2)} years", short)
                elif m.group(3):
                    a1 = int(m.group(3))
                    short = infer_age_short(a1)
                    ages[f"age_{a1}"] += 1
                    _add_term(age_group_terms, f"{m.group(3)} years", short)
                if len(evid) < 20: evid.append(s)

        return dict(ages), dict(groups), dict(immune), evid, dict(age_group_terms), dict(special_group_terms)
    # ---- Databases ----
    def predict_databases(self, text: str) -> Tuple[Dict, List[str]]:
        sents = _split_sentences(text)
        top_db = self._top_sentences(sents, self.queries["dbs"], k=max(12, self.top_k))
        evidence: List[str] = []
        found: Dict[str, bool] = {}

        def scan_for_dbs(block: str):
            for canon, rg in self._db_flat:
                if rg.search(block): found[canon] = True

        for s in top_db: scan_for_dbs(s)
        if not found: scan_for_dbs(text)

        db_list = sorted(found.keys())
        if db_list:
            for s in top_db:
                if any(db in s for db in db_list):
                    evidence.append(s)
                    if len(evidence) >= 5: break

        return {"num_databases": len(db_list), "database_list": db_list}, evidence

    # ---- Treatment details ----
    def predict_treatment(self, text: str) -> Tuple[Dict, List[str]]:
        sents = _split_sentences(text)
        top_dur = self._top_sentences(sents, self.queries["tx_duration"], k=max(8, self.top_k))
        top_dose = self._top_sentences(sents, self.queries["tx_dose"], k=max(8, self.top_k))
        top_comp = self._top_sentences(sents, self.queries["tx_comp"], k=max(8, self.top_k))
        evidence: List[str] = []

        durations: List[str] = []
        for s in top_dur:
            for m in self._rx_duration.finditer(s):
                val = (m.group(1) or m.group(2))
                if val:
                    durations.append(val.strip())
                    if len(evidence) < 10: evidence.append(s)
        if not durations:
            for m in self._rx_duration.finditer(text):
                val = (m.group(1) or m.group(2))
                if val: durations.append(val.strip()); break

        doses: List[str] = []
        schedules: List[str] = []
        for s in top_dose:
            for m in self._rx_dose_units.finditer(s):
                doses.append(m.group(0))
                if len(evidence) < 12: evidence.append(s)
            for m in self._rx_dose_schedule.finditer(s):
                schedules.append(m.group(0))
                if len(evidence) < 12: evidence.append(s)
        if not doses:
            m = self._rx_dose_units.search(text)
            if m: doses.append(m.group(0))
        if not schedules:
            m = self._rx_dose_schedule.search(text)
            if m: schedules.append(m.group(0))

        comparators: List[str] = []
        for s in top_comp:
            for m in self._rx_comparator.finditer(s):
                comp = next((g for g in m.groups() if g), None)
                if comp:
                    comparators.append(comp.lower())
                    if len(evidence) < 15: evidence.append(s)
        if not comparators:
            m = self._rx_comparator.search(text)
            if m:
                comp = next((g for g in m.groups() if g), None)
                if comp: comparators.append(comp.lower())

        durations = list(dict.fromkeys(durations))[:3]
        doses = list(dict.fromkeys(doses))[:5]
        schedules = list(dict.fromkeys(schedules))[:5]
        comparators = list(dict.fromkeys(comparators))[:3]

        return {
            "duration_of_intervention": durations,
            "dosage": {"amounts": doses, "schedules": schedules},
            "comparator": comparators
        }, evidence

    # ------------------------- Master -------------------------
    def predict_all(self, text: str) -> Dict:
        lit_date, ev_date = self.predict_lit_date(text)
        studies, ev_st = self.predict_study_counts(text)
        cr, ev_cty = self.predict_countries_regions(text)
        designs, ev_des = self.predict_designs(text)

        # UPDATED: topics with terms
        topics, outcomes, ev_to, topics_terms = self.predict_topics_outcomes(text)

        # UPDATED: interventions with terms
        diseases, vopts, ev_int, intervention_terms = self.predict_interventions(text)

        # UPDATED: ages/groups with terms
        ages, groups, immune, ev_age, age_terms, sg_terms = self.predict_ages_groups(text)

        article_counts, article_ev_art = self.extract_article_study_counts(text)
        dbs, ev_dbs = self.predict_databases(text)
        treat, ev_trt = self.predict_treatment(text)

        ev = []
        if ev_date: ev.append({"field":"lit_search_date", "text": ev_date})
        ev += [{"field":"studies", "text": e} for e in ev_st]
        ev += [{"field":"articles", "text": e} for e in article_ev_art]
        ev += [{"field":"countries/regions", "text": e} for e in ev_cty]
        ev += [{"field":"designs", "text": e} for e in ev_des]
        ev += [{"field":"topics/outcomes", "text": e} for e in ev_to]
        ev += [{"field":"interventions", "text": e} for e in ev_int]
        ev += [{"field":"ages/groups", "text": e} for e in ev_age]
        ev += [{"field":"databases", "text": e} for e in ev_dbs]
        ev += [{"field":"treatment", "text": e} for e in ev_trt]
        ev = ev[:60]

        return extract_valuable_data({
            "lit_search_date": lit_date,
            "studies": studies,
            "articles": article_counts,
            "countries": cr.get("countries", {}),
            "regions": cr.get("regions", {}),
            "design_counts": designs,

            # counts
            "topics": topics,
            "outcomes": outcomes,
            "interventions": {"diseases": diseases, "vaccine_options": vopts},
            "age_groups": ages,
            "specific_groups": groups,
            "immune_status": immune,

            # NEW term maps
            "topics_terms": topics_terms,                    # e.g. {"Awareness":"kaa","knowledge and awareness":"kaa",...}
            "intervention_terms": intervention_terms,        # e.g. {"HPV":"hpv","influenza":"infl","quadrivalent":"quad"}
            "age_group_terms": age_terms,                    # e.g. {"adolescents":"ado","10–17 years":"ado","adults":"adu"}
            "specific_group_terms": sg_terms,                # e.g. {"caregivers":"cg","parents":"cg","pregnant":"pw"}

            "databases": dbs,
            "treatment": treat,
            "_evidence": ev
        })


# ------------------------- Quick usage -------------------------
# if __name__ == "__main__":
#     text = """
#     As shown in Fig. 1, 229 unique articles were identified after database searches,
#     29 were given full-text review with 14 meeting inclusion criteria. One article was identified by hand-searching.
#     The final number of articles included in this review is fifteen representing 14 unique studies.
#     These studies span ten countries from SSA: Botswana (1), South Africa (2), Nigeria (2), Kenya (3), Ghana (1),
#     Uganda (1), Mali (1), Zambia (1), Tanzania (1) and Malawi (1).
#     We searched MEDLINE (Ovid), Embase, Web of Science, Scopus and Cochrane Library through March 2024.
#     The largest sample sizes were from China (n = 35,812), the United States (n = 10,437), and Germany (n = 7,670).
#     In total, 6 randomized controlled trials, 4 cohort studies, and one case-control study were analyzed;
#     numerous cross-sectional observational studies were also included.
#     Outcomes included infection, hospitalization, ICU admission, and death.
#     We studied influenza and HPV vaccines, including quadrivalent (4vHPV) and bivalent (2vHPV) options;
#     both live and non-live formulations were reported. Follow-up of 12 months was typical;
#     a two-dose schedule at 0 and 6 months was common, with some 3-dose series at 0, 1, 6 months;
#     comparisons versus unvaccinated and placebo groups were reported.
#     Populations included adolescents (10–17 years), adults, and elderly; specific groups included nurses and pregnant women, with
#     some immunocompromised patients. Studies were conducted in Europe, Asia, and Sub-Saharan Africa.
#     """

#     model_path = "allenai/specter2_base"  # or your fine-tuned model path
#     print("Device set to use: auto")
#     pred = SRPredictor(model_path=model_path, device=None, top_k=12)
#     out = pred.predict_all(text)
#     from pprint import pprint
#     pprint(out)
