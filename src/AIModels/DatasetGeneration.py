import os, re, json, random
from dataclasses import dataclass
from typing import List, Optional, Dict
from collections import defaultdict

# Optional (recommended) deps:
# pip install sentence-transformers number-parser pycountry
try:
    from sentence_transformers import SentenceTransformer, util, models
except Exception:
    SentenceTransformer, util, models = None, None, None

try:
    from number_parser import parse_number as _np_parse_number
except Exception:
    _np_parse_number = None

try:
    import pycountry as _pycountry
except Exception:
    _pycountry = None

# ---------------- utilities ----------------

_WORDMAP = {
    "zero":0,"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,"ten":10,
    "eleven":11,"twelve":12,"thirteen":13,"fourteen":14,"fifteen":15,"sixteen":16,"seventeen":17,
    "eighteen":18,"nineteen":19,"twenty":20,"thirty":30,"forty":40,"fifty":50,"sixty":60,"seventy":70,
    "eighty":80,"ninety":90,"hundred":100,"thousand":1000,"million":1_000_000
}
_MONTHS = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"

# Base country list (expanded via pycountry if available)
_BASE_COUNTRIES = {
    "Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Argentina", "Armenia", "Australia", "Austria",
    "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin",
    "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso",
    "Burundi", "Cabo Verde", "Cambodia", "Cameroon", "Canada", "Central African Republic", "Chad", "Chile",
    "China", "Colombia", "Comoros", "Congo", "Costa Rica", "Croatia", "Cuba", "Cyprus", "Czech Republic",
    "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "America",
    "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia", "Fiji", "Finland", "France", "Gabon",
    "Gambia", "Georgia", "Germany", "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau",
    "Guyana", "Haiti", "Honduras", "Hungary", "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland",
    "Israel", "Italy", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kuwait", "Kyrgyzstan",
    "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg",
    "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Mauritania", "Mauritius", "Mexico",
    "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru",
    "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria", "North Korea", "North Macedonia",
    "Norway", "Oman", "Pakistan", "Palau", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines",
    "Poland", "Portugal", "Qatar", "Romania", "Russia", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia",
    "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia",
    "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Islands",
    "Somalia", "South Africa", "South Korea", "South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname",
    "Sweden", "Switzerland", "Syria", "Taiwan", "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo",
    "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine",
    "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu",
    "Vatican City", "Venezuela", "Vietnam", "Yemen", "Zambia", "Zimbabwe"
}
_COUNTRY_ALIASES = {
    "usa": "United States", "u.s.": "United States", "u.s": "United States", "us": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom", "u.k.": "United Kingdom", "u.k": "United Kingdom",
    "south korea": "South Korea", "republic of korea": "South Korea",
    "czechia": "Czech Republic", "iran, islamic republic of": "Iran", "russian federation": "Russia",
}

# Regions to mine (you asked to include these)
_REGIONS = [
    "Africa","Asia","Europe","North America","South America","Oceania","Middle East","Sub-Saharan Africa",
    "Southeast Asia","East Asia","South Asia","Central Asia","Western Europe","Eastern Europe","Latin America",
    "Caribbean","Pacific Islands"
]
_REGION_ALIASES = {
    "ssa": "Sub-Saharan Africa",
    "sub saharan africa": "Sub-Saharan Africa",
    "sub-saharan africa": "Sub-Saharan Africa",
    "south-east asia": "Southeast Asia",
    "south east asia": "Southeast Asia",
    "east asia and pacific": "East Asia",
    "western europe": "Western Europe",
    "eastern europe": "Eastern Europe",
    "latin america and the caribbean": "Latin America",
    "lac": "Latin America",
    "mena": "Middle East",
    "middle-east": "Middle East",
    "pacific": "Pacific Islands",
}

def _normalize_spaces(s:str)->str:
    return re.sub(r"\s+", " ", s or "").strip()

def _linewise(text:str)->List[str]:
    raw = (text or "").splitlines()
    lines = []
    for r in raw:
        r = r.strip(" \t•*-")
        if r:
            lines.append(r)
    return lines

def _split_sentences(text:str)->List[str]:
    parts = re.split(r'(?<=[\.\?!])\s+', text or "")
    parts = [p for p in parts if p and p.strip()]
    lines = _linewise(text)
    merged = list(dict.fromkeys([_normalize_spaces(p) for p in (parts + lines)]))
    return merged

def _strip_commas_to_int(s:str)->Optional[int]:
    m = re.search(r"\d[\d,]*", s or "")
    if not m: return None
    try:
        return int(m.group(0).replace(",", ""))
    except Exception:
        return None

def _int_from_words(s:str)->Optional[int]:
    s = (s or "").lower().replace("–","-").replace("—","-").replace("−","-").replace("-", " ")
    if _np_parse_number:
        try:
            val = _np_parse_number(s)
            if isinstance(val,(int,float)): return int(val)
        except Exception:
            pass
    total, cur, seen = 0, 0, False
    for w in s.split():
        if w in _WORDMAP and _WORDMAP[w] < 100:
            cur += _WORDMAP[w]; seen = True
        elif w in ("hundred","thousand","million"):
            cur = max(1,cur) * _WORDMAP[w]; total += cur; cur = 0; seen = True
        elif re.fullmatch(r"\d+", w):
            total += int(w); cur = 0; seen = True
    total += cur
    return total if seen and total>0 else None

def _int_flexible(s:str)->Optional[int]:
    d = _strip_commas_to_int(s)
    if d is not None: return d
    return _int_from_words(s)

def verbalize_int(n:int)->str:
    if n < 20:
        for k,v in _WORDMAP.items():
            if v==n: return k
    tens = ["","", "twenty","thirty","forty","fifty","sixty","seventy","eighty","ninety"]
    if n < 100:
        return tens[n//10] + ("" if n%10==0 else f"-{verbalize_int(n%10)}")
    if n < 1000:
        h = n//100; r = n%100
        return f"{verbalize_int(h)} hundred" + ("" if r==0 else f" {verbalize_int(r)}")
    if n < 1_000_000:
        th = n//1000; r = n%1000
        return f"{verbalize_int(th)} thousand" + ("" if r==0 else f" {verbalize_int(r)}")
    return str(n)

# ---------------- regex blocks ----------------

_INCLUSION_VERBS = re.compile(
    r"\b(included|were included|was included|retained|synthesi[sz]ed|meta-?analy[sz]ed|analy[sz]ed|"
    r"used for analysis|for data extraction|in the review)\b",
    flags=re.I
)
_DECOYS = re.compile(
    r"\b(excluded|screened|removed|not included|missing data|non-eligible|eligibility)\b",
    flags=re.I
)

# Studies total
_PAT_STUDIES_A = re.compile(
    r"\b([A-Za-z][A-Za-z\- ]|\d[\d,])+\s+(?:stud(?:y|ies)|articles?|trials?)\b[^.]{0,160}?\b"
    r"(?:included|analy[sz]ed|synthesi[sz]ed|meta-?analy[sz]ed|used for analysis|for data extraction|in the review)\b",
    flags=re.I
)
_PAT_STUDIES_B = re.compile(
    r"\b(?:included|analy[sz]ed|synthesi[sz]ed|meta-?analy[sz]ed|used for analysis|for data extraction|in the review)\b"
    r"[^.]{0,160}?\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+(?:stud(?:y|ies)|articles?|trials?)\b",
    flags=re.I
)

# Date
_PAT_DATE = re.compile(
    rf"{_MONTHS}\s+\d{{1,2}},?\s+\d{{4}}|{_MONTHS}\s+\d{{4}}|\d{{4}}-\d{{2}}-\d{{2}}",
    flags=re.I
)
_PAT_DATE_CONTEXT = re.compile(
    r"\b(last|search|searched|conducted|performed|updated|up to|through|until)\b",
    flags=re.I
)

# Country & Region patterns
_PAT_COUNTRY_STUDYCOUNT = re.compile(r"\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\s*\(\s*(\d{1,5})\s*\)")
_PAT_COUNTRY_SAMPLE_N   = re.compile(r"\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\s*\(\s*n\s*=\s*([\d,]+)\s*\)", flags=re.I)

# We’ll use the same shapes for regions:
_PAT_REGION_STUDYCOUNT = _PAT_COUNTRY_STUDYCOUNT
_PAT_REGION_SAMPLE_N   = _PAT_COUNTRY_SAMPLE_N

# Intervention signals
_PAT_INTERV_CORE = re.compile(
    r"\b(effect(?:iveness)?|efficacy|impact|intervention|trial|randomi[sz]ed controlled trial|rct|vaccine|vaccination)\b",
    flags=re.I
)
_PAT_INTERV_VS = re.compile(
    r"\b(vaccinated|immuni[sz]ed|intervention)\s*(?:vs\.?|versus|vs)\s*(unvaccinated|placebo|control|no\s+intervention)\b",
    flags=re.I
)

# Study designs
_DESIGN_PATTERNS: Dict[str, List[re.Pattern]] = {
    "rct": [re.compile(r"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+(?:randomi[sz]ed controlled trials?|rcts?)\b", re.I)],
    "cohort": [re.compile(r"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+cohort stud(?:y|ies)\b", re.I)],
    "case_control": [re.compile(r"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+case[-\s]?control stud(?:y|ies)\b", re.I)],
    "cross_sectional": [re.compile(r"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+cross[-\s]?sectional stud(?:y|ies)\b", re.I)],
    "prospective": [re.compile(r"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+prospective\b", re.I)],
    "retrospective": [re.compile(r"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+retrospective\b", re.I)],
    "longitudinal": [re.compile(r"\b([A-Za-z][A-Za-z\- ]+|\d[\d,]*)\s+longitudinal\b", re.I)],
    "meta_analysis": [re.compile(r"\bmeta[-\s]?analysis\b", re.I)],
    "systematic_review": [re.compile(r"\bsystematic review\b", re.I)],
    "quasi_experimental": [re.compile(r"\bquasi[-\s]?experimental\b", re.I)],
    "before_after": [re.compile(r"\bbefore[-\s]?after stud(?:y|ies)\b", re.I)],
    "time_series": [re.compile(r"\btime[-\s]?series\b", re.I)],
}

# Outcomes
_PAT_OUTCOME_INF = re.compile(r"\b(infection|infected|cases?\b)\b", re.I)
_PAT_OUTCOME_HOS = re.compile(r"\b(hospitali[sz]ation|hospitali[sz]ed|admission[s]?)\b", re.I)
_PAT_OUTCOME_ICU = re.compile(r"\b(ICU|intensive care (?:unit)?|critical care)\b", re.I)
_PAT_OUTCOME_DEA = re.compile(r"\b(death|mortality|fatalit(?:y|ies))\b", re.I)

# Age
_PAT_AGE_NUMERIC = re.compile(
    r"\b(?:(?:ages?\s*)?(?P<a>\d{1,3})\s*(?:–|-|to)\s*(?P<b>\d{1,3})\s*(?:years?|yrs?)\b"
    r"|(?:<|less than)\s*(?P<lt>\d{1,3})\s*(?:years?|yrs?)\b"
    r"|(?:>|\bolder than)\s*(?P<gt>\d{1,3})\s*(?:years?|yrs?)\b"
    r"|(?P<single>\d{1,3})\s*(?:years?|yrs?)\b)",
    flags=re.I
)
_PAT_AGE_GROUPS = re.compile(
    r"\b(infants?|newborns?|neonates?|children|child|adolescents?|teens?|young adults?|adults?|elderly|older adults?)\b",
    flags=re.I
)

_NEG_TEMPLATES = [
    r"We excluded \d+ articles after full[- ]?text",
    r"We screened \d[\d,]* records",
    r"PRISMA flow",
    r"Risk of bias",
    r"Supplementary Table",
    r"Data were collected in \d{4}",
    r"We plan an update in",
]

# ---------------- your dictionaries (topics / interventions / groups) ----------------

TOPIC_DICT = {
    "eff": [
        ("effectiveness","eff"),("impact of","eff"),("effectiveness of","eff"),("efficacy","eff"),
        ("VE","eff"),("CI","eff"),("RR","eff"),("OR","eff"),("RD","eff"),("rate difference","eff"),
        ("odds ratios","eff"),("odds ratio (OR)","eff"),("odds ratios (ORs)","eff"),
        ("IRR","eff"),("relative risks(RR)","eff"),("relative risks","eff"),
        ("efficacy rate","eff"),("effectiveness rate","eff"),("vaccine efficacy","eff"),
        ("hazard ratio","eff"),("HR","eff"),("risk ratio","eff"),("rate ratio","eff"),
        ("adjusted","eff"),("propensity score","eff"),
        ("did not effectively","eff"),("no effect","eff"),("not effective","eff"),
        ("pooled","eff")
    ],
    "saf":[("safety","saf"),("adverse effects","saf"),("adverse events","saf")],
    "rf":[("risk factor","rf"),("risk","rf")],
    "cov":[("coverage","cov"),("uptake","cov"),("the uptake","cov"),("actual uptake","cov"),("vaccine uptake","cov")],
    "kaa":[
        ("acceptance","kaa"),("Barrier","kaa"),("vaccine barriers","kaa"),
        ("knowledge","kaa"),("vaccination willingness and intentions","kaa"),
        ("HPV vaccine acceptability","kaa"),
        ("Awareness and knowledge","kaa"),("Awareness","kaa"),
        ("facilitators of and barriers","kaa"),
        ("awareness,knowledge, acceptability, and intention","kaa"),
        ("knowledge and acceptability","kaa"),("knowledge and awareness","kaa"),
        ("attitudes and beliefs","kaa"),("Knowledge and Attitude","kaa"),
        ("attitude","kaa"),("knowledge, awareness, and attitude","kaa")
    ],
    "adm":[
        ("administration","adm"),("vaccine types","adm"),("dose schedules","adm"),
        ("vaccine types and dose schedules","adm"),("different dose schedules","adm"),
        ("Two doses of","adm")
    ],
    "eco":[
        ("economic","eco"),("cost","eco"),("financial","eco"),("economic impact","eco"),
        ("cost effectiveness","eco"),("cost-effectiveness","eco"),("economic evaluation","eco"),
        ("Cost-effectiveness of HPV vaccination strategies","eco")
    ],
    "mod":[("modeling","mod")],
    "eth":[
        ("racial","eth"),("ethnic","eth"),("ethnic minority","eth"),("racial minority","eth"),
        ("racial/ethnic","eth"),("racial/ethnic minority","eth"),("racial disparity","eth"),
        ("ethnic disparity","eth"),("minority","eth"),("minority population","eth")
    ]
}

INTERVENTION_VPD = {
    "covid":[("COVID-19","covid"),("COVID","covid"),("COVID 19","covid"),("SARS-CoV-2","covid")],
    "aden":[("Adenovirus","aden")],
    "anth":[("anthrax","anth")],
    "camp":[("Campylobacter","camp")],
    "chol":[("Cholera","chol"),("Vibrio cholerae","chol"),("cholerae","chol")],
    "Q__fever":[("Q fever","coxi"),("Coxiella burnetii","coxi"),("Q-fever","coxi")],
    "diph":[("Diphtheria","diph")],
    "ebol":[("Ebola","ebol")],
    "ente":[("Enterovirus","ente")],
    "esch":[("Escherichia coli (ETEC)","esch")],
    "hib":[("Haemophilus influenzae type b","hib")],
    "ha":[("Hepatitis A","ha")],
    "hb":[("Hepatitis B","hb")],
    "hc":[("Hepatitis C","hc")],
    "he":[("Hepatitis E","he")],
    "hs":[("Herpes simplex","hs")],
    "hz":[("Shingles, Herpes zoster","hz")],
    "hiv":[("HIV / AIDS","hiv"),("HIV","hiv"),("AIDS","hiv"),("HIV/AIDS","hiv")],
    "hpv":[("Human papillomavirus","hpv"),("HPV","hpv")],
    "je":[("Japanese encephalitis","je")],
    "leis":[("Leishmaniasis","leis"),("Leishmania","leis")],
    "lyme":[("Lyme disease, Borreliosis","lyme"),("Borreliosis","lyme"),("Lyme disease","lyme")],
    "mala":[("Malaria","mala"),("Plasmodia","mala")],
    "meas":[("Measles","meas")],
    "meni":[("Meningococcal","meni"),("Neisseria","meni")],
    "mump":[("Mumps","mump")],
    "leprosy":[("Leprosy","myle"),("Mycobacterium leprae","myle")],
    "myva":[("Mycobacterium vaccae","myva")],
    "pert":[("Pertussis","pert"),("Bordetella","pert")],
    "plag":[("Plague","plag")],
    "pneu":[("Pneumococcal","pneu")],
    "poli":[("Poliovirus","poli"),("Poliomyelitis","poli")],
    "pseu":[("Pseudomonas aeruginosa","pseu")],
    "rabies":[("Rabies","rabi")],
    "rsv":[("Respiratory syncytial virus","rsv"),("rsv","rsv")],
    "rubella":[("Rubella","rube")],
    "salm":[("Salmonella","salm")],
    "shig":[("Shigella","shig")],
    "smal":[("Smallpox","smal"),("Variola","smal")],
    "strb":[("Streptococcus group B","strb")],
    "tetanus":[("Tetanus","tt")],
    "tbe":[("Tick-borne encephalitis","tbe")],
    "tb":[("Tuberculosis","tb")],
    "typh":[("Typhoid","typh")],
    "vari":[("Varicella, Chickenpox","vari")],
    "yf":[("Yellow fever","yf")],
    "zika":[("Zika","zika")],
    "infl":[("influenza","infl")],
    "deng":[("dengue","deng")],
    "rota":[("rotavirus","rota")]
}

AGE_GROUP_DICT = {
    "nb_0__1":[('newborn','nb'),('babies','nb'),('baby','nb'),('infant','nb'),
               ('toddlers','nb'),('young ones','nb'),('youngsters','nb'),('small children','nb')],
    "chi_2__9":[("child","chi"),("children","chi")],
    "ado_10__17":[("adolescents","ado"),("adolescent","ado"),("young adults","ado")],
    "adu_18__64":[("adults","adu"),("adult","adu")],
    "eld_65__10000":[("elderly","eld"),("older adults","eld")]
}
SPECIFIC_GROUP_DICT = {
    "hcw":[
        ("Physician","hcw"),("Nurse","hcw"),("Surgeon","hcw"),("Dentist","hcw"),("Pharmacist","hcw"),
        ("Physical Therapist","hcw"),("Occupational Therapist","hcw"),
        ("Medical Laboratory Technologist","hcw"),("Radiologist","hcw"),
        ("Dietitian/Nutritionist","hcw"),("Respiratory Therapist","hcw"),
        ("Speech-Language Pathologist","hcw"),("Physician Assistant","hcw"),
        ("Nurse Practitioner","hcw"),("Certified Nursing Assistant (CNA)","hcw"),
        ("Medical Assistant","hcw"),("Paramedic/EMT","hcw"),("Midwife","hcw"),
        ("Psychologist","hcw"),("Social Worker (Clinical)","hcw"),
        ("Hospital Administrator","hcw"),("Medical Researcher","hcw"),
        ("Health Educator","hcw"),("Orthopedic Technician","hcw"),
        ("Optometrist","hcw"),("Podiatrist","hcw"),("Anesthesiologist","hcw"),
        ("Neurologist","hcw"),("Cardiologist","hcw"),("Gastroenterologist","hcw")
    ],
    "pw":[("pregnant","pw"),("pregnant women","pw")],
    "tra":[("traveller","tra")],
    "pcg":[("parents","cg"),("caregivers","cg")]
}
IMMUNE_STATUS_DICT = {"imu":[("immunocompromised","imu")], "hty":[("healthy","hty")]}

# ---------------- dataset types ----------------

@dataclass
class MinedExample:
    query: str
    positive: str
    negatives: List[str]

# ---------------- builder ----------------

class SRTrainingSetBuilder:
    """
    JSONL weakly-labeled dataset builder for systematic reviews.
    (Now includes regions as geo targets in addition to countries.)
    """

    def __init__(self,
                 embedder_name: Optional[str] = "sentence-transformers/all-MiniLM-L6-v2",
                 device: str = "cpu",
                 seed: int = 13,
                 use_pycountry: bool = True):
        self.rng = random.Random(seed)
        self.embedder = None
        if embedder_name:
            self.embedder = self._build_embedder(embedder_name, device)

        # Query families (with paraphrases)
        self.queries = {
            "studies": [
                "How many studies were included?",
                "Report the total number of included studies.",
                "What is the count of studies in the review?",
                "Give the number of included articles."
            ],
            "date": [
                "What is the last literature search date?",
                "When was the literature search last performed?",
                "Up to what date was the search conducted?",
                "What is the final search date?"
            ],
            "country": [
                "Give country counts or sample sizes by country.",
                "List study counts per country and n per country.",
                "Provide country-wise sample sizes (n=) and counts.",
                "What are the per-country totals?"
            ],
            "region": [
                "Give region counts or sample sizes by region.",
                "List study counts per region and n per region.",
                "Provide region-wise sample sizes (n=) and counts.",
                "What are the per-region totals?"
            ],
            "intervention_any": [
                "Does this appear to be an intervention/effectiveness review?",
                "Are there intervention or RCT signals in the text?",
                "Identify intervention-effectiveness phrasing.",
                "Is this comparing vaccinated vs unvaccinated?"
            ],
            "design_any": [
                "List study designs identified.",
                "What study designs are mentioned (RCT, cohort, case-control, cross-sectional)?",
                "Extract study design indicators.",
                "Which designs (prospective, retrospective, longitudinal) appear?"
            ],
            "design_counts": [
                "Extract counts per study design (RCT/cohort/case-control/cross-sectional).",
                "Report numeric counts for each study design.",
                "How many RCTs, cohort, case-control, and cross-sectional studies?"
            ],
            "topic": [
                "Detect topic mentions (efficacy/effectiveness/safety/risk/coverage/acceptance/etc.).",
                "Which topic indicators are present?",
                "Identify effect/safety/risk/coverage/acceptance-related text."
            ],
            "outcome_infection": ["Find infection outcome mentions.","Are infections reported as outcomes?","Locate infection-related endpoints."],
            "outcome_hospital": ["Find hospitalization outcome mentions.","Are hospitalizations reported as outcomes?","Locate hospital admission endpoints."],
            "outcome_icu": ["Find ICU outcome mentions.","Are ICU/critical-care outcomes reported?","Locate intensive care endpoints."],
            "outcome_death": ["Find mortality outcome mentions.","Are deaths/mortality reported as outcomes?","Locate fatality endpoints."],
            "age_numeric": ["Extract numeric age ranges or thresholds.","What numeric ages are included?","Find ages like 5–17 years, <2 years, >65 years."],
            "age_groups": ["Find age-group mentions (infants/children/adolescents/adults/elderly).","Which age groups are referenced?","Extract semantic age-group signals."],
            "specific_groups": ["Detect specific participant groups (HCW, pregnant women, travellers, parents/caregivers).","Which specific groups are mentioned?","Identify healthcare worker and special population signals."],
            "immune_status": ["Detect immune status signals (immunocompromised/healthy).","Which immune-status groups are present?","Identify immunocompromised/healthy mentions."]
        }

        for key, syns in INTERVENTION_VPD.items():
            disease_name = syns[0][0]
            self.queries[f"intervention_{key}"] = [
                f"Identify intervention signals for {disease_name}.",
                f"Is there an intervention or vaccine context for {disease_name}?",
                f"Extract {disease_name}-specific intervention/effectiveness phrasing."
            ]

        # Countries (expand via pycountry if available)
        self.countries = set(_BASE_COUNTRIES)
        if use_pycountry and _pycountry is not None:
            try:
                self.countries = {c.name for c in _pycountry.countries}
                self.countries.update({"United States", "United Kingdom", "South Korea", "Czech Republic"})
            except Exception:
                pass

        # Regions (normalized title case set)
        self.regions = set(_REGIONS)

    # ---------- public API ----------

    
    
    def _build_embedder(self, name: str, device: str):
        """
        Build a SentenceTransformer for either a native ST model (e.g. all-MiniLM-L6-v2)
        or a raw HF encoder (e.g. allenai/specter2_base) with explicit Pooling.
        """
        name = name.strip()
        # Fast path: native ST models load directly
        try:
            if any(tag in name.lower() for tag in [
                "all-mpnet", "all-minilm", "multi-qa", "msmarco", "gte-"
            ]):
                return SentenceTransformer(name, device=device)
        except Exception:
            pass

        # Fallback: construct from HF backbone (e.g., allenai/specter2_base)
        try:
            transformer = models.Transformer(name, max_seq_length=512)   # Specter2 is 768-d, 512 tokens
            pooling = models.Pooling(
                transformer.get_word_embedding_dimension(),
                pooling_mode_cls_token=False,
                pooling_mode_mean_tokens=True,
                pooling_mode_max_tokens=False,
            )
            norm = models.Normalize()  # L2 normalize embeddings (good for cosine)
            return SentenceTransformer(modules=[transformer, pooling, norm], device=device)
        except Exception as e:
            # last resort: let ST try to wrap it (may warn)
            return SentenceTransformer(name, device=device)


    def build_from_dir(self, txt_dir: str, out_jsonl: str,
                       per_doc_limit: Optional[int] = None,
                       add_augmentations: bool = True,
                       semantic_negs_k: int = 4) -> None:
        files = [os.path.join(txt_dir, f) for f in os.listdir(txt_dir) if f.lower().endswith(".txt")]
        all_items: List[MinedExample] = []
        slot_counts = defaultdict(int)

        for path in files:
            text = self._read_text(path)
            if not text:
                continue
            sents = _split_sentences(text)

            items: List[MinedExample] = []
            items += self._mine_studies_total(sents, add_augmentations)
            items += self._mine_last_search_date(sents)

            items += self._mine_countries(sents)
            items += self._mine_regions(sents)  # NEW

            items += self._mine_intervention_any(sents)
            items += self._mine_intervention_per_disease(sents)

            items += self._mine_design_any(sents)
            items += self._mine_design_counts(sents)

            items += self._mine_topics(sents)
            items += self._mine_outcomes(sents)

            items += self._mine_age_numeric(sents)
            items += self._mine_age_groups(sents)
            items += self._mine_specific_groups(sents)
            items += self._mine_immune_status(sents)

            if self.embedder and semantic_negs_k > 0 and items:
                items = self._attach_semantic_negatives(items, sents, k=semantic_negs_k)

            if per_doc_limit and len(items) > per_doc_limit:
                items = self.rng.sample(items, per_doc_limit)

            for it in items:
                for fam, qlist in self.queries.items():
                    if it.query in qlist:
                        slot_counts[fam] += 1
                        break
            all_items.extend(items)

        dedup = {(x.query, x.positive): x for x in all_items}
        final_items = list(dedup.values())
        self.rng.shuffle(final_items)

        with open(out_jsonl, "w", encoding="utf-8") as f:
            for ex in final_items:
                f.write(json.dumps({"query": ex.query, "positive": ex.positive, "negatives": ex.negatives}, ensure_ascii=False) + "\n")

        print(f"Wrote {len(final_items)} training rows to {out_jsonl}")
        print("Slot counts (before dedupe):", dict(slot_counts))

    # ---------- mining slots ----------

    def _emit(self, family_key: str, pos: str, negs: List[str]) -> List[MinedExample]:
        return [MinedExample(query=q, positive=pos, negatives=list(negs)) for q in self.queries[family_key]]

    def _mine_studies_total(self, sents: List[str], add_aug: bool) -> List[MinedExample]:
        out = []
        for s in sents:
            if _DECOYS.search(s): 
                continue
            if not _INCLUSION_VERBS.search(s):
                continue
            m = _PAT_STUDIES_A.search(s) or _PAT_STUDIES_B.search(s)
            if not m:
                continue
            n = _int_flexible(m.group(1))
            if n is None:
                continue
            pos = _normalize_spaces(s)
            negs = self._make_rule_negatives(sents)
            out.extend(self._emit("studies", pos, negs))
            if add_aug:
                aug = self._augment_number_in_sentence(pos, n)
                if aug and aug != pos:
                    out.extend(self._emit("studies", aug, negs))
        return out

    def _mine_last_search_date(self, sents: List[str]) -> List[MinedExample]:
        out = []
        for s in sents:
            if not _PAT_DATE_CONTEXT.search(s):
                continue
            if _PAT_DATE.search(s):
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("date", pos, negs))
        return out

    def _normalize_country(self, name: str) -> str:
        key = (name or "").strip().lower().replace(".", "")
        if key in _COUNTRY_ALIASES:
            return _COUNTRY_ALIASES[key]
        return " ".join(w.capitalize() for w in name.strip().split())

    def _normalize_region(self, name: str) -> str:
        key = (name or "").strip().lower()
        key = key.replace(".", "").replace("–","-").replace("—","-").replace("−","-")
        if key in _REGION_ALIASES:
            return _REGION_ALIASES[key]
        # title case normalize
        norm = " ".join(w.capitalize() for w in re.split(r"\s+", name.strip()))
        # fix hyphenated common ones
        norm = norm.replace("Sub Saharan Africa","Sub-Saharan Africa")
        norm = norm.replace("South East Asia","Southeast Asia")
        return norm

    def _mine_countries(self, sents: List[str]) -> List[MinedExample]:
        out = []
        for s in sents:
            matched = False
            for m in _PAT_COUNTRY_SAMPLE_N.finditer(s):
                ctry_raw, n_raw = m.group(1), m.group(2)
                ctry = self._normalize_country(ctry_raw)
                if ctry in self.countries and _strip_commas_to_int(n_raw) is not None:
                    matched = True; break
            if not matched:
                for m in _PAT_COUNTRY_STUDYCOUNT.finditer(s):
                    ctry_raw, n_raw = m.group(1), m.group(2)
                    ctry = self._normalize_country(ctry_raw)
                    if ctry in self.countries and _int_flexible(n_raw) is not None:
                        matched = True; break
            if matched:
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("country", pos, negs))
        return out

    def _mine_regions(self, sents: List[str]) -> List[MinedExample]:
        """
        Regions mirror the country patterns:
          • Region (n = 12,345)
          • Region (10)
        """
        out = []
        region_set = set(self.regions)  # normalized canonical names
        # Build a fast matcher for raw names & aliases
        region_name_variants = set(region_set)
        region_name_variants.update(_REGION_ALIASES.keys())

        # Quick presence check: only scan sentences that mention any region name/alias
        region_presence = re.compile("|".join(re.escape(x) for x in sorted(region_name_variants, key=len, reverse=True)), re.I)

        for s in sents:
            if not region_presence.search(s):
                continue

            matched = False
            # sample sizes
            for m in _PAT_REGION_SAMPLE_N.finditer(s):
                reg_raw, n_raw = m.group(1), m.group(2)
                reg = self._normalize_region(reg_raw)
                if reg in region_set and _strip_commas_to_int(n_raw) is not None:
                    matched = True; break
            # counts
            if not matched:
                for m in _PAT_REGION_STUDYCOUNT.finditer(s):
                    reg_raw, n_raw = m.group(1), m.group(2)
                    reg = self._normalize_region(reg_raw)
                    if reg in region_set and _int_flexible(n_raw) is not None:
                        matched = True; break

            if matched:
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("region", pos, negs))
        return out

    def _mine_intervention_any(self, sents: List[str]) -> List[MinedExample]:
        out = []
        for s in sents:
            if _DECOYS.search(s): 
                continue
            if _PAT_INTERV_CORE.search(s) or _PAT_INTERV_VS.search(s):
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("intervention_any", pos, negs))
        return out

    def _mine_intervention_per_disease(self, sents: List[str]) -> List[MinedExample]:
        out = []
        for key, syns in INTERVENTION_VPD.items():
            term_re = re.compile("|".join(re.escape(t[0]) for t in syns), re.I)
            for s in sents:
                if _DECOYS.search(s): 
                    continue
                if term_re.search(s) and (_PAT_INTERV_CORE.search(s) or _PAT_INTERV_VS.search(s)):
                    pos = _normalize_spaces(s)
                    negs = self._make_rule_negatives(sents)
                    out.extend(self._emit(f"intervention_{key}", pos, negs))
        return out

    def _mine_design_any(self, sents: List[str]) -> List[MinedExample]:
        out = []
        combo = re.compile("|".join(p.pattern for plist in _DESIGN_PATTERNS.values() for p in plist), re.I)
        for s in sents:
            if _DECOYS.search(s): 
                continue
            if combo.search(s):
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("design_any", pos, negs))
        return out

    def _mine_design_counts(self, sents: List[str]) -> List[MinedExample]:
        out = []
        for s in sents:
            if _DECOYS.search(s):
                continue
            counts_found = False
            for _, rgxs in _DESIGN_PATTERNS.items():
                for rg in rgxs:
                    m = rg.search(s)
                    if not m:
                        continue
                    if m.groups():
                        n = _int_flexible(m.group(1))
                        if n is not None:
                            counts_found = True
                            break
                if counts_found:
                    break
            if counts_found:
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("design_counts", pos, negs))
        return out

    def _mine_topics(self, sents: List[str]) -> List[MinedExample]:
        out = []
        topic_terms = [t for lst in TOPIC_DICT.values() for (t,_) in lst]
        topic_re = re.compile(r"|".join(re.escape(t) for t in topic_terms), re.I)
        for s in sents:
            if _DECOYS.search(s): 
                continue
            if topic_re.search(s):
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("topic", pos, negs))
        return out

    def _mine_outcomes(self, sents: List[str]) -> List[MinedExample]:
        out = []
        for s in sents:
            if _DECOYS.search(s): 
                continue
            if _PAT_OUTCOME_INF.search(s):
                out.extend(self._emit("outcome_infection", _normalize_spaces(s), self._make_rule_negatives(sents)))
            if _PAT_OUTCOME_HOS.search(s):
                out.extend(self._emit("outcome_hospital", _normalize_spaces(s), self._make_rule_negatives(sents)))
            if _PAT_OUTCOME_ICU.search(s):
                out.extend(self._emit("outcome_icu", _normalize_spaces(s), self._make_rule_negatives(sents)))
            if _PAT_OUTCOME_DEA.search(s):
                out.extend(self._emit("outcome_death", _normalize_spaces(s), self._make_rule_negatives(sents)))
        return out

    def _mine_age_numeric(self, sents: List[str]) -> List[MinedExample]:
        out = []
        for s in sents:
            if _DECOYS.search(s):
                continue
            if _PAT_AGE_NUMERIC.search(s):
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("age_numeric", pos, negs))
        return out

    def _mine_age_groups(self, sents: List[str]) -> List[MinedExample]:
        out = []
        for s in sents:
            if _DECOYS.search(s):
                continue
            if _PAT_AGE_GROUPS.search(s):
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("age_groups", pos, negs))
        return out

    def _mine_specific_groups(self, sents: List[str]) -> List[MinedExample]:
        out = []
        terms = [t for lst in SPECIFIC_GROUP_DICT.values() for (t,_) in lst]
        rg = re.compile("|".join(re.escape(t) for t in terms), re.I)
        for s in sents:
            if _DECOYS.search(s):
                continue
            if rg.search(s):
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("specific_groups", pos, negs))
        return out

    def _mine_immune_status(self, sents: List[str]) -> List[MinedExample]:
        out = []
        terms = [t for lst in IMMUNE_STATUS_DICT.values() for (t,_) in lst]
        rg = re.compile("|".join(re.escape(t) for t in terms), re.I)
        for s in sents:
            if _DECOYS.search(s):
                continue
            if rg.search(s):
                pos = _normalize_spaces(s)
                negs = self._make_rule_negatives(sents)
                out.extend(self._emit("immune_status", pos, negs))
        return out

    # ---------- negatives ----------

    def _make_rule_negatives(self, sents: List[str], max_neg: int = 6) -> List[str]:
        pool = []
        for pat in _NEG_TEMPLATES:
            rg = re.compile(pat, flags=re.I)
            pool.extend([_normalize_spaces(s) for s in sents if rg.search(s)])
        pool.extend([_normalize_spaces(s) for s in sents if re.search(r"\b(PRISMA|flow diagram|risk of bias|table \d+)\b", s, flags=re.I)])
        candidates = [x for x in sents if x and len(x) > 20]
        self.rng.shuffle(candidates)
        pool.extend(_normalize_spaces(x) for x in candidates[:max_neg*2])

        uniq, seen = [], set()
        for p in pool:
            if p not in seen:
                seen.add(p); uniq.append(p)
            if len(uniq) >= max_neg:
                break
        return uniq

    def _attach_semantic_negatives(self, items: List[MinedExample], sents: List[str], k:int=4) -> List[MinedExample]:
        clean_sents = [_normalize_spaces(s) for s in sents if s and s.strip()]
        if not clean_sents or not self.embedder:
            return items
        try:
            doc_embs = self.embedder.encode(clean_sents, convert_to_tensor=True, normalize_embeddings=True)
        except Exception:
            return items

        out = []
        for ex in items:
            try:
                q_emb = self.embedder.encode([ex.positive], convert_to_tensor=True, normalize_embeddings=True)[0]
                sims = util.cos_sim(doc_embs, q_emb).cpu().numpy().reshape(-1)
                idx = sims.argsort()[::-1]
                added = 0
                for i in idx:
                    cand = clean_sents[i]
                    if cand == ex.positive: 
                        continue
                    if self._looks_like_positive_for_any_slot(cand):
                        continue
                    ex.negatives.append(cand)
                    added += 1
                    if added >= k: break
            except Exception:
                pass
            ex.negatives = list(dict.fromkeys(ex.negatives))[:10]
            out.append(ex)
        return out

    # ---------- helpers ----------

    def _looks_like_positive_for_any_slot(self, s:str)->bool:
        if _PAT_DATE.search(s) and _PAT_DATE_CONTEXT.search(s):
            return True
        if (_PAT_STUDIES_A.search(s) or _PAT_STUDIES_B.search(s)) and not _DECOYS.search(s):
            return True
        combo = re.compile("|".join(p.pattern for plist in _DESIGN_PATTERNS.values() for p in plist), re.I)
        if combo.search(s):
            return True
        if _PAT_OUTCOME_INF.search(s) or _PAT_OUTCOME_HOS.search(s) or _PAT_OUTCOME_ICU.search(s) or _PAT_OUTCOME_DEA.search(s):
            return True
        if _PAT_AGE_NUMERIC.search(s) or _PAT_AGE_GROUPS.search(s):
            return True
        topic_terms = [t for lst in TOPIC_DICT.values() for (t,_) in lst]
        if re.search("|".join(re.escape(t) for t in topic_terms), s, re.I):
            return True
        if _PAT_COUNTRY_SAMPLE_N.search(s) or _PAT_COUNTRY_STUDYCOUNT.search(s):
            if any(re.search(rf"\b{re.escape(cty)}\b", s) for cty in self.countries):
                return True
            if any(re.search(rf"\b{re.escape(alias)}\b", s, flags=re.I) for alias in _COUNTRY_ALIASES.keys()):
                return True
        # Regions
        if _PAT_REGION_SAMPLE_N.search(s) or _PAT_REGION_STUDYCOUNT.search(s):
            if any(re.search(rf"\b{re.escape(r)}\b", s) for r in self.regions):
                return True
            if any(re.search(rf"\b{re.escape(alias)}\b", s, flags=re.I) for alias in _REGION_ALIASES.keys()):
                return True
        if _PAT_INTERV_CORE.search(s) or _PAT_INTERV_VS.search(s):
            return True
        for key, syns in INTERVENTION_VPD.items():
            if re.search("|".join(re.escape(t[0]) for t in syns), s, re.I):
                return True
        return False

    def _augment_number_in_sentence(self, s:str, n:int)->Optional[str]:
        if str(n) in s:
            return s.replace(str(n), verbalize_int(n))
        words = verbalize_int(n)
        if re.search(re.escape(words), s, flags=re.I):
            return re.sub(re.escape(words), str(n), s, flags=re.I)
        return None

    def _read_text(self, path:str)->str:
        for enc in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                with open(path, "r", encoding=enc) as f:
                    return f.read()
            except Exception:
                continue
        return ""


