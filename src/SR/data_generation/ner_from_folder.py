from __future__ import annotations

import argparse
import json
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

# ============================================================
# Labels
# ============================================================
ATOMIC_LABELS: List[str] = [
    "REVIEW_TYPE","STUDY_DESIGN","N_STUDIES","DATE_OF_LAST_LITERATURE_SEARCH","QUALITY_TOOL",
    "PATHOGEN","CANCER","CONDITION","HPV_TYPE","AGE_GROUP","GENDER","SPECIAL_POP","RISK_GROUP",
    "SAFETY","ACCEPTANCE","EFFICACY","IMMUNOGENICITY","COVERAGE","ECONOMIC","ADMINISTRATION",
    "ETHICAL","LOGISTICS","MODELLING","CLINICAL","LESION","COUNTRY","REGION","WHO_REGION",
    "INCOME_GROUP","VACCINE_TYPE","VACCINE_BRAND","DOSE","ROUTE","PROGRAM","COMPONENT",
    "SCREENING","COMBINATION","BARRIER","FACILITATOR","DATABASE","SEARCH_TERMS","INCLUSION",
    "ANALYSIS","PERIOD","FOLLOWUP","TIMING","SAMPLE_SIZE","PERCENT","COST","QALY","ICER",
    "EFFECT_MEASURE","EFFECT_VALUE","CI","PVALUE",
]
LABEL_SET = set(ATOMIC_LABELS)
LABELS_STR = ", ".join(ATOMIC_LABELS)

# ============================================================
# Label definitions (compact, span-focused)
# ============================================================
LABEL_DEFS: Dict[str, str] = {
    "REVIEW_TYPE": "Type of review. e.g., 'systematic review', 'scoping review', 'meta-analysis', 'rapid review'.",
    "STUDY_DESIGN": "Design of included primary studies. e.g., 'randomized controlled trial', 'cohort study', 'case-control'.",
    "N_STUDIES": "Count of included studies. e.g., '23 studies', '15 trials', 'n=12 studies'.",
    "DATE_OF_LAST_LITERATURE_SEARCH": "Date the search was last run/updated. e.g., 'searched up to March 2023'.",
    "QUALITY_TOOL": "Quality / risk-of-bias tool. e.g., 'GRADE', 'AMSTAR 2', 'ROBINS-I', 'Cochrane RoB 2'.",
    "DATABASE": "Database name. e.g., 'PubMed', 'MEDLINE', 'Embase', 'Scopus', 'Web of Science'.",
    "SEARCH_TERMS": "Explicit search strings/keywords. e.g., '\"HPV\" AND vaccine'.",
    "INCLUSION": "Eligibility/inclusion/exclusion criteria text. e.g., 'included studies with...'.",
    "ANALYSIS": "Analysis methods. e.g., 'random-effects model', 'meta-regression', 'subgroup analysis'.",
    "PERIOD": "Time period range. e.g., '2000–2020', 'between 2012 and 2018'.",
    "FOLLOWUP": "Follow-up duration. e.g., '12 months follow-up', 'followed for 5 years'.",
    "TIMING": "Timing of vaccination/measurement. e.g., '6 weeks post-vaccination', 'before sexual debut'.",

    "PATHOGEN": "Infectious agent name (general). Prefer HPV_TYPE if a specific genotype is given.",
    "HPV_TYPE": "Specific HPV genotype. e.g., 'HPV-16', 'HPV 18', 'HPV-6/11'.",
    "CANCER": "Cancer type. e.g., 'cervical cancer', 'oropharyngeal cancer'.",
    "CONDITION": "Non-cancer condition. e.g., 'genital warts', 'HIV infection'.",
    "LESION": "Pre-cancer lesion terms. e.g., 'CIN2+', 'CIN3', 'HSIL', 'LSIL'.",
    "AGE_GROUP": "Age group/range. e.g., 'girls aged 9–14', 'adolescents', '≥18 years'.",
    "GENDER": "Sex/gender group. e.g., 'women', 'men', 'female', 'male'.",
    "SPECIAL_POP": "Named special populations. e.g., 'pregnant women', 'MSM', 'people living with HIV'.",
    "RISK_GROUP": "Risk descriptors. e.g., 'high-risk', 'immunocompromised', 'sexually active'.",
    "COUNTRY": "Country names.",
    "REGION": "Subnational region/state/province/city.",
    "WHO_REGION": "WHO region name/acronym. e.g., 'AFRO', 'EURO', 'WHO African Region'.",
    "INCOME_GROUP": "World Bank income group. e.g., 'low-income', 'upper-middle-income'.",

    "VACCINE_TYPE": "Vaccine valency/type. e.g., 'bivalent', 'quadrivalent', '9-valent'.",
    "VACCINE_BRAND": "Brand/product name. e.g., 'Gardasil', 'Cervarix', 'Gardasil 9'.",
    "DOSE": "Dose expression/schedule. e.g., 'two-dose schedule', '3 doses', '0.5 mL'.",
    "ROUTE": "Route of administration. e.g., 'intramuscular', 'IM', 'subcutaneous'.",
    "PROGRAM": "Program delivery. e.g., 'school-based program', 'national immunization program'.",
    "COMPONENT": "Program component. e.g., 'reminder letters', 'provider training', 'cold chain'.",
    "SCREENING": "Screening intervention. e.g., 'Pap smear', 'HPV DNA testing', 'VIA'.",
    "COMBINATION": "Combined interventions. e.g., 'vaccination plus screening'.",
    "ADMINISTRATION": "Administration logistics. e.g., 'schedule', 'delivery', 'co-administration'.",

    "SAFETY": "Safety/adverse event outcome wording. e.g., 'adverse events', 'serious adverse events'.",
    "ACCEPTANCE": "Willingness/acceptability wording. e.g., 'acceptability', 'intention to vaccinate'.",
    "COVERAGE": "Coverage/uptake wording. e.g., 'coverage', 'uptake rate', 'vaccination rate'.",
    "EFFICACY": "Efficacy/effectiveness outcome wording. e.g., 'vaccine efficacy', 'effectiveness against'.",
    "IMMUNOGENICITY": "Immune response outcomes. e.g., 'seroconversion', 'antibody titers', 'GMT'.",
    "CLINICAL": "Clinical outcomes/clinical endpoints (generic label when explicitly stated).",
    "MODELLING": "Model-based analysis language. e.g., 'Markov model', 'dynamic transmission model'.",
    "ECONOMIC": "Economic evaluation framing. e.g., 'cost-effectiveness', 'budget impact'.",
    "ETHICAL": "Ethics-related issues. e.g., 'ethical considerations', 'consent', 'equity'.",
    "LOGISTICS": "Logistical constraints. e.g., 'cold-chain', 'supply', 'distribution'.",
    "BARRIER": "Barrier statements. e.g., 'cost', 'lack of access', 'hesitancy'.",
    "FACILITATOR": "Facilitator statements. e.g., 'free vaccination', 'provider recommendation'.",

    "SAMPLE_SIZE": "Sample size in participants/subjects. e.g., 'n=1200', '1200 participants'.",
    "PERCENT": "Percentages including '%'.",
    "COST": "Currency amounts. e.g., '$200', '€150', 'USD 20 million'.",
    "QALY": "QALY mentions. e.g., 'QALYs', 'quality-adjusted life-year'.",
    "ICER": "ICER mentions including units. e.g., '$12,000 per QALY'.",
    "EFFECT_MEASURE": "Effect measure name. e.g., 'odds ratio', 'risk ratio', 'hazard ratio'.",
    "EFFECT_VALUE": "Numeric value tied to an effect measure. e.g., 'OR 0.72', 'RR=1.2'.",
    "CI": "Confidence interval expression. e.g., '95% CI 0.8–1.2'.",
    "PVALUE": "P-value expression. e.g., 'p=0.03', 'P < 0.05'.",
}

def render_label_defs() -> str:
    lines = []
    for lab in ATOMIC_LABELS:
        d = LABEL_DEFS.get(lab, "")
        if d:
            lines.append(f"- {lab}: {d}")
    return "\n".join(lines)

# ============================================================
# Prompt (strong SR-oriented guidance)
# ============================================================
SYSTEM_NER_EXTRACT = f"""You are a domain expert annotating biomedical systematic review papers.

Task:
Extract candidate atomic entity mentions from the given document text.

Return exactly ONE JSON object with this shape:
{{
  "doc_id": "string",
  "candidates": [{{"text": "string", "label": "string"}}]
}}

Hard rules:
- Use ONLY these labels: {LABELS_STR}
- Candidate "text" must be an EXACT substring from the provided text (verbatim).
- Do NOT invent entities. If uncertain, omit.
- Prefer longer spans over shorter ones when they refer to one concept.
- Avoid duplicates (same text + same label).
- Output must be valid JSON only. No commentary.

Systematic-review heuristics:
- Look for Methods/Results style phrases: databases searched, eligibility criteria, designs, effect measures, CI, p-values.
- When multiple entities are present in one phrase, extract each as separate candidates.

Priority rules:
- If span is a specific HPV genotype (e.g., HPV-16), label HPV_TYPE (not PATHOGEN).
- For statistics: label EFFECT_MEASURE for the measure name, EFFECT_VALUE for the numeric value,
  CI for confidence interval text, PVALUE for p-values, PERCENT for percentages, COST/QALY/ICER for economics.
- Do not label bare numbers unless the label definition explicitly allows it.

Label guide:
{render_label_defs()}
"""

# ============================================================
# LLM client (OpenAI Chat or Responses; Ollama Responses)
# ============================================================
@dataclass(frozen=True)
class LLMConfig:
    base_url: str
    api_key: str
    model: str
    timeout_s: int = 120
    max_retries: int = 6
    temperature: float = 0.0

def extract_json_object(s: str) -> str:
    start = s.find("{")
    if start == -1:
        raise ValueError(f"No '{{' found in model output: {s[:300]}")
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        else:
            if ch == '"':
                in_str = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return s[start:i+1]
    raise ValueError(f"Unbalanced JSON braces in model output: {s[start:start+400]}")

class LLMClient:
    def __init__(self, cfg: LLMConfig):
        self.cfg = cfg

    def chat_json(self, system: str, user: str) -> Dict[str, Any]:
        endpoints = [
            ("openai_chat", self.cfg.base_url.rstrip("/") + "/v1/chat/completions"),
            ("responses", self.cfg.base_url.rstrip("/") + "/v1/responses"),
        ]
        last_err: Optional[Exception] = None
        for kind, url in endpoints:
            try:
                return self._call(kind, url, system, user)
            except Exception as e:
                last_err = e
        raise RuntimeError(f"LLM call failed (all endpoints): {last_err}")

    def _call(self, kind: str, url: str, system: str, user: str) -> Dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self.cfg.api_key and self.cfg.api_key.lower() != "dummy":
            headers["Authorization"] = f"Bearer {self.cfg.api_key}"

        if kind == "openai_chat":
            payload = {
                "model": self.cfg.model,
                "temperature": self.cfg.temperature,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "response_format": {"type": "json_object"},
            }
        else:
            payload = {
                "model": self.cfg.model,
                "input": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": self.cfg.temperature,
            }

        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                r = requests.post(url, json=payload, headers=headers, timeout=self.cfg.timeout_s)
                if r.status_code >= 400:
                    raise RuntimeError(f"{kind} {r.status_code}: {r.text[:1200]}")
                data = r.json()
                content = self._extract_text(kind, data).strip()
                return json.loads(extract_json_object(content))
            except Exception as e:
                if attempt == self.cfg.max_retries:
                    raise
                time.sleep(min(20.0, (2 ** (attempt - 1)) + random.random()))

        raise RuntimeError("unreachable")

    @staticmethod
    def _extract_text(kind: str, data: Dict[str, Any]) -> str:
        if kind == "openai_chat":
            return data["choices"][0]["message"]["content"]

        # Responses API typical
        texts: List[str] = []
        for item in (data.get("output") or []):
            for part in (item.get("content") or []):
                if isinstance(part, dict):
                    t = part.get("text")
                    if isinstance(t, str) and t.strip():
                        texts.append(t)
        if texts:
            return "\n".join(texts)

        # Fallbacks
        if isinstance(data.get("output_text"), str):
            return data["output_text"]
        if isinstance(data.get("message"), dict) and isinstance(data["message"].get("content"), str):
            return data["message"]["content"]
        return json.dumps(data)

# ============================================================
# Validation
# ============================================================
@dataclass(frozen=True)
class Candidate:
    text: str
    label: str

@dataclass(frozen=True)
class Span:
    start: int
    end: int
    label: str

def validate_extract_payload(obj: Dict[str, Any], doc_id_expected: str, chunk_text: str) -> List[Candidate]:
    if not isinstance(obj, dict):
        raise ValueError("extract: response is not a JSON object")
    if obj.get("doc_id") != doc_id_expected:
        raise ValueError(f"extract: doc_id mismatch (got {obj.get('doc_id')}, expected {doc_id_expected})")
    cands = obj.get("candidates")
    if not isinstance(cands, list):
        raise ValueError("extract: candidates is not a list")

    out: List[Candidate] = []
    seen = set()
    for c in cands:
        if not isinstance(c, dict):
            continue
        text = c.get("text")
        label = c.get("label")
        if not (isinstance(text, str) and text):
            continue
        if not (isinstance(label, str) and label in LABEL_SET):
            continue
        if text not in chunk_text:
            continue
        key = (text, label)
        if key in seen:
            continue
        seen.add(key)
        out.append(Candidate(text=text, label=label))
    return out

# ============================================================
# Chunking (SR papers: keep chunks moderate)
# ============================================================
@dataclass(frozen=True)
class Chunk:
    start: int
    end: int
    text: str

def chunk_text(text: str, max_chars: int = 4000, overlap: int = 400) -> List[Chunk]:
    if len(text) <= max_chars:
        return [Chunk(0, len(text), text)]
    chunks: List[Chunk] = []
    i = 0
    while i < len(text):
        j = min(len(text), i + max_chars)
        k = text.rfind("\n", i, j)
        if k != -1 and (j - k) < 1500:
            j = k
        chunks.append(Chunk(i, j, text[i:j]))
        if j == len(text):
            break
        i = max(0, j - overlap)
    return chunks

# ============================================================
# Rule-based "easy wins" (boost recall + reduce LLM burden)
# ============================================================
_RE_RULES: List[Tuple[str, str]] = [
    ("HPV_TYPE", r"\bHPV[\s-]?(?:\d{1,2})(?:/\d{1,2})?\b"),  # HPV-16, HPV 18, HPV-6/11
    ("PVALUE", r"\b[pP]\s*[=<]\s*0?\.\d+\b|\b[pP]\s*=\s*0?\.\d+\b"),
    ("CI", r"(?:\b95%\s*CI\b|\bCI\b|confidence interval)\s*[:\(]?\s*[-–—]?\s*\d+(?:\.\d+)?\s*(?:to|[-–—])\s*\d+(?:\.\d+)?\s*\)?"),
    ("PERCENT", r"\b\d{1,3}(?:\.\d+)?\s*%\b"),
    ("ICER", r"\bICER\b.*?(?:per\s+QALY|/QALY)\b"),
    ("QALY", r"\bQALY(?:s)?\b|quality-adjusted life-?year(?:s)?"),
    ("COST", r"(?:USD|EUR|GBP|CAD|AUD)\s*\$?\s*\d[\d,]*(?:\.\d+)?|\$\s*\d[\d,]*(?:\.\d+)?|€\s*\d[\d,]*(?:\.\d+)?"),
    ("EFFECT_MEASURE", r"\b(odds ratio|risk ratio|hazard ratio|rate ratio|prevalence ratio)\b|\b(OR|RR|HR)\b"),
    ("EFFECT_VALUE", r"\b(?:OR|RR|HR)\s*[=:]?\s*\d+(?:\.\d+)?\b"),
]

def regex_candidates(text: str) -> List[Candidate]:
    out: List[Candidate] = []
    seen = set()
    for label, pat in _RE_RULES:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            s = m.group(0)
            if not s or s.isspace():
                continue
            key = (s, label)
            if key in seen:
                continue
            seen.add(key)
            out.append(Candidate(text=s, label=label))
    return out

# ============================================================
# Grounding + overlap resolution with label-aware occurrence scoring
# ============================================================
_LABEL_HINTS = {
    "PVALUE": [r"\bp\s*[=<]", r"\bP\s*[=<]"],
    "CI": [r"\bCI\b", r"confidence interval", r"\b95%\s*CI\b"],
    "EFFECT_MEASURE": [r"odds ratio", r"risk ratio", r"hazard ratio", r"\bOR\b", r"\bRR\b", r"\bHR\b"],
    "EFFECT_VALUE": [r"\bOR\b", r"\bRR\b", r"\bHR\b", r"="],
    "DATABASE": [r"PubMed", r"MEDLINE", r"Embase", r"Scopus", r"Web of Science", r"Cochrane"],
    "QUALITY_TOOL": [r"GRADE", r"AMSTAR", r"ROBINS", r"RoB", r"Cochrane"],
    "N_STUDIES": [r"\bstudies\b", r"\btrials\b"],
    "ICER": [r"\bICER\b", r"per QALY", r"/QALY"],
    "QALY": [r"\bQALY\b", r"quality-adjusted"],
    "COST": [r"\$", r"€", r"\bUSD\b", r"\bEUR\b", r"\bGBP\b"],
}

def find_all(text: str, sub: str) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    if not sub:
        return out
    i = 0
    while True:
        j = text.find(sub, i)
        if j == -1:
            break
        out.append((j, j + len(sub)))
        i = j + 1
    return out

def choose_occurrence(text: str, sub: str, label: str) -> Optional[Tuple[int, int]]:
    occ = find_all(text, sub)
    if not occ:
        return None
    if len(occ) == 1:
        return occ[0]

    patterns = _LABEL_HINTS.get(label, [])
    if not patterns:
        return occ[0]

    best = occ[0]
    best_score = -1
    for (s, e) in occ:
        left = max(0, s - 90)
        right = min(len(text), e + 90)
        ctx = text[left:right]
        score = 0
        for pat in patterns:
            if re.search(pat, ctx, flags=re.IGNORECASE):
                score += 1
        # tie-breaker: earlier occurrence
        if score > best_score:
            best_score = score
            best = (s, e)
    return best

def spans_from_candidates(text: str, candidates: List[Candidate]) -> List[Span]:
    spans: List[Span] = []
    for c in candidates:
        pos = choose_occurrence(text, c.text, c.label)
        if pos is None:
            continue
        spans.append(Span(start=pos[0], end=pos[1], label=c.label))
    return spans

def resolve_overlaps(spans: List[Span]) -> List[Span]:
    spans = sorted(spans, key=lambda s: (s.start, -(s.end - s.start), s.end))
    kept: List[Span] = []
    for sp in spans:
        if not kept:
            kept.append(sp)
            continue
        last = kept[-1]
        if sp.start >= last.end:
            kept.append(sp)
            continue
        len_last = last.end - last.start
        len_sp = sp.end - sp.start
        if len_sp > len_last:
            kept[-1] = sp
    return kept

# ============================================================
# IO helpers
# ============================================================
def read_text_file(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except UnicodeDecodeError:
        with open(path, "r", encoding="latin-1") as f:
            return f.read()

def iter_corpus_txt(folder: str) -> List[Tuple[str, str]]:
    files = [n for n in os.listdir(folder) if n.lower().endswith(".txt")]
    files.sort()
    out: List[Tuple[str, str]] = []
    for name in files:
        doc_id = os.path.splitext(name)[0]
        text = read_text_file(os.path.join(folder, name))
        out.append((doc_id, text))
    return out

def load_done_doc_ids(out_path: str) -> set[str]:
    done: set[str] = set()
    if not os.path.exists(out_path):
        return done
    with open(out_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and isinstance(obj.get("doc_id"), str):
                    done.add(obj["doc_id"])
            except Exception:
                continue
    return done

# ============================================================
# Pipeline
# ============================================================
def annotate_one(llm: LLMClient, doc_id: str, text: str, max_chunk_chars: int, verbose: bool) -> Dict[str, Any]:
    chunks = chunk_text(text, max_chars=max_chunk_chars)
    all_spans: List[Span] = []

    for idx, ch in enumerate(chunks, 1):
        # 1) regex candidates (high precision)
        reg_cands = regex_candidates(ch.text)

        # 2) LLM candidates (semantic)
        user = f"doc_id: {doc_id}\ntext:\n{ch.text}"
        extract_obj = llm.chat_json(SYSTEM_NER_EXTRACT, user)
        llm_cands = validate_extract_payload(extract_obj, doc_id_expected=doc_id, chunk_text=ch.text)

        # 3) merge cands
        merged = reg_cands + llm_cands
        # dedupe merged
        seen = set()
        deduped: List[Candidate] = []
        for c in merged:
            key = (c.text, c.label)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(c)

        # 4) deterministic grounding + overlap resolution within chunk
        chunk_spans = resolve_overlaps(spans_from_candidates(ch.text, deduped))

        # lift to doc offsets
        for sp in chunk_spans:
            all_spans.append(Span(start=sp.start + ch.start, end=sp.end + ch.start, label=sp.label))

        if verbose:
            print(f"[{doc_id}] chunk {idx}/{len(chunks)} {ch.start}-{ch.end} "
                  f"regex={len(reg_cands)} llm={len(llm_cands)} -> spans={len(chunk_spans)}")

    all_spans = resolve_overlaps(sorted(all_spans, key=lambda s: (s.start, s.end)))
    return {"doc_id": doc_id, "text": text, "spans": [s.__dict__ for s in all_spans]}

# ============================================================
# Main
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", required=True, help="folder containing paper1.txt, paper2.txt, ...")
    ap.add_argument("--out", required=True, help="output JSONL path, e.g. data/ner_gold_v1.jsonl")
    ap.add_argument("--base-url", required=True, help="Ollama: http://127.0.0.1:11434 ; OpenAI: https://api.openai.com")
    ap.add_argument("--api-key", required=True, help="API key (use 'dummy' for Ollama)")
    ap.add_argument("--model", required=True, help="model name (Ollama example: llama3.1:8b)")
    ap.add_argument("--max-chunk-chars", type=int, default=4000)
    ap.add_argument("--resume", action="store_true", help="skip doc_ids already in output file")
    ap.add_argument("--errors-out", default="", help="optional path to write error records as JSONL")
    ap.add_argument("--verbose", action="store_true", help="print per-chunk stats")
    args = ap.parse_args()

    llm = LLMClient(LLMConfig(
        base_url=args.base_url,
        api_key=args.api_key,
        model=args.model,
        temperature=0.0,
    ))

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    done: set[str] = set()
    if args.resume:
        done = load_done_doc_ids(args.out)
        print(f"[resume] already have {len(done)} docs in {args.out}")

    err_f = None
    if args.errors_out:
        os.makedirs(os.path.dirname(args.errors_out) or ".", exist_ok=True)
        err_f = open(args.errors_out, "a", encoding="utf-8")

    docs = iter_corpus_txt(args.corpus)

    mode = "a" if args.resume else "w"
    with open(args.out, mode, encoding="utf-8") as f:
        for doc_id, text in docs:
            if doc_id in done:
                continue
            try:
                rec = annotate_one(llm, doc_id, text, max_chunk_chars=args.max_chunk_chars, verbose=args.verbose)
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                f.flush()
            except Exception as e:
                msg = str(e)
                if err_f:
                    err_f.write(json.dumps({"doc_id": doc_id, "error": msg[:4000]}, ensure_ascii=False) + "\n")
                    err_f.flush()
                print(f"[ERROR] {doc_id}: {msg[:4000]}")

    if err_f:
        err_f.close()

if __name__ == "__main__":
    main()




# python sr_ner_builder.py \
#   --corpus corpus_txt \
#   --out data/ner_gold_v1.jsonl \
#   --base-url http://127.0.0.1:11434 \
#   --api-key dummy \
#   --model llama3.1:8b \
#   --resume \
#   --errors-out data/ner_errors.jsonl \
#   --audit \
#   --verbose
