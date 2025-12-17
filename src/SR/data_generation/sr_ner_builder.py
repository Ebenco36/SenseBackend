from __future__ import annotations

import argparse
import json
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests


# ============================================================
# Labels + Definitions
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
LABEL_SET: Set[str] = set(ATOMIC_LABELS)
LABELS_STR = ", ".join(ATOMIC_LABELS)

LABEL_DEFS: Dict[str, str] = {
    "REVIEW_TYPE": "Type of review. e.g., 'systematic review', 'scoping review', 'meta-analysis', 'rapid review'.",
    "STUDY_DESIGN": "Design of included primary studies. e.g., 'randomized controlled trial', 'cohort study', 'case-control'.",
    "N_STUDIES": "Count of included studies. e.g., '23 studies', '15 trials'.",
    "DATE_OF_LAST_LITERATURE_SEARCH": "Date the search was last run/updated. e.g., 'searched up to March 2023'.",
    "QUALITY_TOOL": "Quality/risk-of-bias tool. e.g., 'GRADE', 'AMSTAR 2', 'ROBINS-I', 'RoB 2'.",
    "DATABASE": "Database name. e.g., 'PubMed', 'MEDLINE', 'Embase', 'Scopus', 'Web of Science'.",
    "SEARCH_TERMS": "Explicit search strings/keywords. e.g., '\"HPV\" AND vaccine'.",
    "INCLUSION": "Eligibility/inclusion/exclusion criteria text.",
    "ANALYSIS": "Analysis methods. e.g., 'random-effects model', 'meta-regression', 'subgroup analysis'.",
    "PERIOD": "Time period range. e.g., '2000–2020', 'between 2012 and 2018'.",
    "FOLLOWUP": "Follow-up duration. e.g., '12 months follow-up', 'followed for 5 years'.",
    "TIMING": "Timing of vaccination/measurement. e.g., '6 weeks post-vaccination'.",

    "PATHOGEN": "Infectious agent name (general). Prefer HPV_TYPE if genotype is present.",
    "HPV_TYPE": "Specific HPV genotype. e.g., 'HPV-16', 'HPV 18', 'HPV-6/11'.",
    "CANCER": "Cancer type. e.g., 'cervical cancer'.",
    "CONDITION": "Non-cancer condition. e.g., 'genital warts'.",
    "LESION": "Pre-cancer lesion terms. e.g., 'CIN2+', 'HSIL'.",
    "AGE_GROUP": "Age group/range. e.g., 'girls aged 9–14'.",
    "GENDER": "Sex/gender group. e.g., 'women', 'men'.",
    "SPECIAL_POP": "Named special populations. e.g., 'pregnant women', 'MSM'.",
    "RISK_GROUP": "Risk descriptors. e.g., 'high-risk', 'immunocompromised'.",

    "VACCINE_TYPE": "Vaccine type/valency. e.g., 'bivalent', 'quadrivalent', '9-valent'.",
    "VACCINE_BRAND": "Brand/product name. e.g., 'Gardasil', 'Cervarix'.",
    "DOSE": "Dose expression/schedule. e.g., 'two-dose schedule', '3 doses'.",
    "ROUTE": "Route of administration. e.g., 'intramuscular', 'IM'.",
    "PROGRAM": "Program delivery. e.g., 'school-based program'.",
    "SCREENING": "Screening intervention. e.g., 'Pap smear', 'HPV DNA testing'.",

    "SAFETY": "Safety/adverse event outcome wording.",
    "ACCEPTANCE": "Willingness/acceptability wording.",
    "COVERAGE": "Coverage/uptake wording.",
    "EFFICACY": "Efficacy/effectiveness wording (not % alone).",
    "IMMUNOGENICITY": "Immune response outcomes.",
    "MODELLING": "Model-based analysis language.",
    "ECONOMIC": "Economic evaluation framing.",
    "BARRIER": "Barrier statements.",
    "FACILITATOR": "Facilitator statements.",

    "SAMPLE_SIZE": "Sample size (participants/subjects).",
    "PERCENT": "Percentages including '%'.",
    "COST": "Currency amounts.",
    "QALY": "QALY mentions.",
    "ICER": "ICER mentions including units.",
    "EFFECT_MEASURE": "Effect measure name (OR/RR/HR, odds ratio, risk ratio...).",
    "EFFECT_VALUE": "Numeric value tied to an effect measure (OR 0.72).",
    "CI": "Confidence interval expression.",
    "PVALUE": "P-value expression.",
}

LABEL_REMAP: Dict[str, str] = {
    "SEARCH_DATE": "DATE_OF_LAST_LITERATURE_SEARCH",
}

def render_label_guide() -> str:
    lines = []
    for lab in ATOMIC_LABELS:
        d = LABEL_DEFS.get(lab)
        if d:
            lines.append(f"- {lab}: {d}")
    lines.append("")
    lines.append("Priority rules:")
    lines.append("- If span is an HPV genotype (e.g., HPV-16), label HPV_TYPE (not PATHOGEN).")
    lines.append("- For statistics: EFFECT_MEASURE is the measure name; EFFECT_VALUE is the number; CI is CI text; PVALUE is p-values; PERCENT is %.")
    lines.append("- Do not label bare numbers unless explicitly permitted by the label definition.")
    return "\n".join(lines)


# ============================================================
# Data Models
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

    def length(self) -> int:
        return self.end - self.start

@dataclass(frozen=True)
class Document:
    doc_id: str
    text: str


# ============================================================
# Utilities
# ============================================================
class JsonUtil:
    """Robust extraction of a top-level JSON object from model output."""
    @staticmethod
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
        raise ValueError(f"Unbalanced braces in model output: {s[start:start+400]}")


# ============================================================
# Readers / Writers
# ============================================================
class FolderTxtReader:
    """Reads corpus_txt/*.txt into Documents."""
    def __init__(self, folder: str):
        self.folder = folder

    def iter_docs(self) -> Iterable[Document]:
        files = [n for n in os.listdir(self.folder) if n.lower().endswith(".txt")]
        files.sort()
        for name in files:
            doc_id = os.path.splitext(name)[0]
            path = os.path.join(self.folder, name)
            yield Document(doc_id=doc_id, text=self._read_text(path))

    @staticmethod
    def _read_text(path: str) -> str:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except UnicodeDecodeError:
            with open(path, "r", encoding="latin-1") as f:
                return f.read()


class JsonlWriter:
    def __init__(self, path: str, mode: str = "w"):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.path = path
        self.mode = mode

    def write_records(self, records: Iterable[dict]) -> None:
        with open(self.path, self.mode, encoding="utf-8") as f:
            for obj in records:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                f.flush()


class ResumeIndex:
    """Tracks doc_ids already present in an output JSONL file."""
    def __init__(self, out_path: str):
        self.out_path = out_path

    def load_done(self) -> Set[str]:
        done: Set[str] = set()
        if not os.path.exists(self.out_path):
            return done
        with open(self.out_path, "r", encoding="utf-8") as f:
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
# Chunker
# ============================================================
@dataclass(frozen=True)
class Chunk:
    start: int
    end: int
    text: str

class Chunker:
    """Simple chunker with overlap; avoids huge contexts for local models."""
    def __init__(self, max_chars: int = 4000, overlap: int = 400):
        self.max_chars = max_chars
        self.overlap = overlap

    def chunk(self, text: str) -> List[Chunk]:
        if len(text) <= self.max_chars:
            return [Chunk(0, len(text), text)]
        chunks: List[Chunk] = []
        i = 0
        while i < len(text):
            j = min(len(text), i + self.max_chars)
            k = text.rfind("\n", i, j)
            if k != -1 and (j - k) < 1500:
                j = k
            chunks.append(Chunk(i, j, text[i:j]))
            if j == len(text):
                break
            i = max(0, j - self.overlap)
        return chunks


# ============================================================
# Proposers
# ============================================================
class RegexProposer:
    """High-precision regex candidates (cheap recall booster)."""
    RULES: List[Tuple[str, str]] = [
        ("HPV_TYPE", r"\bHPV[\s-]?(?:\d{1,2})(?:/\d{1,2})?\b"),
        ("PVALUE", r"\b[pP]\s*(?:[=<]\s*0?\.\d+|=\s*0?\.\d+)\b"),
        ("PERCENT", r"\b\d{1,3}(?:\.\d+)?\s*%\b"),
        ("QALY", r"\bQALY(?:s)?\b|quality-adjusted life-?year(?:s)?"),
        ("EFFECT_MEASURE", r"\b(odds ratio|risk ratio|hazard ratio|rate ratio)\b|\b(OR|RR|HR)\b"),
        ("EFFECT_VALUE", r"\b(?:OR|RR|HR)\s*[=:]?\s*\d+(?:\.\d+)?\b"),
        ("COST", r"(?:USD|EUR|GBP|CAD|AUD)\s*\$?\s*\d[\d,]*(?:\.\d+)?|\$\s*\d[\d,]*(?:\.\d+)?|€\s*\d[\d,]*(?:\.\d+)?"),
        ("ICER", r"\bICER\b.*?(?:per\s+QALY|/QALY)\b"),
        ("CI", r"(?:\b95%\s*CI\b|\bCI\b|confidence interval)\s*[:\(]?\s*\d+(?:\.\d+)?\s*(?:to|[-–—])\s*\d+(?:\.\d+)?\s*\)?"),
    ]

    def propose(self, text: str) -> List[Candidate]:
        out: List[Candidate] = []
        seen = set()
        for label, pat in self.RULES:
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


@dataclass(frozen=True)
class LLMConfig:
    base_url: str
    api_key: str
    model: str
    timeout_s: int = 120
    max_retries: int = 6
    temperature: float = 0.0


class LLMClient:
    """
    Supports:
      - OpenAI-style /v1/chat/completions
      - Responses API /v1/responses (works with Ollama 0.13.4)
    """
    def __init__(self, cfg: LLMConfig):
        self.cfg = cfg

    def json_call(self, system: str, user: str) -> Dict[str, Any]:
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
                return json.loads(JsonUtil.extract_json_object(content))
            except Exception:
                if attempt == self.cfg.max_retries:
                    raise
                time.sleep(min(20.0, (2 ** (attempt - 1)) + random.random()))
        raise RuntimeError("unreachable")

    @staticmethod
    def _extract_text(kind: str, data: Dict[str, Any]) -> str:
        if kind == "openai_chat":
            return data["choices"][0]["message"]["content"]

        texts: List[str] = []
        for item in (data.get("output") or []):
            for part in (item.get("content") or []):
                if isinstance(part, dict):
                    t = part.get("text")
                    if isinstance(t, str) and t.strip():
                        texts.append(t)
        if texts:
            return "\n".join(texts)
        if isinstance(data.get("output_text"), str):
            return data["output_text"]
        if isinstance(data.get("message"), dict) and isinstance(data["message"].get("content"), str):
            return data["message"]["content"]
        return json.dumps(data)


class LLMProposer:
    """LLM-based semantic proposer (candidates only)."""
    SYSTEM = f"""You are a domain expert annotating biomedical systematic review papers.

Return exactly ONE JSON object:
{{
  "doc_id": "string",
  "candidates": [{{"text": "string", "label": "string"}}]
}}

Rules:
- Use ONLY these labels: {LABELS_STR}
- "text" must be an EXACT substring from the provided text.
- Do NOT invent entities. If uncertain, omit.
- Prefer longer spans.
- Avoid duplicates.
- Output valid JSON only.

Label guide:
{render_label_guide()}
"""

    def __init__(self, llm: LLMClient):
        self.llm = llm

    def propose(self, doc_id: str, chunk_text: str) -> List[Candidate]:
        user = f"doc_id: {doc_id}\ntext:\n{chunk_text}"
        obj = self.llm.json_call(self.SYSTEM, user)
        return CandidateValidator.validate_candidates(obj, doc_id_expected=doc_id, chunk_text=chunk_text)


class CandidateValidator:
    """Validates/filters LLM candidate outputs."""
    @staticmethod
    def validate_candidates(obj: Dict[str, Any], doc_id_expected: str, chunk_text: str) -> List[Candidate]:
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

            label = LABEL_REMAP.get(label, label)
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
# Grounding + Auditing
# ============================================================
class DeterministicGrounder:
    """Converts (text,label) candidates into exact (start,end,label) spans."""
    LABEL_HINTS = {
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

    def ground(self, chunk_text: str, candidates: Sequence[Candidate]) -> List[Span]:
        spans: List[Span] = []
        for c in candidates:
            pos = self._choose_occurrence(chunk_text, c.text, c.label)
            if pos is None:
                continue
            spans.append(Span(start=pos[0], end=pos[1], label=c.label))
        return spans

    def _choose_occurrence(self, text: str, sub: str, label: str) -> Optional[Tuple[int, int]]:
        occ = self._find_all(text, sub)
        if not occ:
            return None
        if len(occ) == 1:
            return occ[0]

        patterns = self.LABEL_HINTS.get(label, [])
        if not patterns:
            return occ[0]

        best = occ[0]
        best_score = -1
        for (s, e) in occ:
            left = max(0, s - 90)
            right = min(len(text), e + 90)
            ctx = text[left:right]
            score = sum(1 for pat in patterns if re.search(pat, ctx, flags=re.IGNORECASE))
            if score > best_score:
                best_score = score
                best = (s, e)
        return best

    @staticmethod
    def _find_all(text: str, sub: str) -> List[Tuple[int, int]]:
        out: List[Tuple[int, int]] = []
        i = 0
        while True:
            j = text.find(sub, i)
            if j == -1:
                break
            out.append((j, j + len(sub)))
            i = j + 1
        return out


class SpanAuditor:
    """
    Optional second-pass LLM audit:
      - KEEP / DROP / RELABEL
    This improves precision for “semantic” labels.
    """
    SYSTEM = f"""You are auditing NER spans in a biomedical systematic review.

Input includes:
- doc_id
- chunk_text
- proposed spans with (start,end,label,text_excerpt)

Return exactly ONE JSON object:
{{
  "doc_id": "string",
  "decisions": [
    {{
      "start": int,
      "end": int,
      "label": "string",
      "action": "KEEP" | "DROP" | "RELABEL",
      "new_label": "string"   // only when action is RELABEL
    }}
  ]
}}

Rules:
- Use ONLY these labels: {LABELS_STR}
- Only decide on spans provided. Do not add new spans.
- Prefer DROP if ambiguous.
- If RELABEL, choose the single best label.
- Output valid JSON only.

Label guide:
{render_label_guide()}
"""

    def __init__(self, llm: LLMClient, enabled: bool = True):
        self.llm = llm
        self.enabled = enabled

    def audit(self, doc_id: str, chunk_text: str, spans: Sequence[Span]) -> List[Span]:
        if not self.enabled or not spans:
            return list(spans)

        payload = []
        for s in spans:
            payload.append({
                "start": s.start,
                "end": s.end,
                "label": s.label,
                "text": chunk_text[s.start:s.end],
            })

        user = (
            f"doc_id: {doc_id}\n"
            f"chunk_text:\n{chunk_text}\n\n"
            f"spans:\n{json.dumps(payload, ensure_ascii=False)}"
        )

        obj = self.llm.json_call(self.SYSTEM, user)
        return self._apply_decisions(chunk_text, spans, obj)

    def _apply_decisions(self, chunk_text: str, spans: Sequence[Span], obj: Dict[str, Any]) -> List[Span]:
        if not isinstance(obj, dict) or "decisions" not in obj:
            # Fail-open: keep original spans if audit output malformed
            return list(spans)

        # index spans by (start,end,label)
        span_map: Dict[Tuple[int, int, str], Span] = {(s.start, s.end, s.label): s for s in spans}
        out: List[Span] = []

        decisions = obj.get("decisions")
        if not isinstance(decisions, list):
            return list(spans)

        for d in decisions:
            if not isinstance(d, dict):
                continue
            start = d.get("start")
            end = d.get("end")
            label = d.get("label")
            action = d.get("action")

            if not (isinstance(start, int) and isinstance(end, int) and isinstance(label, str) and isinstance(action, str)):
                continue

            label = LABEL_REMAP.get(label, label)
            key = (start, end, label)
            original = span_map.get(key)
            if original is None:
                # decision refers to unknown span; ignore
                continue

            if action == "KEEP":
                out.append(original)
            elif action == "DROP":
                continue
            elif action == "RELABEL":
                new_label = d.get("new_label")
                if isinstance(new_label, str):
                    new_label = LABEL_REMAP.get(new_label, new_label)
                    if new_label in LABEL_SET:
                        out.append(Span(start=start, end=end, label=new_label))
            else:
                # unknown action -> keep original (fail-open)
                out.append(original)

        # If auditor “forgot” spans, keep them (fail-open) — optional:
        # out_keys = {(s.start, s.end, s.label) for s in out}
        # for s in spans:
        #     if (s.start, s.end, s.label) not in out_keys:
        #         out.append(s)

        return out


# ============================================================
# Span normalization + overlap + caps
# ============================================================
class SpanResolver:
    def __init__(self, overlap_policy: str = "keep_longest", cap_per_label_surface: Optional[int] = None):
        if overlap_policy not in {"keep_longest", "keep_first"}:
            raise ValueError("overlap_policy must be keep_longest or keep_first")
        self.overlap_policy = overlap_policy
        self.cap = cap_per_label_surface

    def dedupe(self, spans: Sequence[Span]) -> List[Span]:
        seen = set()
        out = []
        for s in spans:
            key = (s.start, s.end, s.label)
            if key in seen:
                continue
            seen.add(key)
            out.append(s)
        return out

    def resolve_overlaps(self, spans: Sequence[Span]) -> List[Span]:
        if not spans:
            return []

        if self.overlap_policy == "keep_longest":
            spans_sorted = sorted(spans, key=lambda s: (s.start, -s.length(), s.end, s.label))
        else:
            spans_sorted = sorted(spans, key=lambda s: (s.start, s.end, s.label))

        kept: List[Span] = []
        for s in spans_sorted:
            if not kept:
                kept.append(s)
                continue
            last = kept[-1]
            if s.start >= last.end:
                kept.append(s)
                continue

            # overlap
            if self.overlap_policy == "keep_first":
                continue

            if s.length() > last.length():
                kept[-1] = s

        return kept

    def cap_repeats(self, text: str, spans: Sequence[Span]) -> List[Span]:
        if self.cap is None:
            return list(spans)
        counts: Dict[Tuple[str, str], int] = {}
        out: List[Span] = []
        for s in spans:
            surface = text[s.start:s.end]
            key = (s.label, surface)
            c = counts.get(key, 0)
            if c >= self.cap:
                continue
            counts[key] = c + 1
            out.append(s)
        return out


# ============================================================
# Pipeline Orchestrator
# ============================================================
class SRNerDatasetBuilder:
    """
    End-to-end builder:
      - chunk
      - propose (regex + LLM)
      - ground deterministically
      - (optional) audit with LLM
      - resolve overlaps/dedupe/cap
      - lift offsets to document
    """
    def __init__(
        self,
        chunker: Chunker,
        proposers: Sequence[Any],  # objects with propose(...)
        grounder: DeterministicGrounder,
        auditor: SpanAuditor,
        resolver: SpanResolver,
        verbose: bool = False,
    ):
        self.chunker = chunker
        self.proposers = proposers
        self.grounder = grounder
        self.auditor = auditor
        self.resolver = resolver
        self.verbose = verbose

    def build_record(self, doc: Document) -> Dict[str, Any]:
        chunks = self.chunker.chunk(doc.text)
        all_spans: List[Span] = []

        for idx, ch in enumerate(chunks, 1):
            # 1) propose candidates
            candidates: List[Candidate] = []
            for p in self.proposers:
                if isinstance(p, RegexProposer):
                    candidates.extend(p.propose(ch.text))
                else:
                    # LLMProposer
                    candidates.extend(p.propose(doc.doc_id, ch.text))

            # dedupe candidates
            cand_seen = set()
            deduped_cands: List[Candidate] = []
            for c in candidates:
                if c.label not in LABEL_SET:
                    continue
                key = (c.text, c.label)
                if key in cand_seen:
                    continue
                cand_seen.add(key)
                deduped_cands.append(c)

            # 2) deterministic grounding
            chunk_spans = self.grounder.ground(ch.text, deduped_cands)
            chunk_spans = self.resolver.dedupe(chunk_spans)
            chunk_spans = self.resolver.resolve_overlaps(chunk_spans)

            # 3) optional audit (LLM)
            chunk_spans = self.auditor.audit(doc.doc_id, ch.text, chunk_spans)
            chunk_spans = self.resolver.dedupe(chunk_spans)
            chunk_spans = self.resolver.resolve_overlaps(chunk_spans)

            # 4) lift to doc offsets
            for s in chunk_spans:
                all_spans.append(Span(start=s.start + ch.start, end=s.end + ch.start, label=s.label))

            if self.verbose:
                print(f"[{doc.doc_id}] chunk {idx}/{len(chunks)} {ch.start}-{ch.end} "
                      f"cands={len(deduped_cands)} spans={len(chunk_spans)}")

        # 5) global resolve and cap
        all_spans = self.resolver.dedupe(sorted(all_spans, key=lambda s: (s.start, s.end, s.label)))
        all_spans = self.resolver.resolve_overlaps(all_spans)
        all_spans = self.resolver.cap_repeats(doc.text, all_spans)

        return {
            "doc_id": doc.doc_id,
            "text": doc.text,
            "spans": [{"start": s.start, "end": s.end, "label": s.label} for s in all_spans],
        }


# ============================================================
# CLI
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", required=True, help="folder containing paper1.txt, paper2.txt, ...")
    ap.add_argument("--out", required=True, help="output JSONL path, e.g. data/ner_gold_v1.jsonl")
    ap.add_argument("--errors-out", default="", help="optional JSONL for errors")
    ap.add_argument("--resume", action="store_true", help="append + skip doc_ids already in output")
    ap.add_argument("--verbose", action="store_true", help="log per-chunk stats")

    ap.add_argument("--base-url", required=True, help="Ollama: http://127.0.0.1:11434 ; OpenAI: https://api.openai.com")
    ap.add_argument("--api-key", required=True, help="API key (use 'dummy' for Ollama)")
    ap.add_argument("--model", required=True, help="model name (Ollama example: llama3.1:8b)")
    ap.add_argument("--max-chunk-chars", type=int, default=4000)
    ap.add_argument("--overlap-policy", choices=["keep_longest", "keep_first"], default="keep_longest")
    ap.add_argument("--cap-per-label-surface", type=int, default=50, help="0 disables capping")
    ap.add_argument("--audit", action="store_true", help="enable LLM span auditing pass (slower, higher precision)")
    args = ap.parse_args()

    reader = FolderTxtReader(args.corpus)

    llm = LLMClient(LLMConfig(
        base_url=args.base_url,
        api_key=args.api_key,
        model=args.model,
        temperature=0.0,
    ))

    chunker = Chunker(max_chars=args.max_chunk_chars, overlap=400)
    proposers = [RegexProposer(), LLMProposer(llm)]
    grounder = DeterministicGrounder()
    auditor = SpanAuditor(llm, enabled=args.audit)
    cap = None if args.cap_per_label_surface <= 0 else args.cap_per_label_surface
    resolver = SpanResolver(overlap_policy=args.overlap_policy, cap_per_label_surface=cap)

    builder = SRNerDatasetBuilder(
        chunker=chunker,
        proposers=proposers,
        grounder=grounder,
        auditor=auditor,
        resolver=resolver,
        verbose=args.verbose,
    )

    done: Set[str] = set()
    mode = "w"
    if args.resume:
        mode = "a"
        done = ResumeIndex(args.out).load_done()
        print(f"[resume] loaded {len(done)} completed docs from {args.out}")

    err_f = None
    if args.errors_out:
        os.makedirs(os.path.dirname(args.errors_out) or ".", exist_ok=True)
        err_f = open(args.errors_out, "a", encoding="utf-8")

    writer = JsonlWriter(args.out, mode=mode)

    def gen_records() -> Iterable[dict]:
        for doc in reader.iter_docs():
            if doc.doc_id in done:
                continue
            try:
                yield builder.build_record(doc)
            except Exception as e:
                msg = str(e)[:4000]
                print(f"[ERROR] {doc.doc_id}: {msg}")
                if err_f:
                    err_f.write(json.dumps({"doc_id": doc.doc_id, "error": msg}, ensure_ascii=False) + "\n")
                    err_f.flush()

    writer.write_records(gen_records())

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
