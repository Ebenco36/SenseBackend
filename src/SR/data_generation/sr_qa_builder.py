from __future__ import annotations

import argparse
import json
import os
import random
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# ----------------------------
# Question templates
# ----------------------------
QA_TEMPLATES: Dict[str, List[str]] = {
    "REVIEW_TYPE": ["What type of review is this?"],
    "STUDY_DESIGN": ["What study design(s) were included?"],
    "N_STUDIES": ["How many studies were included?"],
    "DATE_OF_LAST_LITERATURE_SEARCH": ["When was the last literature search performed?"],
    "QUALITY_TOOL": ["What tool was used to assess study quality or risk of bias?"],

    "DATABASE": ["Which databases were searched?"],
    "SEARCH_TERMS": ["What search terms were used?"],
    "INCLUSION": ["What were the inclusion criteria?"],
    "ANALYSIS": ["What analysis method was used?"],

    "COUNTRY": ["In which country was the study conducted?"],
    "WHO_REGION": ["Which WHO region was involved?"],
    "INCOME_GROUP": ["What income group was reported?"],

    "VACCINE_TYPE": ["What vaccine type was used?"],
    "VACCINE_BRAND": ["What vaccine brand was used?"],
    "DOSE": ["What dose schedule was used?"],
    "ROUTE": ["What was the route of administration?"],
    "SCREENING": ["What screening method was used?"],

    "EFFECT_MEASURE": ["What effect measure was reported?"],
    "EFFECT_VALUE": ["What was the reported effect size value?"],
    "CI": ["What confidence interval was reported?"],
    "PVALUE": ["What p-value was reported?"],
    "PERCENT": ["What percentage was reported?"],

    "COST": ["What cost value was reported?"],
    "QALY": ["What QALY outcome was reported?"],
    "ICER": ["What ICER was reported?"],
}


# ----------------------------
# Data models
# ----------------------------
@dataclass(frozen=True)
class Span:
    start: int
    end: int
    label: str


@dataclass(frozen=True)
class NerDoc:
    doc_id: str
    text: str
    spans: List[Span]


@dataclass(frozen=True)
class QAExample:
    id: str
    doc_id: str
    question: str
    context: str
    answers: Dict[str, List[Any]]  # {"text":[...], "answer_start":[...]}


# ----------------------------
# IO
# ----------------------------
class JsonlNerReader:
    def __init__(self, path: str):
        self.path = path

    def iter_docs(self) -> Iterable[NerDoc]:
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                doc_id = obj["doc_id"]
                text = obj["text"]
                spans = [Span(int(s["start"]), int(s["end"]), s["label"]) for s in obj.get("spans", [])]
                yield NerDoc(doc_id=doc_id, text=text, spans=spans)


class JsonlWriter:
    def __init__(self, path: str, mode: str = "w"):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.path = path
        self.mode = mode

    def write(self, examples: Iterable[QAExample]) -> None:
        with open(self.path, self.mode, encoding="utf-8") as f:
            for ex in examples:
                f.write(json.dumps({
                    "id": ex.id,
                    "doc_id": ex.doc_id,
                    "question": ex.question,
                    "context": ex.context,
                    "answers": ex.answers,
                }, ensure_ascii=False) + "\n")
                f.flush()


# ----------------------------
# Context selection
# ----------------------------
class ContextSelector:
    """
    Selects a context around an answer span.
    - window_chars controls context size
    - snap_to_sentence tries to expand/shrink to nearby punctuation/newlines
    """
    def __init__(self, window_chars: int = 1200, snap_to_sentence: bool = True):
        self.window_chars = window_chars
        self.snap_to_sentence = snap_to_sentence

    def select(self, text: str, ans_start: int, ans_end: int) -> Tuple[str, int]:
        center = (ans_start + ans_end) // 2
        half = self.window_chars // 2
        ctx_start = max(0, center - half)
        ctx_end = min(len(text), ctx_start + self.window_chars)
        ctx_start = max(0, ctx_end - self.window_chars)

        if self.snap_to_sentence:
            ctx_start, ctx_end = self._snap(text, ctx_start, ctx_end, ans_start, ans_end)

        return text[ctx_start:ctx_end], ctx_start

    @staticmethod
    def _snap(text: str, ctx_start: int, ctx_end: int, ans_start: int, ans_end: int) -> Tuple[int, int]:
        # expand to nearest boundary characters without crossing too far
        # boundaries: newline or sentence punctuation
        boundaries = re.compile(r"[\n\.!\?]")
        # snap start left
        left = text[:ctx_start]
        m = list(boundaries.finditer(left))
        if m:
            new_start = m[-1].end()
            if new_start < ans_start:
                ctx_start = new_start
        # snap end right
        right = text[ctx_end:]
        m2 = boundaries.search(right)
        if m2:
            new_end = ctx_end + m2.end()
            if new_end > ans_end:
                ctx_end = new_end
        return ctx_start, min(len(text), ctx_end)


# ----------------------------
# Template provider
# ----------------------------
class QuestionBank:
    def __init__(self, templates: Dict[str, List[str]]):
        self.templates = templates

    def supports(self, label: str) -> bool:
        return label in self.templates and len(self.templates[label]) > 0

    def sample(self, label: str, rng: random.Random) -> str:
        return rng.choice(self.templates[label])


# ----------------------------
# QA Builder
# ----------------------------
class QADatasetBuilder:
    def __init__(
        self,
        qbank: QuestionBank,
        ctx_selector: ContextSelector,
        max_per_label_per_doc: int = 3,
        seed: int = 13,
        include_unanswerable: bool = False,
        neg_labels: Optional[Sequence[str]] = None,
    ):
        self.qbank = qbank
        self.ctx_selector = ctx_selector
        self.max_per_label_per_doc = max_per_label_per_doc
        self.rng = random.Random(seed)
        self.include_unanswerable = include_unanswerable
        self.neg_labels = list(neg_labels) if neg_labels else list(QA_TEMPLATES.keys())

    def build_from_doc(self, doc: NerDoc) -> List[QAExample]:
        examples: List[QAExample] = []

        # group spans by label
        by_label: Dict[str, List[Span]] = {}
        for s in doc.spans:
            if self.qbank.supports(s.label):
                by_label.setdefault(s.label, []).append(s)

        # positive examples
        for label, spans in by_label.items():
            self.rng.shuffle(spans)
            spans = spans[: self.max_per_label_per_doc]

            for i, sp in enumerate(spans):
                answer_text = doc.text[sp.start:sp.end]
                context, ctx_start = self.ctx_selector.select(doc.text, sp.start, sp.end)
                answer_start = sp.start - ctx_start

                # sanity check for offset correctness within context
                if answer_start < 0 or answer_start + len(answer_text) > len(context):
                    continue
                if context[answer_start:answer_start + len(answer_text)] != answer_text:
                    continue

                q = self.qbank.sample(label, self.rng)
                ex_id = f"{doc.doc_id}:{label}:pos:{i}"

                examples.append(QAExample(
                    id=ex_id,
                    doc_id=doc.doc_id,
                    question=q,
                    context=context,
                    answers={"text": [answer_text], "answer_start": [answer_start]},
                ))

        # optional unanswerable negatives (SQuAD2 style)
        if self.include_unanswerable:
            present = set(by_label.keys())
            missing = [lab for lab in self.neg_labels if lab not in present and self.qbank.supports(lab)]
            self.rng.shuffle(missing)
            missing = missing[: min(3, len(missing))]  # small number per doc

            for j, lab in enumerate(missing):
                q = self.qbank.sample(lab, self.rng)
                # context: take the beginning (or any chunk); for negatives, it just needs to be plausible
                context = doc.text[: min(len(doc.text), self.ctx_selector.window_chars)]
                ex_id = f"{doc.doc_id}:{lab}:neg:{j}"
                examples.append(QAExample(
                    id=ex_id,
                    doc_id=doc.doc_id,
                    question=q,
                    context=context,
                    answers={"text": [], "answer_start": []},
                ))

        return examples


# ----------------------------
# CLI
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="input NER JSONL, e.g. data/ner_gold_v1.jsonl")
    ap.add_argument("--out", dest="out", required=True, help="output QA JSONL, e.g. data/qa_train.jsonl")
    ap.add_argument("--window-chars", type=int, default=1200)
    ap.add_argument("--max-per-label-per-doc", type=int, default=3)
    ap.add_argument("--seed", type=int, default=13)
    ap.add_argument("--no-snap", action="store_true", help="disable sentence-ish snapping")
    ap.add_argument("--unanswerable", action="store_true", help="include SQuAD2-style unanswerable examples")
    args = ap.parse_args()

    reader = JsonlNerReader(args.inp)
    writer = JsonlWriter(args.out, mode="w")

    qbank = QuestionBank(QA_TEMPLATES)
    ctx_selector = ContextSelector(window_chars=args.window_chars, snap_to_sentence=not args.no_snap)
    builder = QADatasetBuilder(
        qbank=qbank,
        ctx_selector=ctx_selector,
        max_per_label_per_doc=args.max_per_label_per_doc,
        seed=args.seed,
        include_unanswerable=args.unanswerable,
    )

    def gen() -> Iterable[QAExample]:
        for doc in reader.iter_docs():
            for ex in builder.build_from_doc(doc):
                yield ex

    writer.write(gen())
    print(f"Done. Wrote QA JSONL to {args.out}")


if __name__ == "__main__":
    main()


# python sr_qa_builder.py \
#   --in data/ner_gold_v1.jsonl \
#   --out data/qa_train.jsonl \
#   --window-chars 1200 \
#   --max-per-label-per-doc 3 \
#   --unanswerable
