from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from datasets import Dataset

def build_bio_tagset(labels: List[str]) -> Tuple[List[str], Dict[str,int], Dict[int,str]]:
    tags = ["O"]
    for lab in labels:
        tags += [f"B-{lab}", f"I-{lab}"]
    tag2id = {t:i for i,t in enumerate(tags)}
    id2tag = {i:t for t,i in tag2id.items()}
    return tags, tag2id, id2tag

@dataclass(frozen=True)
class NerFeaturizer:
    tokenizer: Any
    tag2id: Dict[str,int]
    max_len: int = 512

    def featurize(self, docs: List[Dict[str, Any]]) -> Dataset:
        rows = []
        for d in docs:
            text = d["text"]
            spans = d.get("spans", [])

            enc = self.tokenizer(
                text,
                truncation=True,
                max_length=self.max_len,
                return_offsets_mapping=True,
            )
            offsets: List[Tuple[int,int]] = enc["offset_mapping"]

            labels = self._charspans_to_bio(offsets, spans)

            # mask special tokens
            masked = []
            for (s,e), lab_id in zip(offsets, labels):
                masked.append(-100 if s == e else lab_id)

            enc.pop("offset_mapping")
            enc["labels"] = masked
            rows.append(enc)

        return Dataset.from_list(rows)

    def _charspans_to_bio(self, offsets: List[Tuple[int,int]], spans: List[Dict[str, Any]]) -> List[int]:
        # resolve overlaps by preferring longer spans
        norm = sorted(
            [(int(s["start"]), int(s["end"]), str(s["label"])) for s in spans],
            key=lambda x: (-(x[1]-x[0]), x[0], x[1])
        )
        chosen = []
        for s0,e0,lab in norm:
            ok = True
            for s1,e1,_ in chosen:
                if not (e0 <= s1 or s0 >= e1):
                    ok = False
                    break
            if ok:
                chosen.append((s0,e0,lab))

        out = [self.tag2id["O"]] * len(offsets)

        for s0,e0,lab in chosen:
            b = f"B-{lab}"
            i = f"I-{lab}"
            started = False
            for idx,(ts,te) in enumerate(offsets):
                if ts == te:
                    continue
                if te <= s0 or ts >= e0:
                    continue
                out[idx] = self.tag2id[b] if not started else self.tag2id[i]
                started = True

        return out
