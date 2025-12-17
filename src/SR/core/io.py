import json, random
from typing import Any, Dict, List, Tuple

def read_jsonl(path: str) -> List[Dict[str, Any]]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out

def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def split_docs(docs: List[Dict[str, Any]], seed: int = 42, train=0.8, val=0.1):
    rng = random.Random(seed)
    docs = docs[:]
    rng.shuffle(docs)
    n = len(docs)
    n_train = int(train * n)
    n_val = int(val * n)
    return docs[:n_train], docs[n_train:n_train+n_val], docs[n_train+n_val:]
