#!/usr/bin/env python3
"""
train_sr_models.py - WITH MEMORY SAFEGUARDS
Prevents laptop crashes via:
- RAM limits
- CPU thread limits
- OOM error handling
- Lower default batch sizes
"""

import os, json, math, logging, sys, gc
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

import torch
from torch.utils.data import DataLoader
from sentence_transformers import SentenceTransformer, InputExample, losses, evaluation, models

# ===== MEMORY SAFEGUARDS =====
# Limit CPU threads to prevent overheating/overload
os.environ["OMP_NUM_THREADS"] = "2"
os.environ["MKL_NUM_THREADS"] = "2"
os.environ["OPENBLAS_NUM_THREADS"] = "2"

# Optional: Set RAM limit (Linux/macOS only)
try:
    import resource
    def set_memory_limit_gb(gb: int):
        """Limit process RAM usage to prevent system crash"""
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        limit = gb * 1024 ** 3
        resource.setrlimit(resource.RLIMIT_AS, (limit, hard))
        logging.info(f"Memory limit set to {gb} GB")
    
    # Uncomment to enable (e.g., 4GB limit):
    # set_memory_limit_gb(4)
except ImportError:
    logging.warning("resource module not available (Windows). Memory limits not set.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ----------------------- Data loading -----------------------

@dataclass
class SRRow:
    query: str
    positive: str
    negatives: List[str]

class SRJsonlLoader:
    """Loads mined SR Q/A-style retrieval rows from JSONL file."""
    def __init__(self, path: str):
        self.path = path

    def load(self) -> List[SRRow]:
        rows: List[SRRow] = []
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                obj = json.loads(line)
                rows.append(SRRow(
                    query=obj["query"].strip(),
                    positive=obj["positive"].strip(),
                    negatives=[n.strip() for n in obj.get("negatives", []) if n.strip()]
                ))
        if not rows:
            raise ValueError(f"No rows found in {self.path}.")
        logging.info(f"Loaded {len(rows)} rows from {self.path}")
        return rows

# ----------------------- Model builder -----------------------

def build_embedder(name: str, device: str = "cpu", max_seq_length: int = 256) -> SentenceTransformer:
    """
    Build a SentenceTransformer with LOWER max_seq_length to save memory.
    Default 256 tokens (vs 512) - adjust as needed.
    """
    name_l = name.lower().strip()
    try:
        if any(tag in name_l for tag in ["all-minilm", "all-mpnet", "multi-qa", "msmarco", "gte-"]):
            logging.info(f"Loading native SentenceTransformer: {name}")
            model = SentenceTransformer(name, device=device)
            model.max_seq_length = max_seq_length
            return model
    except Exception:
        pass

    try:
        logging.info(f"Building from HF backbone: {name} (max_seq_length={max_seq_length})")
        transformer = models.Transformer(name, max_seq_length=max_seq_length)
        pooling = models.Pooling(
            transformer.get_word_embedding_dimension(),
            pooling_mode_cls_token=False,
            pooling_mode_mean_tokens=True,
            pooling_mode_max_tokens=False,
        )
        norm = models.Normalize()
        return SentenceTransformer(modules=[transformer, pooling, norm], device=device)
    except Exception:
        logging.warning(f"Could not build from HF backbone; falling back to SentenceTransformer({name}).")
        return SentenceTransformer(name, device=device)

# ----------------------- Training/Eval dataset builders -----------------------

class SRTrainDataset:
    def __init__(self, rows: List[SRRow]):
        self.examples = [InputExample(texts=[r.query, r.positive]) for r in rows]

    def dataloader(self, batch_size: int = 8, shuffle: bool = True) -> DataLoader:
        if len(self.examples) == 0:
            raise ValueError("No training examples available.")
        return DataLoader(self.examples, batch_size=batch_size, shuffle=shuffle)

class SREvalIR:
    def __init__(self, rows: List[SRRow], include_negatives_in_corpus: bool = True):
        self.rows = rows
        self.include_negatives_in_corpus = include_negatives_in_corpus
        self.corpus: Dict[str, str] = {}
        self.queries: Dict[str, str] = {}
        self.relevant_docs: Dict[str, Dict[str, int]] = {}
        self._build()

    def _build(self):
        doc_id = 0
        def add_doc(text: str) -> str:
            nonlocal doc_id
            key = f"D{doc_id}"
            self.corpus[key] = text
            doc_id += 1
            return key

        for qi, row in enumerate(self.rows):
            qid = f"Q{qi}"
            self.queries[qid] = row.query
            pos_id = add_doc(row.positive)
            self.relevant_docs[qid] = {pos_id: 1}

            if self.include_negatives_in_corpus:
                for neg in row.negatives:
                    add_doc(neg)

        logging.info(f"Eval corpus size: {len(self.corpus)} docs | queries: {len(self.queries)}")

    def evaluator(self, name: str = "sr_ir_eval", map_at_k: List[int] = [1, 3, 5, 10]):
        return evaluation.InformationRetrievalEvaluator(
            queries=self.queries,
            corpus=self.corpus,
            relevant_docs=self.relevant_docs,
            name=name,
            map_at_k=map_at_k,
            show_progress_bar=True
        )

def save_scores(scores, path="scoreboard.json"):
    with open(path, "w") as fp:
        json.dump(scores, fp, indent=2)
    logging.info(f"Saved final evaluation metrics: {path}")

# ----------------------- Trainer with OOM handling -----------------------

class SRTrainer:
    def __init__(self,
                 base_model_name: str,
                 device: str = "cpu",
                 batch_size: int = 8,  # Lower default
                 epochs: int = 3,
                 lr: float = 2e-5,
                 max_seq_length: int = 256,  # Lower default
                 out_dir: Optional[str] = None):
        self.base_model_name = base_model_name
        self.device = device
        self.batch_size = batch_size
        self.epochs = epochs
        self.lr = lr
        self.max_seq_length = max_seq_length
        self.out_dir = out_dir or f"models/{base_model_name.split('/')[-1]}_sr_finetuned"
        self.model: Optional[SentenceTransformer] = None

    def train_and_eval(self,
                       train_rows: List[SRRow],
                       eval_rows: List[SRRow],
                       eval_name: str) -> Dict[str, float]:
        try:
            # Build model
            self.model = build_embedder(self.base_model_name, device=self.device, 
                                       max_seq_length=self.max_seq_length)

            # Datasets
            train_ds = SRTrainDataset(train_rows)
            train_loader = train_ds.dataloader(batch_size=self.batch_size, shuffle=True)

            # Loss
            loss = losses.MultipleNegativesRankingLoss(self.model)

            # Evaluator
            eval_ir = SREvalIR(eval_rows, include_negatives_in_corpus=True)
            evaluator = eval_ir.evaluator(name=eval_name, map_at_k=[1, 3, 5, 10])

            # Warmup
            warmup_steps = math.ceil(len(train_loader) * self.epochs * 0.1)

            # Fit with OOM handling
            logging.info(f"Starting training: batch_size={self.batch_size}, max_seq={self.max_seq_length}")
            self.model.fit(
                train_objectives=[(train_loader, loss)],
                epochs=self.epochs,
                warmup_steps=warmup_steps,
                evaluator=evaluator,
                evaluation_steps=max(10, len(train_loader)),
                output_path=self.out_dir,
                optimizer_params={'lr': self.lr},
                show_progress_bar=True
            )

            # Force memory cleanup
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

            # Final eval
            best_model = SentenceTransformer(self.out_dir, device=self.device)
            scores = evaluator(best_model)
            logging.info(f"[{self.base_model_name}] Final IR scores: {scores}")
            
            return scores

        except RuntimeError as e:
            if "out of memory" in str(e).lower():
                logging.error(f"❌ OUT OF MEMORY ERROR!")
                logging.error(f"   Current batch_size: {self.batch_size}")
                logging.error(f"   Current max_seq_length: {self.max_seq_length}")
                logging.error(f"   Suggestions:")
                logging.error(f"     1. Reduce batch_size (try {self.batch_size // 2})")
                logging.error(f"     2. Reduce max_seq_length (try {self.max_seq_length // 2})")
                logging.error(f"     3. Use device='cpu' instead of GPU")
                logging.error(f"     4. Train on a subset of data first")
                sys.exit(1)
            raise

# ----------------------- Simple split util -----------------------

def simple_split(rows: List[SRRow], ratio: float = 0.85) -> Tuple[List[SRRow], List[SRRow]]:
    n = len(rows)
    cut = max(1, int(n * ratio))
    return rows[:cut], rows[cut:] if cut < n else rows[-1:]

# ----------------------- Runner -----------------------

def run_training(
    jsonl_path: str = "sr_auto_train.jsonl",
    device: str = "cpu",
    epochs: int = 2,  # Lower default
    batch_size: int = 8,  # Lower default
    max_seq_length: int = 256,  # Lower default
    lr: float = 2e-5,
    debug_mode: bool = False  # Use subset for testing
):
    logging.info(f"🚀 Starting SR model training (memory-safe mode)")
    logging.info(f"   Device: {device}")
    logging.info(f"   Batch size: {batch_size}")
    logging.info(f"   Max seq length: {max_seq_length}")
    logging.info(f"   Epochs: {epochs}")
    
    # Load data
    loader = SRJsonlLoader(jsonl_path)
    rows = loader.load()
    
    # Debug mode: use small subset
    if debug_mode:
        rows = rows[:min(100, len(rows))]
        logging.info(f"🔧 DEBUG MODE: Using only {len(rows)} rows")

    train_rows, eval_rows = simple_split(rows, ratio=0.85)
    logging.info(f"Train: {len(train_rows)} | Eval: {len(eval_rows)}")

    # Train MiniLM (lightweight)
    minilm_trainer = SRTrainer(
        base_model_name="sentence-transformers/all-MiniLM-L6-v2",
        device=device, epochs=epochs, batch_size=batch_size, 
        max_seq_length=max_seq_length, lr=lr,
        out_dir="models/minilm_sr_finetuned"
    )
    minilm_scores = minilm_trainer.train_and_eval(
        train_rows, eval_rows, eval_name="sr_ir_eval_minilm"
    )

    # Train Specter2 (heavier - may need smaller batch)
    specter_batch = max(4, batch_size // 2)  # Use smaller batch for larger model
    logging.info(f"Using reduced batch_size={specter_batch} for Specter2")
    
    specter_trainer = SRTrainer(
        base_model_name="allenai/specter2_base",
        device=device, epochs=epochs, batch_size=specter_batch,
        max_seq_length=max_seq_length, lr=lr,
        out_dir="models/specter2_sr_finetuned"
    )
    specter_scores = specter_trainer.train_and_eval(
        train_rows, eval_rows, eval_name="sr_ir_eval_specter2"
    )

    # Scoreboard
    print("\n=== 📊 Scoreboard (MAP@k) ===")
    keys = sorted([k for k in minilm_scores.keys() if k.startswith("map@")], 
                  key=lambda x: int(x.split("@")[1]))
    for k in keys:
        mv = minilm_scores.get(k, float("nan"))
        sv = specter_scores.get(k, float("nan"))
        print(f"{k:>7} | MiniLM: {mv:.4f} | Specter2: {sv:.4f}")
    
    save_scores({"minilm": minilm_scores, "specter2": specter_scores})

# if __name__ == "__main__":
#     # Safe defaults for laptop training
#     run_training(
#         jsonl_path="sr_auto_train.jsonl",
#         device="cpu",  # Change to "cuda" or "mps" if you have GPU
#         epochs=2,
#         batch_size=4,  # Very safe default
#         max_seq_length=128,  # Very safe default
#         lr=2e-5,
#         debug_mode=True  # Set False for full training
#     )
