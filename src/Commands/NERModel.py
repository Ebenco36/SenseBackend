import torch
import re
import pandas as pd
import numpy as np
import random
from typing import List, Dict
from datasets import Dataset, DatasetDict
from sklearn.model_selection import train_test_split
from transformers import (
    AutoTokenizer,
    AutoModelForTokenClassification,
    TrainingArguments,
    Trainer,
    DataCollatorForTokenClassification
)
from seqeval.metrics import classification_report, f1_score

# =============================================================================
# 1. CONFIGURATION AND DATA SCHEMA
# =============================================================================

class SystemConfig:
    MODEL_NAME = "microsoft/BiomedNLP-PubMedBERT-base-uncased-abstract-fulltext"
    NER_LABELS = [
        "POP", "INT", "CMP", "OUT", "STUDY_DESIGN", "STUDY_COUNT", 
        "PARTICIPANTS", "DATABASE", "SEARCH_DATE", "EFFECT_METRIC", 
        "STAT_VALUE", "LOCATION", "AGE_GROUP", "GENDER"
    ]
    TEST_SIZE = 0.20
    VALIDATION_SIZE = 0.20
    RANDOM_SEED = 42
    TRAINING_ARGS = {
        "output_dir": "./results/baseline",
        "num_train_epochs": 10,
        "per_device_train_batch_size": 8,
        "per_device_eval_batch_size": 8,
        "learning_rate": 3e-5,
        "warmup_steps": 100,
        "weight_decay": 0.01,
        "logging_steps": 20,
        "evaluation_strategy": "epoch",
        "save_strategy": "epoch",
        "load_best_model_at_end": True,
        "metric_for_best_model": "eval_f1",
        "save_total_limit": 1,
    }

    def __init__(self):
        self.full_ner_label_list = self._get_iob_labels(self.NER_LABELS)
        self.label2id = {label: i for i, label in enumerate(self.full_ner_label_list)}
        self.id2label = {i: label for label, i in self.label2id.items()}

    def _get_iob_labels(self, base_labels: List[str]) -> List[str]:
        iob_labels = ["O"]
        for label in base_labels:
            iob_labels.extend([f"B-{label}", f"I-{label}"])
        return iob_labels

# =============================================================================
# 2. SYNTHETIC DATA GENERATOR
# =============================================================================

class SyntheticDataGenerator:
    def __init__(self, config: SystemConfig, vocab_csv_path: str, templates_csv_path: str):
        self.config = config
        self.vocab = self._load_vocab_from_csv(vocab_csv_path)
        self.templates = self._load_templates_from_csv(templates_csv_path)
        print("--- SyntheticDataGenerator initialized with CSVs ---")

    def _load_vocab_from_csv(self, filepath: str) -> Dict[str, List[tuple]]:
        df = pd.read_csv(filepath)
        vocab_dict = {}
        for entity_type, group in df.groupby('entity_type'):
            phrases = []
            for _, row in group.iterrows():
                phrase = row['phrase']
                word_count = len(phrase.split())
                phrases.append((phrase, word_count))
            vocab_dict[entity_type] = phrases
        return vocab_dict

    def _load_templates_from_csv(self, filepath: str) -> List[str]:
        return pd.read_csv(filepath)['template'].tolist()

    def generate(self, num_samples: int) -> List[Dict]:
        dataset = []
        for _ in range(num_samples):
            template = random.choice(self.templates)
            sentence_tokens = []
            ner_tags_str = []

            parts = re.split(r'(\[\w+\])', template)
            for part in parts:
                if part.startswith('[') and part.endswith(']'):
                    entity_tag = part[1:-1]
                    if entity_tag in self.vocab:
                        phrase, _ = random.choice(self.vocab[entity_tag])
                        phrase_tokens = phrase.split()
                        sentence_tokens.extend(phrase_tokens)
                        ner_tags_str.append(f"B-{entity_tag}")
                        ner_tags_str.extend([f"I-{entity_tag}"] * (len(phrase_tokens) - 1))
                else:
                    text_tokens = part.split()
                    sentence_tokens.extend(text_tokens)
                    ner_tags_str.extend(['O'] * len(text_tokens))

            dataset.append({
                "tokens": sentence_tokens,
                "ner_tags_str": ner_tags_str
            })

        df = pd.DataFrame(dataset)
        df['ner_tags'] = df['ner_tags_str'].apply(lambda tags: [self.config.label2id.get(tag, 0) for tag in tags])
        return df.to_dict(orient='records')

# =============================================================================
# 3. MODEL PIPELINE WITH INFERENCE
# =============================================================================

class ModelPipeline:
    def __init__(self, config: SystemConfig):
        self.config = config
        self.tokenizer = AutoTokenizer.from_pretrained(config.MODEL_NAME)
        self.model = None  # Will load later for inference

    def _tokenize_and_align_labels(self, examples):
        tokenized_inputs = self.tokenizer(examples["tokens"], truncation=True, is_split_into_words=True)
        labels = []
        for i, label in enumerate(examples["ner_tags"]):
            word_ids = tokenized_inputs.word_ids(batch_index=i)
            previous_word_idx = None
            label_ids = []
            for word_idx in word_ids:
                if word_idx is None:
                    label_ids.append(-100)
                elif word_idx != previous_word_idx:
                    label_ids.append(label[word_idx])
                else:
                    label_ids.append(-100)
                previous_word_idx = word_idx
            labels.append(label_ids)
        tokenized_inputs["labels"] = labels
        return tokenized_inputs

    def run(self, training_data: List[Dict]):
        train_val_data, test_data = train_test_split(
            training_data, test_size=self.config.TEST_SIZE, random_state=self.config.RANDOM_SEED
        )
        train_data, val_data = train_test_split(
            train_val_data, test_size=self.config.VALIDATION_SIZE, random_state=self.config.RANDOM_SEED
        )

        raw_datasets = DatasetDict({
            'train': Dataset.from_list(train_data),
            'validation': Dataset.from_list(val_data),
            'test': Dataset.from_list(test_data)
        })
        print("\n--- Data Splits ---")
        print(raw_datasets)

        tokenized_datasets = raw_datasets.map(self._tokenize_and_align_labels, batched=True)

        model = AutoModelForTokenClassification.from_pretrained(
            self.config.MODEL_NAME,
            num_labels=len(self.config.full_ner_label_list),
            id2label=self.config.id2label,
            label2id=self.config.label2id
        )

        def compute_metrics(p):
            predictions, labels = p
            predictions = np.argmax(predictions, axis=2)
            true_predictions = [
                [self.config.id2label[p_id] for (p_id, l_id) in zip(prediction, label) if l_id != -100]
                for prediction, label in zip(predictions, labels)
            ]
            true_labels = [
                [self.config.id2label[l_id] for (_, l_id) in zip(prediction, label) if l_id != -100]
                for prediction, label in zip(predictions, labels)
            ]
            return {
                "f1": f1_score(true_labels, true_predictions, average='macro'),
                "report_str": classification_report(true_labels, true_predictions, zero_division=0)
            }

        args = TrainingArguments(**self.config.TRAINING_ARGS)
        trainer = Trainer(
            model=model,
            args=args,
            train_dataset=tokenized_datasets["train"],
            eval_dataset=tokenized_datasets["validation"],
            tokenizer=self.tokenizer,
            data_collator=DataCollatorForTokenClassification(self.tokenizer),
            compute_metrics=compute_metrics,
        )

        print("\n--- Starting Model Training ---")
        trainer.train()

        print("\n--- Evaluating on Test Set ---")
        test_metrics = trainer.evaluate(tokenized_datasets["test"])
        print("\n" + "="*50)
        print("FINAL TEST RESULTS")
        print("="*50)
        print(f"Final Test Set F1-Score: {test_metrics['eval_f1']:.4f}\n")
        print(test_metrics['eval_report_str'])

        final_model_path = f"{self.config.TRAINING_ARGS['output_dir']}/best_model"
        trainer.save_model(final_model_path)
        print(f"\nModel saved to: {final_model_path}")

        # Load model back for inference
        self.model = AutoModelForTokenClassification.from_pretrained(final_model_path)
        self.model.eval()
        self.model.to('cuda' if torch.cuda.is_available() else 'cpu')

    def sliding_window_inference(self, long_text, window_size=256, stride=128):
        tokens = self.tokenizer.tokenize(long_text)
        spans = []

        for start in range(0, len(tokens), stride):
            end = start + window_size
            window_tokens = tokens[start:end]
            if not window_tokens:
                continue

            # Convert tokens back to string and encode properly
            window_text = self.tokenizer.convert_tokens_to_string(window_tokens)
            inputs = self.tokenizer(window_text, return_tensors="pt", truncation=True, max_length=512).to(self.model.device)

            with torch.no_grad():
                outputs = self.model(**inputs).logits

            predictions = outputs.argmax(dim=-1)[0].tolist()
            token_ids = inputs["input_ids"][0].tolist()

            tokens_decoded = self.tokenizer.convert_ids_to_tokens(token_ids)

            for token, pred_id in zip(tokens_decoded, predictions):
                label = self.config.id2label[pred_id]
                if label != "O" and not token.startswith("##"):
                    clean_token = token.replace("▁", "").replace("##", "")
                    spans.append((clean_token, label))

        return spans


# =============================================================================
# 4. MAIN EXECUTION BLOCK
# =============================================================================

if __name__ == '__main__':
    config = SystemConfig()

    data_generator = SyntheticDataGenerator(
        config,
        vocab_csv_path="systematic_review_vocab.csv",
        templates_csv_path="systematic_review_templates.csv"
    )
    training_data = data_generator.generate(num_samples=1000)

    pipeline = ModelPipeline(config)
    pipeline.run(training_data)

    # Example Inference
    long_text = """
    This systematic review identified 45 studies including 10 randomized controlled trials.
    The last literature search was performed on August 3, 2016.
    """
    print("\n--- Inference on Long Text ---")
    spans = pipeline.sliding_window_inference(long_text)
    for span_text, entity in spans:
        print(f"{entity}: {span_text}")