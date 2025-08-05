import pandas as pd
import re
from transformers import pipeline, AutoTokenizer, AutoModelForTokenClassification
from typing import List, Dict

# =============================================================================
# 1. CONFIGURATION
# =============================================================================
# We only need the model path to load the fine-tuned model.
# Ensure this path matches the 'output_dir' in your training script.
MODEL_PATH = "./results/baseline/best_model"

# =============================================================================
# 2. INFERENCE CLASS (IMPROVED FOR LONG TEXTS)
# =============================================================================

class NERTester:
    """
    A class to load a fine-tuned NER model and run inference on new text.
    This version is robust to handling long documents that exceed the model's max length.
    """
    def __init__(self, model_path: str):
        self.model_path = model_path
        self.pipeline = self._load_pipeline()

    def _load_pipeline(self):
        """Loads the fine-tuned model and tokenizer into a NER pipeline."""
        print(f"--- Loading fine-tuned model from: {self.model_path} ---")
        try:
            tokenizer = AutoTokenizer.from_pretrained(self.model_path)
            model = AutoModelForTokenClassification.from_pretrained(self.model_path)
            
            # FIX: Explicitly set the model_max_length on the tokenizer.
            # The fine-tuning process can sometimes cause this property to be lost,
            # which disables the pipeline's automatic chunking mechanism for long texts.
            # 512 is the standard for BERT-based models.
            tokenizer.model_max_length = 512
            
            # The pipeline handles tokenization, inference, and entity aggregation.
            ner_pipeline = pipeline(
                "ner",
                model=model,
                tokenizer=tokenizer,
                aggregation_strategy="simple", # Groups sub-word tokens into single entities
                # Add a stride to handle long documents with overlapping windows.
                stride=128,
            )
            print("--- Model loaded successfully. ---")
            return ner_pipeline
        except Exception as e:
            print(f"\n[ERROR] Could not load the model from '{self.model_path}'.")
            print("Please make sure you have run the training script successfully first.")
            print(f"Details: {e}")
            return None

    def predict(self, text: str) -> List[Dict]:
        """Runs the NER pipeline on a given text."""
        if not self.pipeline:
            print("[ERROR] Pipeline not available. Cannot run prediction.")
            return []
        
        # Clean up excessive whitespace and newlines for better processing
        cleaned_text = re.sub(r'\s+', ' ', text).strip()
        entities = self.pipeline(cleaned_text)
        return entities

import re
def normalize_hyphens(text):
    return re.sub(r'(\b[A-Za-z]+)-(\d+|\b[A-Za-z]+\b)', r'\1 \2', text)


def run_ner_inference(tester, text: str, confidence_threshold: float = 0.6) -> dict:
    """
    Runs NER inference on a given text using the provided NERTester instance.
    Returns a dictionary where keys are Entity Types and values are lists of (Text Span, Confidence).
    """
    # Normalize hyphenated words for better token alignment
    text = normalize_hyphens(text)

    if not tester.pipeline:
        print("NER pipeline is not initialized.")
        return {}

    print("\n" + "="*50)
    print("RUNNING INFERENCE")
    print("="*50)

    # Predict entities
    extracted_entities = tester.predict(text)

    if not extracted_entities:
        print("  No entities recognized.")
        return {}

    # Convert to DataFrame for easy processing
    df = pd.DataFrame(extracted_entities)
    df = df.rename(columns={'entity_group': 'Entity Type', 'word': 'Text Span', 'score': 'Confidence'})

    # Filter by confidence threshold
    filtered_df = df[df['Confidence'] > confidence_threshold].sort_values(by='Confidence', ascending=False)

    if filtered_df.empty:
        print("  No entities recognized above confidence threshold.")
        return {}

    # Build dictionary: {Entity Type: [(Text Span, Confidence), ...]}
    result_dict = {}
    for entity_type in filtered_df['Entity Type'].unique():
        entity_rows = filtered_df[filtered_df['Entity Type'] == entity_type]
        result_dict[entity_type] = list(zip(entity_rows['Text Span'], entity_rows['Confidence']))

    # Display summary
    for entity, spans in result_dict.items():
        print(f"\n{entity}:")
        for text_span, conf in spans:
            print(f"  - {text_span} (Confidence: {conf:.3f})")

    return result_dict