import pandas as pd
import re
from transformers import pipeline, AutoTokenizer, AutoModelForTokenClassification
from typing import List, Dict
from src.Services.Taggers.TaggerInterface import TaggerInterface

class NERTester(TaggerInterface):
    """
    A class to load a fine-tuned NER model and run inference on new text.
    This version is robust to handling long documents that exceed the model's max length.
    """
    def __init__(self):
        self.model_path = "./results/baseline/best_model"
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
    
    def normalize_hyphens(self, text):
        return re.sub(r'(\b[A-Za-z]+)-(\d+|\b[A-Za-z]+\b)', r'\1 \2', text)


    def run_ner_inference(self, text: str, confidence_threshold: float = 0.6) -> dict:
        """
        Runs NER inference on a given text using the provided NERTester instance.
        Returns a dictionary where keys are Entity Types and values are lists of (Text Span, Confidence).
        """
        # Normalize hyphenated words for better token alignment
        text = self.normalize_hyphens(text)

        if not self.pipeline:
            print("NER pipeline is not initialized.")
            return {}

        print("\n" + "="*50)
        print("RUNNING INFERENCE")
        print("="*50)

        # Predict entities
        extracted_entities = self.predict(text)

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
    
    
    def format_data_with_confidence(self, raw_data: dict) -> dict:
        """
        Formats raw extracted data into a dictionary where each key
        maps to a list of strings, with each string containing the
        original value and its confidence score.

        Args:
            raw_data: The raw dictionary from the extraction model.

        Returns:
            A dictionary with lists of formatted strings.
        """
        formatted_dict = {}
        for raw_key, items in raw_data.items():
            # Use a list comprehension to format each tuple into the desired string format.
            # The score is rounded to four decimal places for readability.
            formatted_list = [f"{text} (confidence: {score:.4f})" for text, score in items]
            
            # Use a clean, lowercase key for the final dictionary.
            clean_key = f"{raw_key.lower()}_bert"
            formatted_dict[clean_key] = formatted_list
            
        return formatted_dict


    def process(self, text: str) -> dict:
        """
        Processes the input text and returns recognized entities above a confidence threshold.
        This method is a wrapper for run_ner_inference to maintain consistency with the original interface.
        """
        result = self.run_ner_inference(text, 0.6)
        data = self.format_data_with_confidence(result)
        print(data)
        return data