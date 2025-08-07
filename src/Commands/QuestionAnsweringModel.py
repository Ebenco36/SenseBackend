import re
import json
import torch
from transformers import pipeline
from typing import Dict, Any, List

from src.Services.Taggers.TaggerInterface import TaggerInterface

class QAPaperExtractor(TaggerInterface):
    """
    Extracts specific information from academic texts using a
    DistilBERT question-answering model with multiple, robust questions.
    """

    def __init__(self, model_name: str = "distilbert-base-uncased-distilled-squad"):
        # self.text = text
        device = "mps" if torch.backends.mps.is_available() else "cpu"
        print(f"Using device: {device}")
        
        self.qa_pipeline = pipeline(
            "question-answering", 
            model=model_name,
            device=device
        )
        
        # Use a list of robust question variations for each key
        self.questions = {
            "search_dates_qa": [
                "What was the date of the last literature search?",
                "When was the last literature search conducted?",
                "What was the search period?",
                "Data sources were searched through what date?"
            ],
            "total_participants_qa": [
                "Describe the study population and their sample sizes.",
                "What were the participant groups and their respective counts?",
                "List the demographics of the participants.",
                "What was the breakdown of the participants in the study?"
            ],
            "included_studies_qa": [
                "How many studies were included in the review?",
                "What was the final number of included articles?",
                "How many studies met the inclusion criteria?",
                "What was the total number of eligible studies?"
            ],
            "study_design_qa": [
                "What was the study design?",
                "What type of study was conducted?",
                "What research methods were used?"
            ],
            "countries_of_study_qa": [
                "In which countries were the studies conducted?",
                "What countries were involved in the research?",
                "Which nations participated in the studies?",
                "Which countries were included in the studies?",
                "What countries did the participants come from?",
                "Where were the studies conducted?",
                "List the countries or locations involved in the research.",
                "What was the geographic scope of the review?",
                "How many countries were included?"
            ],
            "rct_count_qa": [
                "How many randomized controlled trials were included?",
                "What is the count of RCTs in the review?",
                "How many RCTs were part of the studies?",
                "What was the number of randomized controlled trials?",
                "How many randomized controlled trials were included?",
                "What was the number of RCTs?",
                "How many studies were randomized trials?",
                "What was the total count of randomized controlled trials?"
            ],
            "nrsi_count_qa": [
                "How many non-randomized studies were included?",
                "What is the count of NRSIs in the review?",
                "How many NRSIs were part of the studies?",
                "What was the number of non-randomized studies?",
                "How many non-randomized studies were included?",
                "What was the number of NRSIs?",
                "How many studies were non-randomized?",
                "What was the total count of observational or non-randomized studies?"
            ],
            "databases_lists_qa": [
                "What databases were searched for the review?",
                "Which databases were included in the literature search?",
                "List the databases used in the study.",
                "What were the data sources for the research?"
            ]
        }

    def _normalize_answer(self, key: str, answer: str) -> Any:
        # (This helper function remains the same as before)
        answer = answer.strip()
        if key in ["included_studies", "total_participants"]:
            numbers = re.findall(r'[\d,]+', answer)
            if numbers:
                try:
                    return int(numbers[0].replace(',', ''))
                except (ValueError, IndexError):
                    return answer
        return answer

    def extract(self) -> Dict[str, Any]:
        """
        Asks all question variations for each key and returns the best answer.
        """
        results = {}
        print("🤖 Asking the model robust questions...")
        
        for key, question_list in self.questions.items():
            best_answer_obj = None
            
            # Prepare all questions for this key to run in a single batch
            pipeline_input = [{"question": q, "context": self.text} for q in question_list]
            answers = self.qa_pipeline(pipeline_input)
            
            # Find the answer with the highest confidence score from all variations
            best_answer_obj = max(answers, key=lambda x: x['score'])
            
            # Only include the best answer if it meets a confidence threshold
            if best_answer_obj and best_answer_obj['score'] > 0.1:
                results[key] = self._normalize_answer(key, best_answer_obj['answer'])
                
        return results
    
    def process(self, text: str) -> Dict[str, Any]:
        """
        Processes the given text and returns a dictionary of results.
        """
        self.text = text
        structured_data = self.extract()
        # print(structured_data)
        return structured_data