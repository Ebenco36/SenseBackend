from transformers import BertTokenizer, BertForQuestionAnswering, pipeline

class LiteratureSearchQA:
    def __init__(self):
        # Load pre-trained BERT model for QA and tokenizer
        self.tokenizer = BertTokenizer.from_pretrained("bert-large-uncased-whole-word-masking-finetuned-squad")
        self.model = BertForQuestionAnswering.from_pretrained("bert-large-uncased-whole-word-masking-finetuned-squad")
        
        # Initialize the QA pipeline
        self.qa_pipeline = pipeline("question-answering", model=self.model, tokenizer=self.tokenizer)

    # def extract_information(self, text: str) -> dict:
    #     # Define the questions to extract key information
    #     questions = {
    #         "last_search_date": "When was the last literature search conducted?",
    #         "total_studies_included": "How many studies were included in the review?",
    #         "total_population": "What is the total population size studied?",
    #         "total_sample_size": "What is the total sample size?",
    #         "sex_proportion": "What is the sex proportion distribution?",
    #         "RCT_count": "How many RCTs were included in the review?",
    #         "NSRI_count": "How many NSRIs were included in the review?"
    #     }
        
    #     # Extract the answers for each question
    #     extracted_info = {}
    #     for key, question in questions.items():
    #         extracted_info[key] = self.extract_answer(text, question)
        
    #     return extracted_info

    def extract_last_search_date(self, text: str) -> str:
        """Extract the last literature search date."""
        question = "When was the last literature search conducted?"
        return self.extract_answer(text, question)

    def extract_total_studies_included(self, text: str) -> str:
        """Extract the total number of studies included."""
        question = "How many studies were included in the review?"
        return self.extract_answer(text, question)

    def extract_total_population(self, text: str) -> str:
        """Extract the total population size studied."""
        question = "What is the total population size studied?"
        return self.extract_answer(text, question)

    def extract_total_sample_size(self, text: str) -> str:
        """Extract the total sample size."""
        question = "What is the total sample size?"
        return self.extract_answer(text, question)

    def extract_sex_proportion(self, text: str) -> str:
        """Extract the sex proportion distribution."""
        question = "What is the sex proportion distribution?"
        return self.extract_answer(text, question)
    
    def extract_population_proportion(self, text: str) -> str:
        """Extract the population proportion distribution."""
        question = "What is the population proportion distribution?"
        return self.extract_answer(text, question)
    
    def extract_country_proportion(self, text: str) -> str:
        """Extract the country proportion distribution."""
        question = "What is the country proportion distribution?"
        return self.extract_answer(text, question)

    def extract_total_RCT_count(self, text: str) -> str:
        """Extract the count of Randomized Controlled Trials (RCTs)."""
        question = """
            How many RCTs (
                randomized controlled trial, randomised controlled trial,
                randomized trial, randomised trial, clinical trial, 
                double-blind study, placebo-controlled, randomised comparative
            ) 
            were included in the review?
        """
        return self.extract_answer(text, question)

    
    def extract_total_mix_method_count(self, text: str) -> str:
        """Extract the count of Mixed Methods."""
        question = """
           How many mixed methods studies, including convergent design and explanatory sequential design, were included in the review?
        """
        return self.extract_answer(text, question)
    
    def extract_total_NSRI_count(self, text: str) -> str:
        """Extract the count of NSRI studies."""
        question = """
        How many NSRIs (
            non-randomized studies of interventions, 
            observational studies, quasi-experimental,
            non-randomized controlled study, natural experiment,
            test-negative designs, cross-sectional study, controlled clinical, 
            cohort study, prospective study, retrospective study, 
            longitudinal study, case-control study, pre-post studies, 
            interrupted time series, case reports, case series
        ) 
        were included in the review?
        """
        return self.extract_answer(text, question)

    def extract_answer(self, text: str, question: str) -> str:
        """Helper function to extract the answer to a specific question from the text."""
        result = self.qa_pipeline(question=question, context=text)
        return result['answer']