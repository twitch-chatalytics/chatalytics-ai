import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class ToxicityAnalyzer:
    def __init__(self, model_name="unitary/toxic-bert"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Initializing ToxicityAnalyzer with device: {self.device}")

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.to(self.device)
        print(f"Model {model_name} loaded successfully.")

    def predict_toxicity(self, messages, batch_size=32):
        print("Starting toxicity prediction...")
        scores = []
        total_batches = len(messages) // batch_size + (0 if len(messages) % batch_size == 0 else 1)

        for i in tqdm(range(0, len(messages), batch_size), desc="Processing batches", total=total_batches):
            batch = messages[i:i + batch_size]
            inputs = self.tokenizer(batch, padding=True, truncation=True, return_tensors="pt").to(self.device)

            with torch.no_grad():
                outputs = self.model(**inputs)

            probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
            batch_scores = probabilities[:, 1].cpu().tolist()
            scores.extend(batch_scores)

        print("Toxicity prediction completed.")

        return scores


class ToxicityDataInserter:
    def __init__(self, repository):
        self.repository = repository
        print("DataInserter initialized with given repository.")

    def insert_toxicity_report(self, data, query):
        print("Starting data insertion into the database...")
        total_rows = len(data)

        with tqdm(total=total_rows, desc="Inserting data") as pbar:
            for index, row in data.iterrows():
                # Ensure toxic_message_count is an integer
                toxic_message_count = int(row['toxic_message_count'])
                self.repository.write(query, (index, row['average_toxicity'], toxic_message_count))
                pbar.update(1)

        print("Data insertion completed.")
