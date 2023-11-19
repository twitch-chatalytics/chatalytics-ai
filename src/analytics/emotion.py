import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from data.database import CassandraDB


class EmotionAnalyzer:
    def __init__(self, model_name="bhadresh-savani/distilbert-base-uncased-emotion"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.to(self.device)

    def predict_emotions(self, messages, batch_size=32):
        results = []
        total_batches = len(messages) // batch_size + (0 if len(messages) % batch_size == 0 else 1)

        for i in tqdm(range(0, len(messages), batch_size), desc="Processing batches", total=total_batches):
            batch = messages[i:i + batch_size]
            inputs = self.tokenizer(batch, padding=True, truncation=True, return_tensors="pt").to(self.device)

            with torch.no_grad():
                outputs = self.model(**inputs)

            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
            results.extend(predictions.cpu().numpy())

        return results


class EmotionDataInserter:
    def __init__(self, repository):
        self.repository = repository

    def insert_emotion_report(self, data, query):
        print("Starting data insertion into the database...")
        total_rows = len(data)

        with tqdm(total=total_rows, desc="Inserting data", colour='green') as pbar:
            for index, row in data.iterrows():
                # Extracting each emotion count or defaulting to 0 if not present
                sadness = row.get('sadness', 0)
                joy = row.get('joy', 0)
                love = row.get('love', 0)
                anger = row.get('anger', 0)
                fear = row.get('fear', 0)
                surprise = row.get('surprise', 0)

                # Writing the data to Cassandra
                self.repository.write(query, (index, sadness, joy, love, anger, fear, surprise))
                pbar.update(1)

        print("Data insertion completed.")
