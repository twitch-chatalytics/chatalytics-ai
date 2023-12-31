import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class ToxicityAnalyzer:
    def __init__(self, model_name="unitary/toxic-bert"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.to(self.device)

    def predict_toxicity(self, messages, batch_size=32):
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

        return scores
