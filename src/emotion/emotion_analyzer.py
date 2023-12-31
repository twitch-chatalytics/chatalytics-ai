import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from utils.preprocess_text import preprocess_text


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
            batch = [preprocess_text(str(msg)) for msg in messages[i:i + batch_size]]
            inputs = self.tokenizer(batch, padding=True, truncation=True, return_tensors="pt").to(self.device)

            try:
                with torch.no_grad():
                    outputs = self.model(**inputs)
                predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
                results.extend(predictions.cpu().numpy())
            except Exception as e:
                print(f"Error processing batch {i // batch_size}: {e}")

        return results
