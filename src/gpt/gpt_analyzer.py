from transformers import GPT2Tokenizer, GPT2LMHeadModel
import torch


class GPTAnalyzer:

    def __init__(self):
        self.tokenizer = GPT2Tokenizer.from_pretrained('gpt2')
        self.model = GPT2LMHeadModel.from_pretrained('gpt2')

    def generate_summary(self, input_data):
        prompt = "Write a summary about the following emotional scores which an end user will understand and get great value from: "
        prompt += ", ".join([f"{emotion}: {score}" for emotion, score in input_data.items()])

        inputs = self.tokenizer.encode(prompt, return_tensors='pt')
        outputs = self.model.generate(inputs, max_length=150, num_return_sequences=1)

        return self.tokenizer.decode(outputs[0], skip_special_tokens=True)
