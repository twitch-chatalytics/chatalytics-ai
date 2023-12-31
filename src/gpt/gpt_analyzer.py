from transformers import AutoTokenizer, AutoModelForCausalLM
from transformers import pipeline
from transformers import T5Tokenizer, T5ForConditionalGeneration
from transformers import BartTokenizer, BartForConditionalGeneration
from transformers import GPT2Tokenizer, GPT2LMHeadModel
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch


class GPTAnalyzer:

    def generate_summary(self, df):
        model_id = "mistralai/Mixtral-8x7B-v0.1"
        tokenizer = AutoTokenizer.from_pretrained(model_id)

        model = AutoModelForCausalLM.from_pretrained(model_id, torch_dtype=torch.float16).to(0)

        text = "Hello my name is"
        inputs = tokenizer(text, return_tensors="pt").to(0)

        outputs = model.generate(**inputs, max_new_tokens=20)
        print(tokenizer.decode(outputs[0], skip_special_tokens=True))

        # prompt = ("Explain the following emotional scores in simple terms: ")
        #
        # # Iterate over the DataFrame rows
        # for index, row in df.iterrows():
        #     scores = ", ".join([f"{emotion}: {row[emotion]}" for emotion in df.columns])
        #     prompt += f"{scores}"
        #
        # inputs = self.tokenizer(prompt, return_tensors="pt", max_length=512, truncation=True)
        # summary_ids = self.model.generate(inputs['input_ids'], num_beams=4, max_length=150, early_stopping=True)
        #
        # print(self.tokenizer.decode(summary_ids[0], skip_special_tokens=True))
