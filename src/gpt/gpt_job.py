from gpt.gpt_analyzer import GPTAnalyzer
from gpt.gpt_data_inserter import GPTDataInserter


class GPTAnalysisJob:
    def __init__(self, repository):
        self.repository = repository
        self.emotion_analyzer = GPTAnalyzer()
        self.data_inserter = GPTDataInserter(repository)
