from gpt.gpt_analyzer import GPTAnalyzer
from gpt.gpt_data_inserter import GPTDataInserter


class GPTAnalysisJob:
    def __init__(self, repository):
        self.repository = repository
        self.gpt_analyzer = GPTAnalyzer()
        self.data_inserter = GPTDataInserter(repository)

    def process(self, last_run_time):
        print("Processing GPTAnalysisJob...")

        df = self.repository.get_emotional_trends_over_time(71092938)

        if df.empty:
            print("No new messages to process.")
            return

        response = self.gpt_analyzer.generate_summary(df)

        print(f"Response: {response}")
