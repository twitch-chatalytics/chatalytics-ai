# toxicity_job.py

from toxicity.toxicity_analyzer import ToxicityAnalyzer
from toxicity.toxicity_data_inserter import ToxicityDataInserter


class ToxicityAnalysisJob:
    def __init__(self, repository):
        self.repository = repository
        self.toxicity_analyzer = ToxicityAnalyzer()
        self.data_inserter = ToxicityDataInserter(repository)

    def process(self, last_run_time):
        df = self.repository.fetch_twitch_messages(last_run_time)

        if df.empty:
            print("No new messages to process.")
            return

        df['toxicity_score'] = self.toxicity_analyzer.predict_toxicity(df['message_text'].tolist())

        toxicity_threshold = 0.5
        user_toxicity = df.groupby('viewer').agg(
            average_toxicity=('toxicity_score', 'mean'),
            toxic_message_count=('toxicity_score', lambda x: (x > toxicity_threshold).sum())
        )

        insert_query = "INSERT INTO twitch.spark_message_toxicity_report (viewer, average_toxicity, toxic_message_count) VALUES (%s, %s, %s)"
        self.data_inserter.insert_toxicity_report(user_toxicity, insert_query)
