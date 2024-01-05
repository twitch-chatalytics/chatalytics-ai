import pandas as pd

from emotion.emotion_analyzer import EmotionAnalyzer
from emotion.emotion_data_inserter import EmotionDataInserter


class EmotionAnalysisJob:

    def __init__(self, repository):
        self.repository = repository
        self.emotion_analyzer = EmotionAnalyzer()
        self.data_inserter = EmotionDataInserter(repository)

    def process(self, last_run_time):
        print("Processing EmotionAnalysisJob...")

        df = self.repository.fetch_twitch_messages(last_run_time)

        if df.empty:
            print("No new messages to process.")
            return

        try:
            emotion_scores = self.emotion_analyzer.predict_emotions(df['message_text'].tolist())
            emotion_labels = ['sadness', 'joy', 'love', 'anger', 'fear', 'surprise']

            emotions_df = pd.DataFrame(emotion_scores, columns=emotion_labels)
            emotions_df['message_id'] = df['id']
            emotions_df['streamer_id'] = df['streamer_id']
            emotions_df['viewer'] = df['viewer']

            insert_query = """
                INSERT INTO twitch.spark_message_emotion_ranking (
                    message_id, streamer_id, viewer, sadness, joy, love, anger, fear, surprise
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_id) 
                DO UPDATE SET 
                    sadness = EXCLUDED.sadness, 
                    joy = EXCLUDED.joy, 
                    love = EXCLUDED.love, 
                    anger = EXCLUDED.anger, 
                    fear = EXCLUDED.fear, 
                    surprise = EXCLUDED.surprise;
            """

            self.data_inserter.insert_emotion_report(emotions_df, insert_query)
        except Exception as e:
            print(f"Error in EmotionAnalysisJob: {e}")
