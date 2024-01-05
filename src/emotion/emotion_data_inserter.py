import logging
from tqdm import tqdm


class EmotionDataInserter:

    def __init__(self, repository):
        self.repository = repository

    def insert_emotion_report(self, data, query):
        print("Starting data insertion into the database...")

        with tqdm(total=len(data), desc="Inserting data") as pbar:
            for index, row in data.iterrows():

                parameters = (
                    row.message_id, row.streamer_id, row.viewer, row.sadness, row.joy, row.love, row.anger, row.fear,
                    row.surprise
                )

                try:
                    self.repository.write(query, parameters)
                except Exception as e:
                    print(f"Error inserting data: {e}")
                    continue

                pbar.update(1)

        print("Data insertion completed.")
