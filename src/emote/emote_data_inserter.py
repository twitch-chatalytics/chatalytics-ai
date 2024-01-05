import logging
from tqdm import tqdm


class EmoteDataInserter:

    def __init__(self, repository):
        self.repository = repository

    def insert_emotion_report(self, data, query):
        print("Starting data insertion into the database...")

        with tqdm(total=len(data), desc="Inserting data") as pbar:
            for index, row in data.iterrows():

                parameters = (row['emote'], row['image_link'], row['count'], row['streamer_id'])

                try:
                    self.repository.write(query, parameters)
                except Exception as e:
                    print(f"Error inserting data: {e}")
                    continue

                pbar.update(1)

        print("Data insertion completed.")
