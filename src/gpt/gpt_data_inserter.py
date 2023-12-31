from tqdm import tqdm


class GPTDataInserter:
    def __init__(self, repository):
        self.repository = repository

    def insert_emotion_report(self, data, query):
        print("Starting data insertion into the database...")

        with tqdm(total=len(data), desc="Inserting data", colour='green') as pbar:
            for index, row in data.iterrows():

                parameters = (

                )

                try:
                    self.repository.write(query, parameters)
                except Exception as e:
                    print(f"Error inserting data: {e}")
                    continue

                pbar.update(1)

        print("Data insertion completed.")
