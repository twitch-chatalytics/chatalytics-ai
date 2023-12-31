from tqdm import tqdm


class ToxicityDataInserter:
    def __init__(self, repository):
        self.repository = repository

    def insert_toxicity_report(self, data, query):
        total_rows = len(data)

        with tqdm(total=total_rows, desc="Inserting data") as pbar:
            for index, row in data.iterrows():
                toxic_message_count = int(row['toxic_message_count'])
                self.repository.write(query, (index, row['average_toxicity'], toxic_message_count))
                pbar.update(1)
