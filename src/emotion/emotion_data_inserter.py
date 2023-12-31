import logging
from tqdm import tqdm


class EmotionDataInserter:
    """
    Class responsible for inserting emotion analysis data into the database.
    """

    def __init__(self, repository):
        """
        Initializes the EmotionDataInserter with a repository.

        Args:
            repository: The repository for database operations.
        """
        self.repository = repository

    def insert_emotion_report(self, data, query):
        """
        Inserts the emotion analysis data into the database.

        Args:
            data (pandas.DataFrame): DataFrame containing the emotion analysis data.
            query (str): SQL query for inserting the data.
        """
        logging.info("Starting data insertion into the database...")

        with tqdm(total=len(data), desc="Inserting data", colour='green') as pbar:
            for row in data.itertuples():
                if not self._validate_row(row):
                    logging.warning(f"Skipping invalid row: message_id={row.message_id}, owner_id={row.owner_id}")
                    continue

                parameters = (
                    row.message_id, row.owner_id, row.viewer, row.sadness, row.joy, row.love, row.anger, row.fear,
                    row.surprise
                )

                try:
                    self.repository.write(query, parameters)
                except Exception as e:
                    logging.error(f"Error inserting data: {e}")
                    continue

                pbar.update(1)

        logging.info("Data insertion completed.")

    def _validate_row(self, row):
        """
        Validates a row of data to ensure it meets the criteria for insertion.

        Args:
            row (pd.Series): A single row of data.

        Returns:
            bool: True if valid, False otherwise.
        """
        return isinstance(row.message_id, int) and isinstance(row.owner_id, int)