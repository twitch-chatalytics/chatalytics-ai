from datetime import datetime, timedelta, timezone

import pandas as pd


class JobRunner:
    def __init__(self, repository, job_type):
        self.repository = repository
        self.job_type = job_type

    def get_last_run_timestamp(self):
        return self.repository.get_last_run_timestamp(self.job_type)

    def update_last_run_timestamp(self, timestamp):
        self.repository.update_last_run(self.job_type, timestamp)

    def run(self, job):
        job_start_time = datetime.now(timezone.utc)
        last_run_time = self.get_last_run_timestamp()

        if last_run_time is None:
            last_run_time = job_start_time - timedelta(days=200)

        df = self.repository.fetch_twitch_messages(last_run_time)

        if df.empty:
            print("No new messages to process.")
            return

        # Convert the 'timestamp' column to datetime if it's not already
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        last_run_time = last_run_time.replace(tzinfo=None)

        # Filter out messages older than last_run_time
        original_count = len(df)
        df = df[df['timestamp'] > last_run_time]
        filtered_count = original_count - len(df)

        print(f"Total messages fetched: {original_count}")
        print(f"Messages filtered out (older than last run time): {filtered_count}")

        if df.empty:
            print("No new messages to process after filtering.")
            return

        job.process(df)

        self.update_last_run_timestamp(job_start_time)
