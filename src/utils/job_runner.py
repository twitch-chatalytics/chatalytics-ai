from datetime import datetime, timedelta, timezone

import pandas as pd


class JobRunner:
    def __init__(self, repository, job_type):
        self.repository = repository
        self.job_type = job_type

    def get_last_run_timestamp(self):
        print("Getting last run timestamp...")

        return self.repository.get_last_run_timestamp(self.job_type)

    def update_last_run_timestamp(self, timestamp):
        print("Updating last run timestamp")

        self.repository.update_last_run(self.job_type, timestamp)

    def run(self, job):
        job_start_time = datetime.now(timezone.utc)
        last_run_time = self.get_last_run_timestamp()

        if last_run_time is None:
            last_run_time = job_start_time - timedelta(minutes=1)

        job.process(last_run_time)

        self.update_last_run_timestamp(job_start_time)
