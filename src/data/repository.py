import pandas as pd
import psycopg2
from numpy import int64


class TwitchMessagesRepository:

    def __init__(self):
        self.connection = psycopg2.connect(
            host="192.168.1.217",
            dbname="chatalytics",
            user="user",
            password="password"
        )

    def write(self, query, parameters):
        parameters = tuple([p.item() if isinstance(p, int64) else p for p in parameters])

        with self.connection.cursor() as cursor:
            cursor.execute(query, parameters)

    def get_last_run_timestamp(self, job_type):
        with self.connection.cursor() as cursor:
            query = """
            SELECT last_run_timestamp 
            FROM twitch.spark_run_history 
            WHERE job_type = %s;
            """
            cursor.execute(query, (job_type,))
            result = cursor.fetchone()
            return result[0] if result else None

    def update_last_run(self, job_type, job_end_time):
        with self.connection.cursor() as cursor:
            query = """
            INSERT INTO twitch.spark_run_history (job_type, last_run_timestamp)
            VALUES (%s, %s)
            ON CONFLICT (job_type) 
            DO UPDATE SET last_run_timestamp = EXCLUDED.last_run_timestamp;
            """
            cursor.execute(query, (job_type, job_end_time))
            self.connection.commit()

    def fetch_twitch_messages(self, last_run_time):
        with self.connection.cursor() as cursor:
            query = """
            SELECT * 
            FROM twitch.twitch_message
            WHERE timestamp > %s;
            """
            cursor.execute(query, (last_run_time,))
            rows = cursor.fetchall()

        return pd.DataFrame(rows, columns=['id', 'owner_id', 'viewer', 'message_text', 'timestamp'])

    def __del__(self):
        self.connection.close()
