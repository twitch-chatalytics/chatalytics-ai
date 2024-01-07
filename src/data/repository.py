import pandas as pd
import psycopg2
from numpy import int64


class Repository:

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

    def get_twitch_streamers(self):
        with self.connection.cursor() as cursor:
            query = """
            SELECT * 
            FROM twitch.twitch_streamer
            """
            cursor.execute(query, )
            rows = cursor.fetchall()

        return pd.DataFrame(rows, columns=[
            'id',
            'user_id',
            'login',
            'display_name',
            'type',
            'broadcaster_type',
            'description',
            'profile_image_url',
            'offline_image_url',
            'created_date',
            'last_modified_date'
        ])

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

        return pd.DataFrame(rows, columns=['id', 'streamer_id', 'viewer_id', 'message_text', 'timestamp'])

    def fetch_all_twitch_messages(self):
        with self.connection.cursor() as cursor:
            query = """
            SELECT * 
            FROM twitch.twitch_message;
            """
            cursor.execute(query, )
            rows = cursor.fetchall()

        return pd.DataFrame(rows, columns=['id', 'streamer_id', 'viewer_id', 'message_text', 'timestamp'])

    def get_emotional_trends_over_time(self, streamer_id):
        with self.connection.cursor() as cursor:
            query = """
            SELECT DATE(tm.timestamp) AS message_date,
               AVG(smer.sadness)  AS avg_sadness,
               AVG(smer.joy)      AS avg_joy,
               AVG(smer.love)     AS avg_love,
               AVG(smer.anger)    AS avg_anger,
               AVG(smer.fear)     AS avg_fear,
               AVG(smer.surprise) AS avg_surprise
            FROM twitch.twitch_message tm
                     JOIN
                 twitch.spark_message_emotion_ranking smer ON tm.id = smer.message_id
            WHERE tm.streamer_id = %s
            GROUP BY message_date
            ORDER BY message_date;
            """
            cursor.execute(query, (streamer_id,))
            rows = cursor.fetchall()

        return pd.DataFrame(rows,
                            columns=['message_date', 'avg_sadness', 'avg_joy', 'avg_love', 'avg_anger', 'avg_fear',
                                     'avg_surprise'])

    def __del__(self):
        self.connection.close()
