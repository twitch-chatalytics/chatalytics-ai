import re

import pandas as pd

from emote.emote_analyzer import EmoteAnalyzer
from emote.emote_data_inserter import EmoteDataInserter


class EmoteAnalysisJob:

    def __init__(self, repository):
        self.repository = repository
        self.emote_analyzer = EmoteAnalyzer()
        self.data_inserter = EmoteDataInserter(repository)

    def process(self, last_run_time):
        print("Processing EmoteAnalysisJob...")

        user_df = self.repository.get_twitch_users()
        messages_df = self.repository.fetch_all_twitch_messages()

        if messages_df.empty:
            print("No new messages to process.")
            return

        try:
            twitch_emotes = self.emote_analyzer.get_twitch_default_emotes()

            emote_rows = []
            for _, user in user_df.iterrows():
                try:
                    # streamer_custom_emotes = self.emote_analyzer.get_channel_emotes(user['login'])
                    streamer_custom_emotes = {}
                except Exception as e:
                    streamer_custom_emotes = {}

                # Combine default and custom emotes
                all_emotes = {**twitch_emotes, **streamer_custom_emotes}

                # Filter messages for the current user
                user_messages = messages_df[messages_df['owner_id'] == user['user_id']]

                # Analyze emote usage in messages
                # Assuming all_emotes is a list of dictionaries as per the provided structure
                for emote_data in all_emotes['data']:
                    emote_name = emote_data['name'].lower()
                    emote_name_escaped = re.escape(emote_name)
                    image_link = emote_data['images']['url_4x']  # Extract the url_1x value from the images dictionary

                    # Count the occurrences of each emote in the user's messages
                    emote_count = user_messages['message_text'].str.lower().str.count(emote_name_escaped.lower()).sum()

                    # Append the data to emote_rows
                    emote_rows.append({
                        'emote': emote_name,
                        'image_link': image_link,
                        'count': emote_count,
                        'owner_id': user['user_id']
                    })

            emote_usage_df = pd.DataFrame(emote_rows)

            insert_query = """
                INSERT INTO twitch.spark_emote_usage (
                    emote, image_link, count, owner_id
                ) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (emote, owner_id) 
                DO UPDATE SET 
                    count = twitch.spark_emote_usage.count + EXCLUDED.count;
            """

            self.data_inserter.insert_emotion_report(emote_usage_df, insert_query)

        except Exception as e:
            print(f"Error in EmoteAnalysisJob: {e}")
