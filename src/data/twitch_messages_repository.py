import pandas as pd

from data.database import CassandraDB

import pandas as pd


class TwitchMessagesRepository:

    def __init__(self):
        self.cassandra = CassandraDB(hosts="192.168.1.217", keyspace="twitch_messages")

    def fetch_twitch_messages(self):
        query = "SELECT message_text, user FROM twitch_messages.twitch_messages"
        rows = self.cassandra.fetch_data(query)

        return pd.DataFrame(rows)
