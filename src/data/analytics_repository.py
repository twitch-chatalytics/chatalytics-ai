import pandas as pd

from data.database import CassandraDB


class AnalyticsRepository:

    def __init__(self):
        self.connection = CassandraDB(hosts="192.168.1.217", keyspace="analytics")

    def write(self, query, parameters):
        self.connection.session.execute(query=query, parameters=parameters)
