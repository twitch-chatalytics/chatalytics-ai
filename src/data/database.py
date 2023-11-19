from cassandra.cluster import Cluster, PlainTextAuthProvider


class CassandraDB:
    def __init__(self, hosts, keyspace, username=None, password=None):
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            self.cluster = Cluster([hosts], auth_provider=auth_provider)  # Make sure hosts is a list
        else:
            self.cluster = Cluster([hosts])  # Make sure hosts is a list
        self.keyspace = keyspace
        self.session = self.cluster.connect(self.keyspace)

    def fetch_data(self, query):
        try:
            rows = self.session.execute(query)
            return rows
        except Exception as e:
            print("Error fetching data from Cassandra:", e)
            return None

    def close(self):
        self.cluster.shutdown()
