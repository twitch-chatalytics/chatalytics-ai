from pyspark.sql import SparkSession


def main():
    # Initialize a SparkSession
    spark = SparkSession \
        .builder \
        .appName("PySpark Cassandra Example") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.master", "spark://localhost:7077") \
        .config("spark.cassandra.connection.host", "192.168.1.217") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    # Read data from Cassandra
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="twitch_messages", table="twitch_messages") \
        .load()

    df.printSchema()

    # Show the DataFrame
    df.show()

    amount = df.count()

    print(f"Count: {amount}")

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
