from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, lower

extra_jars = "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,com.github.jnr:jnr-posix:3.1.15"


def main():
    # Initialize a SparkSession
    spark = SparkSession \
        .builder \
        .appName("PySpark Cassandra") \
        .config("spark.jars.packages", extra_jars) \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.master", "spark://192.168.1.38:7077") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "4g") \
        .config("spark.cassandra.connection.host", "192.168.1.217") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.default.parallelism", "1") \
        .config("spark.driver.host", "192.168.1.228") \
        .config("spark.driver.bindAddress", "192.168.1.228") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    # Read data from Cassandra
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="twitch_messages", table="twitch_messages") \
        .load()

    # Convert text to lowercase
    twitch_messages = df.withColumn("message_text", lower(col("message_text")))

    # Remove punctuation and special characters (keeping spaces)
    twitch_messages = twitch_messages.withColumn("message_text",
                                                 regexp_replace(col("message_text"), "[^a-zA-Z\\s]", ""))

    # Tokenize text to words for StopWordsRemover
    tokenizer = Tokenizer(inputCol="message_text", outputCol="words")
    twitch_messages = tokenizer.transform(twitch_messages)

    # Remove stop words
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    twitch_messages = remover.transform(twitch_messages)

    twitch_messages.printSchema()

    twitch_messages.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(keyspace="spark_output", table="twitch_messages_") \
        .save()

    spark.stop()


if __name__ == "__main__":
    main()
