from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, regexp_replace, lower

from spark.util.cassandra import get_spark

extra_jars = "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,com.github.jnr:jnr-posix:3.1.15"


def main():
    spark = get_spark()

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
