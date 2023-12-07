from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, regexp_replace, lower

from src.spark.util.postgres import get_spark


def main():
    spark = get_spark()

    # Read data from Postgres
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://192.168.1.217:5432/chatalytics") \
        .option("dbtable", "twitch_message") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # df = spark.read \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .options(keyspace="twitch", table="twitch_messages") \
    #     .load()

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

    twitch_messages.show(truncate=False, vertical=True)

    # twitch_messages.write \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .mode('append') \
    #     .options(keyspace="spark_output", table="twitch_messages_") \
    #     .save()

    spark.stop()


if __name__ == "__main__":
    main()
