from pyspark.sql.functions import sum as _sum

from src.spark.util.postgres import get_spark

extra_jars = "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,com.github.jnr:jnr-posix:3.1.15"


def main():
    spark = get_spark()

    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="twitch_messages", table="count") \
        .load()

    # Group by 'channel_name' and sum 'count'
    aggregated_df = df.groupBy("channel_name").agg(_sum("count").alias("total_count"))

    # Write aggregated data back to a new table in Cassandra
    aggregated_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="spark_output", table="channel_totals") \
        .save(mode="append")

    spark.stop()


if __name__ == "__main__":
    main()
