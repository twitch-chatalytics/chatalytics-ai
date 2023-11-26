from pyspark.sql import SparkSession

extra_jars = "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,com.github.jnr:jnr-posix:3.1.15"


def get_spark():
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
        .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    return spark
