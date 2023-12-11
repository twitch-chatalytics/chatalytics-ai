from pyspark.sql import SparkSession

extra_jars = "org.postgresql:postgresql:42.7.0"


def get_spark():
    spark = SparkSession \
        .builder \
        .appName("PySpark PostgreSQL") \
        .config("spark.jars.packages", extra_jars) \
        .config("spark.master", "spark://192.168.1.217:7077") \
        .config("spark.logConf", "true") \
        .getOrCreate()

    return spark
