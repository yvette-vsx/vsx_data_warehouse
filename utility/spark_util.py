import os
from pyspark.sql import SparkSession
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark():
    spark = (
        SparkSession.builder.master("local[4]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.3.3")
        .getOrCreate()
    )
    return spark
