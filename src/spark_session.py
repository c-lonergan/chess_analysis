from pyspark.sql import SparkSession
from config import APP_NAME, SPARK_MASTER

def get_spark():
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .master(SPARK_MASTER)
        .getOrCreate()
    )