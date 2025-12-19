from pyspark.sql import SparkSession
from config import SPARK_APP_NAME, SPARK_MASTER

def get_spark():
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .getOrCreate()
    )
