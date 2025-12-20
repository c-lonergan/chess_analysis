from pyspark.sql.functions import col, to_date
from spark_session import get_spark
from .common import load_raw_games

def rapid_rating_trend():
    spark = get_spark()
    rapid = load_raw_games()

    # consider last 90 days
    rapid_90 = rapid.filter(
        col("end_time_ts") >= (spark.sql("select date_sub(current_date(), 90)").first()[0])
    )

    trend = (
        rapid_90
        .withColumn("game_date", to_date(col("end_time_ts")))
        .select("game_date", "your_rating")
        .orderBy("game_date")
    )

    # Persist to disk or return DataFrame
    trend.write.mode("overwrite").parquet("data/processed/rating_trend.parquet")
    return trend
