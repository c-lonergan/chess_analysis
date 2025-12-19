from pyspark.sql.functions import col, count, desc
from spark_session import get_spark
from config import DATA_DIR, CHESSCOM_USERNAME

def opening_stats(top_k=10):
    spark = get_spark()
    path = f"{DATA_DIR}/processed/openings.jsonl"
    df = spark.read.json(path)

    my_games = df.filter(
        (col("white") == CHESSCOM_USERNAME) | (col("black") == CHESSCOM_USERNAME)
    )

    white_openings = (
        my_games.filter(col("is_white"))
        .groupBy("opening")
        .agg(count("*").alias("games"))
        .orderBy(desc("games"))
        .limit(top_k)
    )

    black_openings = (
        my_games.filter(~col("is_white"))
        .groupBy("opening")
        .agg(count("*").alias("games"))
        .orderBy(desc("games"))
        .limit(top_k)
    )

    white_openings.write.mode("overwrite").parquet("data/processed/white_openings.parquet")
    black_openings.write.mode("overwrite").parquet("data/processed/black_openings.parquet")

    return white_openings, black_openings
