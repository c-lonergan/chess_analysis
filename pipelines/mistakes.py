from pyspark.sql.functions import col, count, desc, avg
from spark_session import get_spark
from config import DATA_DIR

def mistake_stats(top_k=15):
    spark = get_spark()
    path = f"{DATA_DIR}/processed/mistakes.jsonl"
    df = spark.read.json(path)

    by_move = (
        df.groupBy("san")
        .agg(
            count("*").alias("times"),
            avg("delta").alias("avg_delta")
        )
        .orderBy(desc("times"))
        .limit(top_k)
    )

    by_move.write.mode("overwrite").parquet("data/processed/mistakes_by_move.parquet")
    return by_move
