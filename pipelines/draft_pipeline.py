# chess_analytics/pipelines/common.py
import json
from pathlib import Path
from pyspark.sql.functions import col, lit, regexp_extract, when
from pyspark.sql import DataFrame
from config import CHESSCOM_USERNAME, DATA_DIR
from spark_session import get_spark

def load_raw_games() -> DataFrame:
    spark = get_spark()
    raw_dir = Path(DATA_DIR) / "raw"
    files = [str(p) for p in raw_dir.glob("games_*.json")]
    df = spark.read.json(files)

    # archives JSON is { "games": [ ... ] }
    games = df.selectExpr("explode(games) as game").select("game.*")

    # mark whether you are white or black
    games = games.withColumn(
        "is_white",
        (col("white.username") == lit(CHESSCOM_USERNAME))
    )

    # filter rapid by time_class or time_control when available
    # some wrappers store "time_class", others have "time_control"
    games = games.withColumn(
        "time_class",
        when(col("time_class").isNotNull(), col("time_class"))
        .otherwise(
            when(col("time_control").startswith("600"),
                 lit("rapid"))
        )
    )

    rapid = games.filter(col("time_class") == "rapid")

    # Extract your rating and opponent rating from PGN header-like fields if present
    rapid = rapid.withColumn(
        "your_rating",
        when(col("is_white"), col("white.rating")).otherwise(col("black.rating"))
    ).withColumn(
        "opp_rating",
        when(col("is_white"), col("black.rating")).otherwise(col("white.rating"))
    )

    # unify game end time
    rapid = rapid.withColumn(
        "end_time_ts",
        col("end_time").cast("timestamp")
    )

    return rapid
