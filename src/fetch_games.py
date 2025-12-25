import os
from pandas.io.json._normalize import nested_to_record
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

from chesscom_client import ChessComClient
from spark_session import get_spark
from utils import get_logger

from config import DATA_DIR, N_PARTITIONS

logger = get_logger()

"""
initiate client
get archive list
client.get each url in archive
for each url, get games
save to data/raw/username/game.parquet
    white & black nested
if data/raw/username doesnt exist, mkdir
if file exists, skip, unless year_month = now
"""

RAW_SCHEMA = StructType([
    StructField("uuid", StringType(), True),
    StructField("url", StringType(), True),
    StructField("end_time", LongType(), True),
    StructField("time_class", StringType(), True),
    StructField("time_control", StringType(), True),
    StructField("rules", StringType(), True),
    StructField("rated", BooleanType(), True),
    StructField("eco", StringType(), True),
    StructField("tcn", StringType(), True),
    StructField("pgn", StringType(), True),
    StructField("fen", StringType(), True),
    StructField("initial_setup", StringType(), True),
    StructField("white.uuid", StringType(), True),
    StructField("white.username", StringType(), True),
    StructField("white.result", StringType(), True),
    StructField("white.rating", LongType(), True),
    StructField("black.uuid", StringType(), True),
    StructField("black.username", StringType(), True),
    StructField("black.result", StringType(), True),
    StructField("black.rating", LongType(), True),
])

def main(spark) -> None:
    client = ChessComClient()
    username = str(client.username) 
    url_list = client.get_last_n_months_archives(n=1)
    
    output_path = f"{DATA_DIR}/raw/{username}"
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    save_archive_in_url_list(url_list, output_path, client)

def save_archive_in_url_list(url_list, output_path, client) -> None:
    for url in url_list:
        file_name = make_archive_file_name(url)
        data = client._get(url).json()['games']
        flat_data = nested_to_record(data)
        df = spark.createDataFrame(flat_data, schema=RAW_SCHEMA)
        df.coalesce(N_PARTITIONS) \
            .write \
            .mode("overwrite") \
            .parquet(os.path.join(output_path,file_name))

def make_archive_file_name(url: str) -> str:
    arr = url.rsplit("/")
    filename = f"{arr[-2]}_{arr[-1]}"
    return filename

if __name__ == "__main__":
    spark = get_spark()
    main(spark)
    spark.stop()