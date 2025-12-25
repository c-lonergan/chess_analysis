import os

import json

from typing import List, Dict
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, LongType, BoolType

from chesscom_client import ChessComClient
from spark_session import get_spark
from config import DATA_DIR, N_PARTITIONS
from utils import get_logger

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
    StructField("rated", BoolType(), True),
    StructField("pgn", StringType(), True),
])

def main(spark) -> None:
    client = ChessComClient()
    username = str(client.username) 
    url_list = client.get_last_n_months_archives() #n=1
    
    output_path = f"{DATA_DIR}/raw/{username}"
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    save_archive_in_url_list(url_list, output_path, client)

def save_archive_in_url_list(url_list, output_path, client) -> None:
    for url in url_list:
        file_name = make_archive_file_name(url)
        archive = client._get(url).json()['games']

    #     with open('/home/charlie/coding/chess_analytics/data/json/data.json', 'w') as f:
    #         json.dump(archive, f)
    # return

        df = spark.createDataFrame(archive, schema=RAW_SCHEMA)
        df.coalesce(N_PARTITIONS) \
            .write \
            .mode("overwrite") \
            .parquet(os.path.join(output_path,file_name))

def make_archive_file_name(url: str) -> str:
    arr = url.rsplit("/")
    filename = f"{arr[-2]}_{arr[-1]}"
    return filename

if __name__ == "__main__":
    # spark = None 
    spark = get_spark()
    main(spark)
    spark.stop()