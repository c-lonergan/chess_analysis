import json
import pandas as pd
from pandas.io.json._normalize import nested_to_record
from pyspark.sql import functions as F

from spark_session import get_spark

# spark = get_spark()
path = '/home/charlie/coding/chess_analytics/data/json/data.json'

def flatten(l):
    return [item for sublist in l for item in sublist]

with open(path) as f:
    games = json.load(f)['games']

new_data = nested_to_record(games)
print(new_data[0])


# flat_data = flatten(data['games'])
# print(flat_data)
# spark.stop()