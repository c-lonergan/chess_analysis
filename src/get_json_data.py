import json
from chesscom_client import ChessComClient

client = ChessComClient()

username = 'charlie_barly'
output_path = '/home/charlie/coding/chess_analytics/data/json/data.json'

url_list = client.get_last_n_months_archives()
url = url_list[-1]
data = client._get(url).json()

with open(output_path, 'w') as f:
    json.dump(data, f)