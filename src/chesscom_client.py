"""
To-Do:
- Handle Exception errors rather than just raise_for_status()
- Custom date ranges
- Do we need a class?
"""

import os
import datetime as dt
import requests
from dotenv import load_dotenv

from config import CHESSCOM_USERNAME, MONTHS_BACK, DATA_DIR
from spark_session import get_spark

load_dotenv()

BASE = "https://api.chess.com/pub/player"
CHESSCOM_EMAIL = os.getenv("CHESSCOM_EMAIL")
TIMEOUT = 10

class ChessComClient:
    def __init__(self):
        self.username = CHESSCOM_USERNAME
        self.base_url = f"{BASE}/{CHESSCOM_USERNAME}"
        self.headers = {"User-Agent": f"{CHESSCOM_USERNAME}-agent (contact: {CHESSCOM_EMAIL})"}

    def _get(self, url):
        # some error handling would be nice
        r = requests.get(
            url,
            headers=self.headers,
            timeout=TIMEOUT
        )
        r.raise_for_status()
        return r

    def get_basic_info(self):
        response = self._get(self.base_url).json()
        return response
        
    def get_last_n_months_archives(self, n = MONTHS_BACK):
        url = f"{self.base_url}/games/archives"
        archives = self._get(url).json()
        return sorted(archives['archives'])[-n:]

  
