"""
To-Do:
- Handle Exception errors rather than just raise_for_status()
"""

import os
import datetime as dt
import requests
import json
from pathlib import Path
from dotenv import load_dotenv

from config import CHESSCOM_USERNAME, MONTHS_BACK, DATA_DIR

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
        r = requests.get(
            url,
            headers=self.headers,
            timeout=TIMEOUT
        )
        r.raise_for_status()
        return r.json()

    def get_basic_info(self):
        return self._get(self.base_url)
        
    def get_last_n_months_archives(self, n = MONTHS_BACK):
        url = f"{self.base_url}/games/archives"
        archives = self._get(url)
        return sorted(archives['archives'])[-n:]

    def download_archives(self):
        raw_dir = Path(DATA_DIR) / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        archives = self.get_last_n_months_archives()
        out_files = []
        for url in archives:
            month_str = url.rsplit("/", 1)[-1]
            out_path = raw_dir / f"games_{month_str}.json"
            if out_path.exists():
                out_files.append(str(out_path))
                continue
            r = self._get(url)
            r.raise_for_status()
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(r.json(), f)
            out_files.append(str(out_path))
        return out_files

if __name__ == "__main__":
    client = ChessComClient()
    # info = client.get_basic_info()
    # print(info)
    files = client.download_archives()
    print(files)
