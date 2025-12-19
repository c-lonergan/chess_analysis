"""
To-Do:
- Handle Exception errors rather than just raise_for_status()
- Custom date ranges
- Do we need a class?
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
        return r

    def get_basic_info(self):
        response = self._get(self.base_url).json()
        return response
        
    def get_last_n_months_archives(self, n = MONTHS_BACK):
        url = f"{self.base_url}/games/archives"
        archives = self._get(url).json()
        return sorted(archives['archives'])[-n:]

    def make_archive_file_name(self, url: str) -> str:
        arr = url.rsplit("/")
        filename = f"games_{arr[-4]}_{arr[-2]}_{arr[-1]}.json"
        return filename

    def download_archives(self):
        raw_dir = os.path.join(DATA_DIR, "raw")
        os.makedirs(raw_dir, exist_ok=True)

        archives = self.get_last_n_months_archives()
        
        out_files = []
        for url in archives:
            filename = self.make_archive_file_name(url)
            out_path = os.path.join(raw_dir, filename)
            if os.path.isfile(out_path):
                out_files.append(str(out_path))
                continue
            r = self._get(url)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(r.json(), f)
            out_files.append(str(out_path))
        
        return out_files

if __name__ == "__main__":
    client = ChessComClient()
    files = client.download_archives()
    print(files)
