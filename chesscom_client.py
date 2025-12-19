import os
import datetime as dt
import requests
import json
from pathlib import Path
from config import CHESSCOM_USERNAME, DAYS_BACK, DATA_DIR

BASE = "https://api.chess.com/pub/player"

def list_archives(username: str):
    url = f"{BASE}/{username}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()["archives"]

def last_n_month_archives(n: int):
    archives = list_archives(CHESSCOM_USERNAME)
    return sorted(archives)[-n:]

def download_archives():
    raw_dir = Path(DATA_DIR) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    archives = last_n_month_archives(DAYS_BACK)
    out_files = []
    for url in archives:
        month_str = url.rsplit("/", 1)[-1]
        out_path = raw_dir / f"games_{month_str}.json"
        if out_path.exists():
            out_files.append(str(out_path))
            continue
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(r.json(), f)
        out_files.append(str(out_path))
    return out_files

if __name__ == "__main__":
    url = f"{BASE}/{CHESSCOM_USERNAME}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    print(r.json())