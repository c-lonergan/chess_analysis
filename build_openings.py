# chess_analytics/enrich_openings.py
import json
from pathlib import Path
import chess.pgn
from config import DATA_DIR, CHESSCOM_USERNAME

def parse_opening_from_pgn(pgn_str):
    game = chess.pgn.read_game(io.StringIO(pgn_str))
    eco = game.headers.get("ECO", None)
    opening = game.headers.get("Opening", None)
    return eco, opening

def build_opening_file():
    raw_dir = Path(DATA_DIR) / "raw"
    out_path = Path(DATA_DIR) / "processed" / "openings.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as out_f:
        for f in raw_dir.glob("games_*.json"):
            with open(f, "r", encoding="utf-8") as g:
                data = json.load(g)
            for game in data["games"]:
                if "pgn" not in game:
                    continue
                eco, opening = parse_opening_from_pgn(game["pgn"])
                rec = {
                    "url": game.get("url"),
                    "pgn": None,  # drop pgn to keep file small if you want
                    "white": game["white"]["username"],
                    "black": game["black"]["username"],
                    "is_white": game["white"]["username"] == CHESSCOM_USERNAME,
                    "eco": eco,
                    "opening": opening,
                }
                out_f.write(json.dumps(rec) + "\n")
