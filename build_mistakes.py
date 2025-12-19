# chess_analytics/enrich_mistakes.py
import json
from pathlib import Path
from config import DATA_DIR, CHESSCOM_USERNAME
from stockfish_eval import find_first_blunder

def build_mistakes_file():
    raw_dir = Path(DATA_DIR) / "raw"
    out_path = Path(DATA_DIR) / "processed" / "mistakes.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as out_f:
        for f in raw_dir.glob("games_*.json"):
            with open(f, "r", encoding="utf-8") as g:
                data = json.load(g)
            for game in data["games"]:
                if "pgn" not in game:
                    continue
                you_white = game["white"]["username"] == CHESSCOM_USERNAME
                res = find_first_blunder(game["pgn"], you_white)
                if res is None:
                    continue
                out_f.write(json.dumps({
                    "url": game.get("url"),
                    "is_white": you_white,
                    "move_index": res["move_index"],
                    "san": res["san"],
                    "prev_eval": res["prev_eval"],
                    "new_eval": res["new_eval"],
                    "delta": res["delta"],
                }) + "\n")
