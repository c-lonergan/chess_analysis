import chess
import chess.pgn
import io
from stockfish import Stockfish
from config import STOCKFISH_PATH, STOCKFISH_DEPTH, CHESSCOM_USERNAME

stockfish = Stockfish(path=STOCKFISH_PATH)
stockfish.set_depth(STOCKFISH_DEPTH)

def eval_position(board: chess.Board) -> float:
    stockfish.set_fen_position(board.fen())
    info = stockfish.get_evaluation()
    if info["type"] == "cp":
        return info["value"] / 100.0
    else:
        # mate scores; clamp to large value
        sign = 1 if info["value"] > 0 else -1
        return 20.0 * sign

def find_first_blunder(pgn_str: str, you_are_white: bool, threshold: float = 1.5):
    game = chess.pgn.read_game(io.StringIO(pgn_str))
    board = game.board()
    evals = []
    prev_eval = None
    move_index = 0
    for move in game.mainline_moves():
        board.push(move)
        raw_eval = eval_position(board)
        eval_for_you = raw_eval if you_are_white else -raw_eval
        if prev_eval is not None:
            drop = prev_eval - eval_for_you
            if drop >= threshold:
                # blunder on this move from your perspective
                san = board.san(move)
                return {
                    "move_index": move_index,
                    "san": san,
                    "prev_eval": prev_eval,
                    "new_eval": eval_for_you,
                    "delta": drop,
                }
        prev_eval = eval_for_you
        move_index += 1
    return None
