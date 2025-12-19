from chesscom_client import download_archives
from pipelines.rating_trend import rapid_rating_trend
from pipelines.openings import opening_stats
from enrich_openings import build_opening_file
from enrich_mistakes import build_mistakes_file
from pipelines.mistakes import mistake_stats

def main():
    download_archives()
    # opening enrichment
    build_opening_file()
    # mistake enrichment (engine-heavy; might take time)
    build_mistakes_file()

    # analytics
    trend = rapid_rating_trend()
    white_openings, black_openings = opening_stats()
    mistakes = mistake_stats()

if __name__ == "__main__":
    main()
