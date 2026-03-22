import argparse
import os
import sys

# Add fastf1_fetcher to path for schema import
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'fastf1_fetcher'))
# Add this directory for local imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from schema import init_schema  # noqa: E402
from fetcher import LivetimingFetcher  # noqa: E402


def main():
    parser = argparse.ArgumentParser(
        description="F1 Telemetry Ingestion via Livetiming API"
    )
    parser.add_argument("--year", type=int, default=2026, help="Season year (default: 2026)")
    parser.add_argument("--round", type=int, default=None, help="Fetch single round only")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4)")
    args = parser.parse_args()

    print(f"Initializing schema...")
    init_schema()

    print(f"Fetching {args.year} season via livetiming API...")
    fetcher = LivetimingFetcher(max_workers=args.workers)
    fetcher.fetch_season(args.year, round_filter=args.round)


if __name__ == "__main__":
    main()
