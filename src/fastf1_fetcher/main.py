import argparse
from schema import init_schema
from fetcher import FastF1Fetcher
from downloader import Downloader
from agent import Agent


def main():
    parser = argparse.ArgumentParser(description="F1 Telemetry Ingestion via FastF1")
    parser.add_argument("--start-year", type=int, default=2018, help="First year to ingest")
    parser.add_argument("--end-year", type=int, default=2025, help="Last year to ingest")
    parser.add_argument("--skip-radio", action="store_true", help="Skip team radio download and transcription")
    parser.add_argument("--workers", type=int, default=4, help="Parallel FastF1 session loaders")
    args = parser.parse_args()

    print("Initializing schema...")
    init_schema()

    fetcher = FastF1Fetcher(max_workers=args.workers)

    for year in range(args.start_year, args.end_year + 1):
        print(f"\n=== Fetching season {year} ===")
        fetcher.fetch_season(year)

    print("\n=== Retrying failed sessions ===")
    fetcher.retry_failed()

    if not args.skip_radio:
        print("\n=== Downloading team radio audio ===")
        Downloader.download_team_radio_audio()

        print("\n=== Transcribing team radio ===")
        Agent.run()

    print("\nDone!")


if __name__ == "__main__":
    main()
