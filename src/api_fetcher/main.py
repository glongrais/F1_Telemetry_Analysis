from fetcher import Fetcher
from downloader import Downloader
from agent import Agent
def main():

    Fetcher.fetch_batches()
    Fetcher.retry_fetch_failed_queries()

    Downloader.download_team_radio_audio()
    Agent.run()
if __name__ == "__main__":
    main()

