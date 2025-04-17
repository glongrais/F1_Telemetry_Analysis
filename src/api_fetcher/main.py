from fetcher import Fetcher

def main():

    #Fetcher.fetch_batches()
    Fetcher.retry_fetch_failed_queries()

if __name__ == "__main__":
    main()

