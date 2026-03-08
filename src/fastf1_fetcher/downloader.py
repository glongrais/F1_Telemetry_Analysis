import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from tqdm import tqdm
import db_reader
import db_writer

DATA_PATH = os.path.join(os.path.dirname(__file__), '../../data/radios/')


class Downloader:
    @classmethod
    def download_team_radio_audio(cls, workers=8):
        os.makedirs(DATA_PATH, exist_ok=True)

        watermark = db_reader.get_downloader_watermark()
        team_radios = db_reader.get_team_radios(watermark)

        results = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(cls._download_file, radio): radio
                for radio in team_radios if radio['recording_url']
            }
            for future in tqdm(as_completed(futures), total=len(futures),
                               desc="Downloading team radios", unit="file"):
                radio = futures[future]
                try:
                    path = future.result()
                    results.append((radio['team_radio_id'], path, None))
                except Exception as e:
                    results.append((radio['team_radio_id'], None, radio['recording_url']))

        # Write to DB sequentially in ID order to keep watermark consistent
        results.sort(key=lambda r: r[0])
        conn = db_writer.get_connection()
        try:
            for team_radio_id, path, failed_url in results:
                if path:
                    db_writer.upsert_team_radio_file(conn, team_radio_id, path)
                else:
                    db_writer.insert_failed_download(conn, team_radio_id, failed_url)
                db_writer.update_downloader_watermark(conn, team_radio_id)
        finally:
            conn.close()

    @classmethod
    def _download_file(cls, radio):
        url = radio['recording_url']
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            path = os.path.join(DATA_PATH, url.split('/')[-1])
            with open(path, "wb") as f:
                f.write(response.content)
            return path
        raise requests.RequestException(f"HTTP {response.status_code}")
