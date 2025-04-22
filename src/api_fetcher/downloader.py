import requests
from tqdm import tqdm
from db_reader import DatabaseReader
from db_writer import DatabaseWriter


DATA_PATH = '../../data/radios/'

class Downloader:
    @classmethod
    def download_team_radio_audio(cls):

        watermark = DatabaseReader.get_downloader_watermark()
        team_radios = DatabaseReader.get_team_radios(watermark)

        for team_radio in tqdm(team_radios, desc="Downloading team radios", unit="file"):
            url = team_radios[team_radio][3]
            response = requests.get(url)
            if response.status_code == 200:
                path = DATA_PATH + url.split('/')[-1]
                with open(path, "wb") as file:
                    file.write(response.content)
                DatabaseWriter.upsert_team_radio_file(team_radio, path)
                DatabaseWriter.update_downloader_watermark(team_radio)
                #print("Download completed successfully.")
            else:
                DatabaseWriter.insert_failed_download(team_radio, url)
                #print(f"Failed to download file. Status code: {response.status_code}")
