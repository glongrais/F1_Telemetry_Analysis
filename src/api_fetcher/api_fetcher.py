import requests
import sqlite3

DB_PATH = '../../data/f1_data.db'

class ApiFetcher:

    @classmethod
    def fetch_data(cls, api_url):
        """
        Fetches data from the given API URL and handles errors gracefully.
        """
        try:
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an error for bad status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {api_url}: {e}")
            with open("failed_api_calls.txt", "a") as file:
                file.write(f"{api_url}\n")
            return []

    @classmethod
    def get_car_data(cls, params=""):
        """
        Fetches car data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/car_data" + params
        return cls.fetch_data(api_url)

    @classmethod
    def get_drivers(cls):
        """
        Fetches driver data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/drivers"
        return cls.fetch_data(api_url)

    @classmethod
    def get_intervals(cls):
        """
        Fetches interval data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/intervals+"
        return cls.fetch_data(api_url)

    @classmethod
    def get_laps(cls, params=""):
        """
        Fetches lap data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/laps" + params
        return cls.fetch_data(api_url)

    @classmethod
    def get_locations(cls):
        """
        Fetches location data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/location"
        return cls.fetch_data(api_url)

    @classmethod
    def get_meetings(cls):
        """
        Fetches meetings data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/meetings"
        return cls.fetch_data(api_url)

    @classmethod
    def get_pits(cls):
        """
        Fetches pit data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/pit"
        return cls.fetch_data(api_url)

    @classmethod
    def get_positions(cls, params=""):
        """
        Fetches position data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/position" + params
        return cls.fetch_data(api_url)

    @classmethod
    def get_race_control(cls):
        """
        Fetches race control data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/race_control"
        return cls.fetch_data(api_url)

    @classmethod
    def get_sessions(cls):
        """
        Fetches sessions data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/sessions"
        return cls.fetch_data(api_url)

    @classmethod
    def get_stints(cls):
        """
        Fetches stints data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/stints"
        return cls.fetch_data(api_url)

    @classmethod
    def get_team_radio(cls):
        """
        Fetches team radio data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/team_radio"
        return cls.fetch_data(api_url)

    @classmethod
    def get_weather(cls):
        """
        Fetches weather data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/weather"
        return cls.fetch_data(api_url)
    
    @classmethod
    def get_sessions_context(cls):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM core__sessions_context")
        sessions_context = cursor.fetchall()
        conn.close()
        for i in range(len(sessions_context)):
            s = list(sessions_context[i]) 
            s[-1] = s[-1].split(',')
            sessions_context[i] = s
        return sessions_context
    
    @classmethod
    def get_watermarks(cls):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM watermarks")
        answers = cursor.fetchall()
        conn.close()

        watermarks = {}
        for answer in answers:
            watermarks[answer[0]] = (answer[1], answer[2])
        return watermarks