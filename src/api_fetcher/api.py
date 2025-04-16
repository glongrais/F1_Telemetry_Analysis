import requests
import sqlite3

DB_PATH = '../../data/f1_data.db'

class Api:

    @classmethod
    def fetch_data(cls, api_url, endpoint_name=""):
        """
        Fetches data from the given API URL and handles errors gracefully.
        """
        try:
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an error for bad status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {api_url}: {e}")
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            cursor.execute("INSERT INTO failed_queries (endpoint_name, query) VALUES (?, ?)", (endpoint_name, api_url))

            conn.commit()
            conn.close()
            return []

    @classmethod
    def get_car_data(cls, params=""):
        """
        Fetches car data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/car_data" + params
        return cls.fetch_data(api_url, endpoint_name="car_data")

    @classmethod
    def get_drivers(cls, params=""):
        """
        Fetches driver data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/drivers" + params
        return cls.fetch_data(api_url, endpoint_name="drivers")

    @classmethod
    def get_intervals(cls, params=""):
        """
        Fetches interval data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/intervals" + params
        return cls.fetch_data(api_url, endpoint_name="intervals")

    @classmethod
    def get_laps(cls, params=""):
        """
        Fetches lap data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/laps" + params
        return cls.fetch_data(api_url, endpoint_name="laps")

    @classmethod
    def get_locations(cls, params=""):
        """
        Fetches location data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/location" + params
        return cls.fetch_data(api_url, endpoint_name="locations")

    @classmethod
    def get_meetings(cls, params=""):
        """
        Fetches meetings data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/meetings" + params
        return cls.fetch_data(api_url, endpoint_name="meetings")

    @classmethod
    def get_pits(cls, params=""):
        """
        Fetches pit data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/pit" + params
        return cls.fetch_data(api_url, endpoint_name="pits")

    @classmethod
    def get_positions(cls, params=""):
        """
        Fetches position data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/position" + params
        return cls.fetch_data(api_url, endpoint_name="positions")

    @classmethod
    def get_race_control(cls, params=""):
        """
        Fetches race control data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/race_control" + params
        return cls.fetch_data(api_url, endpoint_name="race_control")

    @classmethod
    def get_sessions(cls, params=""):
        """
        Fetches sessions data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/sessions" + params
        return cls.fetch_data(api_url, endpoint_name="sessions")

    @classmethod
    def get_stints(cls, params=""):
        """
        Fetches stints data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/stints" + params
        return cls.fetch_data(api_url, endpoint_name="stints")

    @classmethod
    def get_team_radio(cls, params=""):
        """
        Fetches team radio data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/team_radio" + params
        return cls.fetch_data(api_url, endpoint_name="team_radio")

    @classmethod
    def get_weather(cls, params=""):
        """
        Fetches weather data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/weather" + params
        return cls.fetch_data(api_url, endpoint_name="weather")
    
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
            watermarks[answer[0]] = (answer[1], answer[2], answer[3])
        return watermarks