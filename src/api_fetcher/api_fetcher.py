import requests

class ApiFetcher:

    @classmethod
    def get_car_data(cls):
        """
        Fetches car data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/car_data"
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()
    
    @classmethod
    def get_drivers(cls):
        """
        Fetches driver data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/drivers"
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()
    
    @classmethod
    def get_intervals(cls):
        """
        Fetches interval data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/intervals"
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()
    
    @classmethod
    def get_laps(cls):
        """
        Fetches lap data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/laps"
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()

    @classmethod
    def get_locations(cls):
        """
        Fetches location data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/location"
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()

    @classmethod
    def get_meetings(cls):
        """
        Fetches meetings data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/meetings"
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()

    @classmethod
    def get_pits(cls):
        """
        Fetches pit data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/pit"
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()
    
    @classmethod
    def get_positions(cls):
        """
        Fetches position data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/position"
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()
    
    @classmethod
    def get_race_control(cls):
        """
        Fetches race control data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/race_control"
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()

    @classmethod
    def get_sessions(cls):
        """
        Fetches sessions data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/sessions"
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()

    @classmethod
    def get_stints(cls):
        """
        Fetches stints data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/stints"
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    
    @classmethod
    def get_weather(cls):
        """
        Fetches weather data from the OpenF1 API.
        """
        api_url = "https://api.openf1.org/v1/weather"
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()