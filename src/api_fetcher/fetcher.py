from api import Api
from db_writer import DatabaseWriter
from datetime import datetime, timedelta

SESSION_KEY = 0
DATE_START = 1
DATE_END = 2
SESSION_TYPE = 3
DRIVER_NUMBERS = 4

class Fetcher:

    @classmethod
    def fetch_car_data(cls, session_key, driver_number, start_time, end_time):
        """
        Fetches car data in batches for a specific driver and time range.
        """
        params = f"?session_key={session_key}&driver_number={driver_number}&date>={start_time}&date<={end_time}"
        car_data = Api.get_car_data(params=params)
        if car_data:
            DatabaseWriter.upsert_car_data(car_data)
            print(f"Inserted car data for session={session_key}, driver_number={driver_number}, time range={start_time} to {end_time}.")
        else:
            print(f"No car data found for session={session_key}, driver_number={driver_number}, time range={start_time} to {end_time}.")

    @classmethod
    def fetch_batches(cls):

        # Map endpoint names to their corresponding API methods
        endpoint_methods = {
            "meetings": Api.get_meetings,
            "sessions": Api.get_sessions,
            "drivers": Api.get_drivers,
            "car_data": Api.get_car_data,
            "intervals": Api.get_intervals,
            "laps": Api.get_laps,
            "locations": Api.get_locations,
            "pits": Api.get_pits,
            "positions": Api.get_positions,
            "race_control": Api.get_race_control,
            "stints": Api.get_stints,
            "team_radio": Api.get_team_radio,
            "weather": Api.get_weather,
        }

        # Example: Fetch watermarks and session context
        watermarks = Api.get_watermarks()

        for endpoint, method in endpoint_methods.items():
            sessions_context = Api.get_sessions_context()
            print(f"Fetching data for endpoint: {endpoint}")

            if endpoint not in ["car_data", "intervals", "locations", "positions"]:
                # Fetch data for endpoints that do not require batching
                params = f"?meeting_key>={watermarks[endpoint][0]}"
                data = method(params)
                DatabaseWriter.upsert_data(endpoint, data)
            else:
                # Fetch data in batches for endpoints that require batching
                for session in sessions_context:
                    if session[SESSION_KEY] < watermarks[endpoint][1]:
                        continue
                    
                    # Special case for intervals where no data is recorded outside of races
                    if endpoint == "intervals" and session[SESSION_TYPE] != "Race":
                        continue

                    cls.fetch_session(endpoint, method, session, watermarks)

    @classmethod
    def fetch_session(cls, endpoint, method, session, watermarks):
        """
        Fetches session data based on the provided context and watermarks.
        """
        batch_duration = timedelta(hours=1)  # One-hour batches

        for driver_number in session[DRIVER_NUMBERS]:
            if session[SESSION_KEY] == watermarks[endpoint][1] and int(driver_number) < watermarks[endpoint][2]:
                continue
            start_time = datetime.fromisoformat(session[DATE_START]) - timedelta(minutes=30)
            end_time = start_time + batch_duration
            end_session_time = datetime.fromisoformat(session[DATE_END]) + timedelta(minutes=30)

            while start_time < end_session_time:
                # Fetch data for the current batch
                params = f"?session_key={session[SESSION_KEY]}&driver_number={driver_number}&date>={start_time.isoformat()}&date<={end_time.isoformat()}"
                data = method(params=params)
                if data:
                    DatabaseWriter.upsert_data(endpoint, data)
                    print(f"Inserted {endpoint} data for session={session[SESSION_KEY]}, driver_number={driver_number}, time range={start_time} to {end_time}.")
                else:
                    print(f"No {endpoint} data found for session={session[SESSION_KEY]}, driver_number={driver_number}, time range={start_time} to {end_time}.")

                # Move to the next batch
                start_time = end_time
                end_time = start_time + batch_duration
    
    @classmethod
    def retry_fetch_failed_url(cls):
        return