from api_fetcher import ApiFetcher
from db_writer import DatabaseWriter
from datetime import datetime, timedelta

def main():

    """car_data = ApiFetcher.get_car_data()
    drivers_data = ApiFetcher.get_drivers()
    intervals_data = ApiFetcher.get_intervals()
    laps_data = ApiFetcher.get_laps()
    locations_data = ApiFetcher.get_locations()
    meetings_data = ApiFetcher.get_meetings()
    pits_data = ApiFetcher.get_pits()
    positions_data = ApiFetcher.get_positions()
    race_control_data = ApiFetcher.get_race_control()
    sessions_data = ApiFetcher.get_sessions()
    stints_data = ApiFetcher.get_stints()
    team_data = ApiFetcher.get_team_radio()
    weather_data = ApiFetcher.get_weather()

    DatabaseWriter.upsert_car_data(car_data)
    DatabaseWriter.upsert_drivers(drivers_data)
    DatabaseWriter.upsert_intervals(intervals_data)
    DatabaseWriter.upsert_laps(laps_data)
    DatabaseWriter.upsert_locations(locations_data)
    DatabaseWriter.upsert_meetings(meetings_data)
    DatabaseWriter.upsert_pits(pits_data)
    DatabaseWriter.upsert_positions(positions_data)
    DatabaseWriter.upsert_race_control(race_control_data)
    DatabaseWriter.upsert_sessions(sessions_data)
    DatabaseWriter.upsert_stints(stints_data)
    DatabaseWriter.upsert_team_radio(team_data)
    DatabaseWriter.upsert_weather(weather_data) """

    """count = 1
    for i in range(7763, 10011):
        for j in range(1, 100):
            for k in range(0, 9):
                params = "?session_key="+str(i)+"&driver_number="+str(j)+"&n_gear="+str(k)
                car_data = ApiFetcher.get_car_data(params=params)
                DatabaseWriter.upsert_car_data(car_data)
                print(f"Inserted lap data for seesion key {i} and drive {j}. {count}/21123")
                count += 1"""

    session_keys = range(7763, 10011)  # Example session keys
    driver_numbers = range(1, 100)  # Example driver numbers
    batch_duration = timedelta(hours=1)  # One-hour batches

    sessions_context = ApiFetcher.get_sessions_context()
    watermarks = ApiFetcher.get_watermarks()

    SESSION_KEY = 0
    DATE_START = 1
    DATE_END = 2
    DRIVER_NUMBERS = 3
    for session in sessions_context[1:]:
        if session[SESSION_KEY] < watermarks['car_data'][1]:
            continue
        for driver_number in session[DRIVER_NUMBERS]:

            start_time = datetime.fromisoformat(session[DATE_START]) - timedelta(minutes=30)
            end_time = start_time + batch_duration
            end_session_time = datetime.fromisoformat(session[DATE_END]) + timedelta(minutes=30)

            while start_time < end_session_time:
                # Fetch car data for the current batch
                fetch_car_data_in_batches(
                    session_key=session[SESSION_KEY],
                    driver_number=driver_number,
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat()
                )
                # Move to the next batch
                start_time = end_time
                end_time = start_time + batch_duration

def fetch_car_data_in_batches(session_key, driver_number, start_time, end_time):
    """
    Fetches car data in batches for a specific driver and time range.
    """
    params = f"?session_key={session_key}&driver_number={driver_number}&date>={start_time}&date<={end_time}"
    car_data = ApiFetcher.get_car_data(params=params)
    if car_data:
        DatabaseWriter.upsert_car_data(car_data)
        print(f"Inserted car data for session={session_key}, driver_number={driver_number}, time range={start_time} to {end_time}.")
    else:
        print(f"No car data found for session={session_key}, driver_number={driver_number}, time range={start_time} to {end_time}.")


if __name__ == "__main__":
    main()
