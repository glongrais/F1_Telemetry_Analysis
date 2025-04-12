from api_fetcher import ApiFetcher
from db_writer import DatabaseWriter

def main():

    #car_data = ApiFetcher.get_car_data()
    drivers_data = ApiFetcher.get_drivers()
    #intervals_data = ApiFetcher.get_intervals()
    laps_data = ApiFetcher.get_laps()
    #locations_data = ApiFetcher.get_locations()
    meetings_data = ApiFetcher.get_meetings()
    pits_data = ApiFetcher.get_pits()
    positions_data = ApiFetcher.get_positions()
    race_control_data = ApiFetcher.get_race_control()
    sessions_data = ApiFetcher.get_sessions()
    stints_data = ApiFetcher.get_stints()
    team_data = ApiFetcher.get_team_radio()
    weather_data = ApiFetcher.get_weather()

    #DatabaseWriter.upsert_car_data(car_data)
    DatabaseWriter.upsert_drivers(drivers_data)
    #DatabaseWriter.upsert_intervals(intervals_data)
    DatabaseWriter.upsert_laps(laps_data)
    #DatabaseWriter.upsert_locations(locations_data)
    DatabaseWriter.upsert_meetings(meetings_data)
    DatabaseWriter.upsert_pits(pits_data)
    DatabaseWriter.upsert_positions(positions_data)
    DatabaseWriter.upsert_race_control(race_control_data)
    DatabaseWriter.upsert_sessions(sessions_data)
    DatabaseWriter.upsert_stints(stints_data)
    DatabaseWriter.upsert_team_radio(team_data)
    DatabaseWriter.upsert_weather(weather_data)

if __name__ == "__main__":
    main()
