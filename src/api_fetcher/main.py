from fetcher import Fetcher

def main():

    Fetcher.fetch_batches()
    """car_data = Api.get_car_data()
    drivers_data = Api.get_drivers()
    intervals_data = Api.get_intervals()
    laps_data = Api.get_laps()
    locations_data = Api.get_locations()
    meetings_data = Api.get_meetings()
    pits_data = Api.get_pits()
    positions_data = Api.get_positions()
    race_control_data = Api.get_race_control()
    sessions_data = Api.get_sessions()
    stints_data = Api.get_stints()
    team_data = Api.get_team_radio()
    weather_data = Api.get_weather()

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

if __name__ == "__main__":
    main()

