import fastf1
import json

# Fetching race data
fastf1.Cache.enable_cache('../Cache')
session = fastf1.get_session(2023, 'monaco', 'race')  # Replace with the desired race details

# Extracting lap times
session.load(telemetry=True)
laps = session.laps
drivers = session.drivers
print(drivers)
data = {}
for driver in drivers:
    tmp = laps.pick_driver(driver).get_car_data().add_driver_ahead(drop_existing=True)
    data[driver] = tmp.to_json(orient='index')


# Writing lap times to a file
output_file = '../Resources/lap_times.json'
with open(output_file, 'w') as f:
    json.dump(data, f)