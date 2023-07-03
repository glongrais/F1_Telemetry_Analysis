import fastf1
import json
import concurrent.futures
import sys
import multiprocessing
import numpy as np

def calculate_distance(data, driver, index):
    distances = np.zeros(index)
    for i in range(index):

        distance = 0
        current_driver = driver

        drivers = [driver]

        if i >= len(data[driver]):
            distances[i] = -1
            continue

        while current_driver in data and data[current_driver].iloc[i]["DriverAhead"]:
            distance += float(data[current_driver].iloc[i]["DistanceToDriverAhead"])
            current_driver = data[current_driver].iloc[i]["DriverAhead"]
            if current_driver in drivers:
                distance = -1
                break
            drivers.append(current_driver)
        
        distances[i] = round(distance, 2)

    return driver, distances

def calculate_distances(data):
    distances = {}

    total_drivers = len(data)
    drivers = [driver for driver in data]
    completed_count = 0
    completed_load = 0

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for driver in drivers:
                futures.append(executor.submit(calculate_distance, data, driver, 24000))
                completed_load += 1
                progress = completed_load / total_drivers * 100
                sys.stdout.write("\rLoading: %.2f%%" % progress)
                sys.stdout.flush()

        for future in concurrent.futures.as_completed(futures):

            driver, distance = future.result()
            distances[driver] = distance

            # Update progress indicator
            completed_count += 1

            progress = completed_count / total_drivers * 100
            sys.stdout.write("\rProgress: %.2f%%" % progress)
            sys.stdout.flush()

    print()  # Move to the next line after progress indicator is complete

    return distances


if __name__ == '__main__':
    # Fetching race data
    fastf1.Cache.enable_cache('../Cache')
    session = fastf1.get_session(2023, 'monaco', 'race')  # Replace with the desired race details

    # Extracting lap times
    session.load(telemetry=True)
    laps = session.laps
    drivers = session.drivers

    data = {}
    for driver in drivers:
        tmp = laps.pick_driver(driver).get_car_data().add_driver_ahead(drop_existing=True)
        #data[driver] = tmp.to_json(orient='index')
        data[driver] = tmp
    print(calculate_distances(data))
    # Writing lap times to a file
    #output_file = '../Resources/lap_times.json'
    #with open(output_file, 'w') as f:
    #    json.dump(data, f)