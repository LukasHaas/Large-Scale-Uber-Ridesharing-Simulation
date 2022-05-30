import os
import pandas as pd
import geopandas as gpd
from typing import List
from datetime import datetime
from src.simulation.params import *
from src.utils.formatting import cdate
from .driver_analytics import DriverAnalytics

KEPLER_STR = '%d%m%y %H:%M:%S'

def __create_new_run() -> str:
    folder_name = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    new_dir = os.path.join(os.getcwd(), 'runs', folder_name)
    print('Created new directory for run:', new_dir)
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)

    return new_dir


def extract_ride_information(ride_collection: List) -> pd.DataFrame:
    """Aggregates information from ride requests for analysis.

    Args:
        ride_collection (List): list of all "Rider" objects.
    """
    rides = []
    for ride in ride_collection:
        datetime = cdate(ride.start_wait_time, format_str=KEPLER_STR)
        date = datetime.split()[0]
        time = datetime.split()[1]
        point = ride.pos_point if ride.cancelled else ride.des_point
        point_long = point.coords.xy[0][0]
        point_lat = point.coords.xy[1][0]
        #start_long = ride.pos_point.coords.xy[0][0]
        #start_lat = ride.pos_point.coords.xy[1][0]
        #end_long = ride.des_point.coords.xy[0][0]
        #end_lat = ride.des_point.coords.xy[1][0]
        icon = 'cancel' if ride.cancelled else 'check'
        wait_time = ride.wait_time
        ride_time = ride.ride_time
        completed = ride.completed
        rides.append([date, time, point, point_long, point_lat, icon, wait_time, ride_time, completed])
    
    col_info = ['date', 'time', 'geometry', 'long', 'lat', 'icon', 'wait_time', 'ride_time', 'completed']
    ride_df = pd.DataFrame(rides, columns=col_info)
    ride_df = gpd.GeoDataFrame(ride_df, crs="EPSG:4326", geometry='geometry')
    return ride_df


def extract_driver_information(da: DriverAnalytics) -> pd.DataFrame:
    """Extract driver analytics data.

    Args:
        da (DriverAnalytics): driver analytics gatherer.

    Returns:
        pd.DataFrame: driver data.
    """
    col_info = ['date', 'time', 'driver_id', 'from_lon', 'from_lat', 'to_lon', 'to_lat', 'is_oos']
    driver_df = pd.DataFrame(da.analytics, columns=col_info)
    # driver_df = gpd.GeoDataFrame(driver_df, crs="EPSG:4326", geometry='geometry')
    return driver_df

def save_metadata(path: str):
    data = {
        'UBER_MARKET_SHARE': UBER_MARKET_SHARE,
        'MIN_TRIP_TIME': MIN_TRIP_TIME,
        'START_DATE': START_DATE,
        'INITIAL_DRIVERS': INITIAL_DRIVERS,
        'INITIAL_TIME': INITIAL_TIME,
        'RUN_DELTA': RUN_DELTA,
        'VERBOSE': VERBOSE,
        'DEBUG': DEBUG
    }
    with open(path + '/metadata.txt', 'w+') as f:
        f.write('SIMULATION METADATA\n')
        f.write('=' * 50)
        f.write('\n')
        for key, value in data.items():
            f.write(f'{key}: {value}\n')


def save_run(ride_collection: List, da: DriverAnalytics):
    """Logs all relevant information from the simulation.

    Args:
        ride_collection (List): list of all "Rider" objects.
    """
    new_dir = __create_new_run()
    ride_info_df = extract_ride_information(ride_collection)
    ride_info_df.to_csv(new_dir + '/ride_info.csv', index=False)

    driver_info_df = extract_driver_information(da)
    driver_info_df.to_csv(new_dir + '/driver_info.csv', index=False)
    
    save_metadata(new_dir)

if __name__ == '__main__':
    __create_new_run()