import os
import pandas as pd
import geopandas as gpd
from typing import List
from datetime import datetime
from src.simulation.params import *
from src.utils.formatting import cdate
from .driver_analytics import DriverAnalytics

KEPLER_STR = '%Y/%m/%d %H:%M:%S'

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
        taz = ride.pos
        point = ride.pos_point if ride.cancelled else ride.des_point
        point_long = point.coords.xy[0][0]
        point_lat = point.coords.xy[1][0]
        icon = 'cancel' if ride.cancelled else 'check'
        cancelled = ride.cancelled
        match_wait_time = ride.wait_time
        driver_wait_time = ride.driver_wait_time
        ride_time = ride.ride_time
        completed = ride.completed
        rides.append([datetime, taz, point, point_long, point_lat, icon, cancelled, match_wait_time, driver_wait_time, ride_time, completed])
    
    col_info = ['datetime', 'taz', 'geometry', 'long', 'lat', 'icon', 'cancelled', 'match_wait_time', 'driver_wait_time', 'ride_time', 'completed']
    ride_df = pd.DataFrame(rides, columns=col_info)
    ride_df = gpd.GeoDataFrame(ride_df, crs="EPSG:4326", geometry='geometry')
    return ride_df


def __get_taz_geometry(row, geo_df):
    return geo_df.loc[row['taz']]['geometry']


def __get_taz_area(row, geo_df):
    return geo_df.loc[row['taz']]['AREA']


def aggregate_rider_TAZ_information(ride_df: pd.DataFrame, geo_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregates the rider information per TAZ.

    Args:
        ride_df (pd.DataFrame): the rider analytics data frame.
        geo_df (pd.DataFrame): the dataframe containing geometries for TAZs

    Returns:
        pd.DataFrame: TAZ-aggregated data.
    """
    ride_df['date'] = ride_df['datetime'].apply(lambda x: x.split()[0])
    ride_df['hour'] = ride_df['datetime'].apply(lambda x: x.split()[1].split(':')[0])
    agg_df = ride_df.groupby(['date', 'hour', 'taz']).agg(
        num_requests=('ride_time', 'count'),
        share_cancelled=('cancelled', 'mean'),
        mean_match_wait=('match_wait_time', 'mean'),
        mean_driver_wait=('driver_wait_time', 'mean'),
        time=('datetime', 'first'),
    ).reset_index()

    agg_df['geometry'] = agg_df.apply(lambda x: __get_taz_geometry(x, geo_df), axis=1)
    agg_df['area'] = agg_df.apply(lambda x: __get_taz_area(x, geo_df), axis=1)
    agg_df['rider_density'] = agg_df['num_requests'] / agg_df['area']
    return agg_df


def aggregate_driver_TAZ_information(driver_df: pd.DataFrame, geo_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregates the driver information per TAZ.

    Args:
        driver_df (pd.DataFrame): the driver analytics data frame.
        geo_df (pd.DataFrame): the dataframe containing geometries for TAZs

    Returns:
        pd.DataFrame: TAZ-aggregated data.
    """
    driver_df['date'] = driver_df['datetime'].apply(lambda x: x.split()[0])
    driver_df['hour'] = driver_df['datetime'].apply(lambda x: x.split()[1].split(':')[0])
    agg_df = driver_df.groupby(['date', 'hour', 'taz']).agg(
        num_drivers=('driver_id', 'count'),
        share_idle=('idle', 'mean'),
        share_oos=('is_oos', 'mean'),
        share_passenger_trip=('passenger_drive', 'mean'),
        time=('datetime', 'first'),
    ).reset_index()

    agg_df['geometry'] = agg_df.apply(lambda x: __get_taz_geometry(x, geo_df), axis=1)
    agg_df['area'] = agg_df.apply(lambda x: __get_taz_area(x, geo_df), axis=1)
    agg_df['driver_density'] = agg_df['num_drivers'] / agg_df['area']
    return agg_df


def __compute_driver_status(row):
    if row['ontrip'] == False:
        return 0
    if row['is_oos']:
        return 1
    else:
        return 2


def extract_driver_information(da: DriverAnalytics) -> pd.DataFrame:
    """Extract driver analytics data.

    Args:
        da (DriverAnalytics): driver analytics gatherer.

    Returns:
        pd.DataFrame: driver data.
    """
    col_info = ['datetime', 'taz', 'driver_id', 'from_lon', 'from_lat', 'to_lon', 'to_lat', 'is_oos', 'ontrip']
    driver_df = pd.DataFrame(da.analytics, columns=col_info)
    if driver_df.empty:
        return None

    driver_df['status'] = driver_df.apply(__compute_driver_status, axis=1)
    driver_df['idle'] = driver_df['status'].apply(lambda x: x == 0)
    driver_df['passenger_drive'] = driver_df['status'].apply(lambda x: x == 2)
    return driver_df


def save_metadata(path: str):
    data = {
        'UBER_MARKET_SHARE': UBER_MARKET_SHARE,
        'MIN_TRIP_TIME': MIN_TRIP_TIME,
        'START_DATE': START_DATE,
        'INITIAL_DRIVERS': INITIAL_DRIVERS,
        'INITIAL_TIME': INITIAL_TIME,
        'RUN_DELTA': RUN_DELTA,
        'BATCH_FREQUENCY': BATCH_FREQUENCY,
        'VERBOSE': VERBOSE,
        'DEBUG': DEBUG
    }
    with open(path + '/metadata.txt', 'w+') as f:
        f.write('SIMULATION METADATA\n')
        f.write('=' * 50)
        f.write('\n')
        for key, value in data.items():
            f.write(f'{key}: {value}\n')


def save_run(ride_collection: List, da: DriverAnalytics, geo_df: pd.DataFrame):
    """Logs all relevant information from the simulation.

    Args:
        ride_collection (List): list of all "Rider" objects.
    """
    new_dir = __create_new_run()
    ride_info_df = extract_ride_information(ride_collection)
    ride_info_df.to_csv(new_dir + '/ride_info.csv', index=False)

    rider_taz_agg_df = aggregate_rider_TAZ_information(ride_info_df, geo_df)
    rider_taz_agg_df.to_csv(new_dir + '/rider_taz_info.csv', index=False)

    driver_info_df = extract_driver_information(da)
    if driver_info_df is not None:
        driver_info_df.to_csv(new_dir + '/driver_info.csv', index=False)

        driver_taz_agg_df = aggregate_driver_TAZ_information(driver_info_df, geo_df)
        driver_taz_agg_df.to_csv(new_dir + '/driver_taz_info.csv', index=False)

    save_metadata(new_dir)

if __name__ == '__main__':
    __create_new_run()