import os
from matplotlib.pyplot import axis
import pandas as pd
import geopandas as gpd
from typing import List
from datetime import datetime
from src.utils.clock import Clock
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


def __match_with_geo_df(df: pd.DataFrame, geo_df: pd.DataFrame) -> pd.DataFrame:
    """Adds geo data to the supplied dataframe.

    Args:
        df (pd.DataFrame): the dataframe to be matched with the geo data.
        geo_df (pd.DataFrame): the dataframe containing the geo data.

    Returns:
        pd.DataFrame: the matched dataframe.
    """
    # Cartesian product
    index = pd.MultiIndex.from_product(df.index.levels)
    df = df.reindex(index).reset_index()

    # Time
    df['time'] = df.apply(__generate_log_time, axis=1)

    # Get geometry
    df['geometry'] = df.apply(lambda x: __get_taz_geometry(x, geo_df), axis=1)
    df['area'] = df.apply(lambda x: __get_taz_area(x, geo_df), axis=1)

    # Fill gaps
    df['has_geometry'] = df['geometry'].isnull()
    df = df.sort_values(by=['taz', 'has_geometry'])
    df['geometry'] = df['geometry'].fillna(method='pad')
    df = df.drop('has_geometry', axis=1)
    
    # Sort
    df = df.sort_values(by=['date', 'hour', 'taz'])
    return df


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
    )

    # Add geo data
    agg_df = __match_with_geo_df(agg_df, geo_df)
    agg_df['num_requests'] = agg_df['num_requests'].fillna(value=0)

    # Compute driver densities
    agg_df['rider_density'] = agg_df['num_requests'] / agg_df['area']
    return agg_df


def __generate_log_time(row):
    date = row['date']
    hour = row['hour']
    return f'{date} {hour}:00:00'


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
    )

    # Add geo data
    agg_df = __match_with_geo_df(agg_df, geo_df)
    agg_df['num_drivers'] = agg_df['num_drivers'].fillna(value=0)

    # Compute driver densities
    agg_df['driver_density'] = agg_df['num_drivers'] / agg_df['area']

    # Remove before and after
    #min_date, max_date = driver_df['date'].min(), driver_df['date'].max()
    #remove_indices = agg_df.loc[((agg_df['date'] == min_date) & (agg_df['hour'] < 3)) |
    #                            ((agg_df['date'] == max_date) & (agg_df['hour'] >= 3))]

    return agg_df


def __compute_driver_status(row):
    if row['ontrip'] == False:
        return 0
    if row['is_oos']:
        return 1
    else:
        return 2


def extract_driver_snapshots(da: DriverAnalytics) -> pd.DataFrame:
    """Extract driver analytics data.

    Args:
        da (DriverAnalytics): driver analytics gatherer.

    Returns:
        pd.DataFrame: driver data.
    """
    col_info = ['datetime', 'taz', 'driver_id', 'from_lon', 'from_lat', 'to_lon', 'to_lat', 'is_oos', 'ontrip', 'num_jobs']
    driver_df = pd.DataFrame(da.analytics, columns=col_info)
    if driver_df.empty:
        return None

    driver_df['status'] = driver_df.apply(__compute_driver_status, axis=1)
    driver_df['idle'] = driver_df['status'].apply(lambda x: x == 0)
    driver_df['passenger_drive'] = driver_df['status'].apply(lambda x: x == 2)
    return driver_df


def extract_driver_information(driver_collection: List) -> pd.DataFrame:
    """Aggregates information from drivers for analysis.

    Args:
        driver_collection (List): list of all "Driver" objects.
    """
    drivers = []
    for driver in driver_collection:
        oos_wait = driver.oos_wait
        oos_drive = driver.oos_drive
        oos_total = driver.oos_total
        service_drive = driver.trip_total
        total_time_active = driver.total_time_active
        num_jobs = driver.num_jobs
        drivers.append([oos_wait, oos_drive, oos_total, service_drive, total_time_active, num_jobs])
    
    col_info = ['oos_wait', 'oos_drive', 'oos_total', 'service_drive', 'total_time_active', 'num_jobs']
    driver_df = pd.DataFrame(drivers, columns=col_info)
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
        'MAX_DRIVER_JOB_QUEUE': MAX_DRIVER_JOB_QUEUE,
        'DYNAMIC_SUPPLY': DYNAMIC_SUPPLY,
        'MARKET_FORCE_SUPPLY': MARKET_FORCE_SUPPLY,
        'VERBOSE': VERBOSE,
        'DEBUG': DEBUG
    }
    with open(path + '/metadata.txt', 'w+') as f:
        f.write('SIMULATION METADATA\n')
        f.write('=' * 50)
        f.write('\n')
        for key, value in data.items():
            f.write(f'{key}: {value}\n')


def save_clock_data(clock: Clock) -> pd.DataFrame:
    """Saves the number of active drivers and riders at any given time.

    Args:
        clock (Clock): clock giving high-level market thickness overviews.

    Returns:
        pd.DataFrame: datframe containing time and active participants.
    """
    col_names = ['time', 'drivers', 'riders_and_requests', 'ratio']
    clock_df = pd.DataFrame(clock.data, columns=col_names)
    return clock_df


def save_run(ride_collection: List, driver_collection: List, da: DriverAnalytics,
             geo_df: pd.DataFrame, clock: Clock=None):
    """Generates all analytics needed for analysis.

    Args:
        ride_collection (List): list containing all rider objects
        driver_collection (List): list containing all driver objects
        da (DriverAnalytics): driver analytics object performing driver snapshots at time intervals
        geo_df (pd.DataFrame): dataframe linking TAZs to geometries
        clock (Clock, optional): models supply and demand side high-level analytics. Defaults to None.
    """
    new_dir = __create_new_run()
    ride_info_df = extract_ride_information(ride_collection)
    ride_info_df.to_csv(new_dir + '/ride_info.csv', index=False)

    driver_info_df = extract_driver_information(driver_collection)
    driver_info_df.to_csv(new_dir + '/driver_info.csv', index=False)

    rider_taz_agg_df = aggregate_rider_TAZ_information(ride_info_df, geo_df)
    rider_taz_agg_df.to_csv(new_dir + '/rider_taz_info.csv', index=False)

    driver_snapshot_df = extract_driver_snapshots(da)
    if driver_snapshot_df is not None:
        driver_snapshot_df.to_csv(new_dir + '/driver_snapshots.csv', index=False)

        driver_taz_agg_df = aggregate_driver_TAZ_information(driver_snapshot_df, geo_df)
        driver_taz_agg_df.to_csv(new_dir + '/driver_taz_info.csv', index=False)

    if clock is not None:
        clock_df = save_clock_data(clock)
        clock_df.to_csv(new_dir + '/clock_info.csv', index=False)

    save_metadata(new_dir)
    print('Simulation data successfully saved.')

if __name__ == '__main__':
    __create_new_run()