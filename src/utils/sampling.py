import random
import numpy as np
import pandas as pd
from typing import List
from shapely.geometry import Polygon, Point
from src.simulation.params import MIN_TRIP_TIME, TRAVEL_TIMES_PATH

travel_time_df = pd.read_csv(TRAVEL_TIMES_PATH, index_col=['hod', 'sourceid', 'dstid'])

def sample_point_in_geometry(geometry: Polygon, num_samples: int) -> List[Polygon]:
    """Samples points in the given geometry

    Args:
        geometry (Polygon): polygon to sample in
        num_samples (int): number of samples

    Returns:
        List[Polygon]: sampled points
    """
    points = []
    minx, miny, maxx, maxy = geometry.bounds
    
    for _ in range(num_samples):
        point = None
        while point is None or geometry.contains(point) == False:
            point = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
        
        points.append(point)
    
    return points[0] if num_samples == 1 else points

def sample_random_trip_time(hour_of_day: int, origin: int, destination: int, is_trip=False):
    """
    Samples time needed from origin to destination by drawing from log-normal
    distribution based on the geometric mean and geometric standard deviation
    travel times for the TAZ pair and hour of day.

    Minimum time for trips is MIN_TRIP_TIME.
    """
    TAZ_times = travel_time_df.loc[(hour_of_day, origin, destination)]   
    geo_mean = TAZ_times['geometric_mean_travel_time']
    geo_std = TAZ_times['geometric_standard_deviation_travel_time']
    time = np.random.lognormal(np.log(geo_mean), np.log(geo_std)) / 60
    if is_trip and time < MIN_TRIP_TIME:
        time = MIN_TRIP_TIME
        
    return time