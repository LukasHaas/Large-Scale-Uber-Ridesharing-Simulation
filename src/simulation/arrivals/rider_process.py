from typing import List
import random
import pandas as pd
import geopandas as gpd
from simpy.core import Environment
from simpy.resources.store import FilterStore
from .arrival_process import ArrivalProcess
from src.simulation.params import UBER_MARKET_SHARE, PICKUP_DROPOFF_PATH
from src.simulation.elements import Rider

class RiderProcess(ArrivalProcess):
    def __init__(self, env: Environment, store: FilterStore, collection: List, arrival_df: pd.DataFrame,
                 geo_df: pd.DataFrame, verbose: bool = True, debug: bool = False):
        """
        Simulates the arrival process of rider pools throughout the city.
        """
        super().__init__(env, store, collection, verbose, debug)
        self.arrival_df = arrival_df
        self.trip_endpoint_data = pd.read_csv(PICKUP_DROPOFF_PATH, index_col=['day_of_week', 'hour'])
        self.geo_df = geo_df

    def run(self):
        rider_number = 0
        while True:
            
            # Determine hour of day and weekday        
            hour = self.env.now / 60
            hour_of_day = int(hour % 24)
            weekday = int((self.env.now / 60 / 24) % 7)
            
            # Simulate Poission arrival
            rate = self.arrival_df.loc[(weekday, hour_of_day)] / 60
            if self.debug:
                rate /= 10 # Slow down
                
            # Adjust for Uber market share
            rate *= UBER_MARKET_SHARE
                
            t = random.expovariate(rate)
            yield self.env.timeout(t)
            
            # Instantiate new rider pool
            Rider(rider_number, self.trip_endpoint_data, self.geo_df, self.env, self.store, self.collection, self.verbose)
            rider_number += 1