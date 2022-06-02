from typing import List
import random
import pandas as pd
from simpy.core import Environment
from simpy.resources.store import FilterStore
from .arrival_process import ArrivalProcess
from src.simulation.params import UBER_MARKET_SHARE, PICKUP_DROPOFF_PATH
from src.simulation.elements import Rider

class RiderProcess(ArrivalProcess):
    def __init__(self, env: Environment, store: FilterStore, collection: List, arrival_df: pd.DataFrame,
                 geo_df: pd.DataFrame, num_active_requests: List, verbose: bool = True, debug: bool = False):
        """
        Simulates the arrival process of rider pools throughout the city.
        """
        super().__init__(env, store, collection, verbose, debug)
        self.arrival_df = arrival_df
        self.trip_endpoint_data = pd.read_csv(PICKUP_DROPOFF_PATH, index_col=['day_of_week', 'hour'])
        self.geo_df = geo_df
        self.num_active_requests = num_active_requests
        self.rider_number = 0

        # Adjust for Uber market share
        self.arrival_df *= UBER_MARKET_SHARE

        # Adjust for debug
        if self.debug:
            self.arrival_df /= 10 # Slow down

        # Load driver supply data
        hour = (self.env.now / 60)
        hour_of_day = int(hour % 24)
        minute = int(self.env.now % 60)
        weekday = int((self.env.now / 60 / 24) % 7)
        self.initial_riders = int(self.arrival_df.loc[(weekday, hour_of_day, minute)]) # Get hour equivalent of riders

        # Spawn intitial riders
        self.spawn_riders(n=self.initial_riders)


    def spawn_riders(self, n: int=1):
        for _ in range(n):
            Rider(self.rider_number, self.trip_endpoint_data, self.geo_df, self.env, self.store, self.collection,
                  self.num_active_requests, self.verbose)
            self.rider_number += 1
        

    def run(self):
        while True:
            
            # Determine minute, hour of day and weekday
            minute = int(self.env.now % 60)   
            hour = self.env.now / 60
            hour_of_day = int(hour % 24)
            weekday = int((self.env.now / 60 / 24) % 7)
            
            # Simulate Poission arrival
            rate = self.arrival_df.loc[(weekday, hour_of_day, minute)] / 60
                
            t = random.expovariate(rate)
            yield self.env.timeout(t)
            
            # Instantiate new rider pool
            self.spawn_riders()
