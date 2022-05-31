import pandas as pd
from typing import List
from simpy.core import Environment
from simpy.resources.store import FilterStore
from .arrival_process import ArrivalProcess
from src.simulation.elements import Driver
from src.simulation.params import PICKUP_DROPOFF_PATH, DRIVER_PATH, UBER_MARKET_SHARE, DEBUG

class DriverProcess(ArrivalProcess):
    def __init__(self, env: Environment, store: FilterStore, collection: List, initial_drivers: int, \
                 num_active_drivers: List, geo_df: pd.DataFrame, verbose: bool = True, debug: bool = False):
        super().__init__(env, store, collection, verbose, debug)
        self.initial_drivers = initial_drivers
        self.geo_df = geo_df
        self.driver_number = 0
        self.__num_active_drivers = num_active_drivers
        self.drivers = []

        # Load driver supply data
        self.num_driver_df = pd.read_csv(DRIVER_PATH, index_col=['hour', 'minute'])
        self.num_driver_df *= UBER_MARKET_SHARE
        if DEBUG:
            self.num_driver_df /= 10

        # Load trip endpoint data
        self.trip_endpoint_data = pd.read_csv(PICKUP_DROPOFF_PATH, index_col=['day_of_week', 'hour'])

        # Spawn initial drivers
        self.spawn_initial_drivers()

    
    @property
    def num_active_drivers(self):
        return self.__num_active_drivers[0]


    def dispatch_drivers(self, n: int):
        """Dispatches n drivers.

        Args:
            n (int): number of drivers to dispatch
        """
        for _ in range(n):
            Driver(self.driver_number, self.trip_endpoint_data, self.geo_df, self.num_driver_df, self.env,
                   self.store, self.collection, self.__num_active_drivers, self.verbose)
            self.driver_number += 1


    def spawn_initial_drivers(self):
        """Spawns initial drivers
        """
        n_drivers = self.initial_drivers if self.debug == False else int(self.initial_drivers / 10)
        self.dispatch_drivers(n_drivers)
        if self.verbose:
            print(f'Spawned {n_drivers:,} drivers')


    def run(self):
        """
        Simulates the arrival process of drivers throughout the city.
        """
        # Offset
        yield self.env.timeout(0.5)

        # Dispath drivers as necessary
        while True:
            
            # Operate every minute
            yield self.env.timeout(1)
            
            # Determine minute, hour of day
            minute = int(self.env.now % 60)   
            hour = self.env.now / 60
            hour_of_day = int(hour % 24)
            
            # Monitor current supply of drivers
            target_uber_supply = self.num_driver_df.loc[(hour_of_day, minute), 'n_drivers']
            num_active = self.num_active_drivers
            
            # If current supply is not high enough, dispatch drivers
            if target_uber_supply > num_active:
                deficit = int(target_uber_supply - num_active)
                self.dispatch_drivers(deficit)