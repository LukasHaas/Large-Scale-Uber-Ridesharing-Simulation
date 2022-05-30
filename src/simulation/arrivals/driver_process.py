import pandas as pd
from .arrival_process import ArrivalProcess
from src.simulation.elements import Driver
from src.simulation.params import PICKUP_DROPOFF_PATH

class DriverProcess(ArrivalProcess):
    def run(self, initial_drivers: int, geo_df: pd.DataFrame):
        """
        Simulates the arrival process of drivers throughout the city.
        """
        trip_endpoint_data = pd.read_csv(PICKUP_DROPOFF_PATH, index_col=['day_of_week', 'hour'])
        n_drivers = initial_drivers if self.debug == False else initial_drivers / 10
        _ = [Driver(i, trip_endpoint_data, geo_df, self.env, self.store, self.collection, self.verbose) for i in range(n_drivers)]
        if self.verbose:
            print(f'Spawned {initial_drivers:,} drivers')