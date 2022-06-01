import numpy as np
import pandas as pd
from typing import List, Tuple
from itertools import product
from src.utils.timing import timing
from .rideshare_algorithm import RideShareMatchingAlgorithm
from .linear_solver import LinearSolver

class ShortestDistance(RideShareMatchingAlgorithm):
    def __init__(self, uber_data: pd.DataFrame):
        """
        Matches riders with drivers minimizing the driver OOS travel time.
        """
        self.uber_data = uber_data
        self.solver = LinearSolver()
    
    @timing
    def create_matches(self, time: float, requests: List, drivers: List) -> List:
        """Generates matches to minimize OOS driving time.

        Args:
            time (float): environment time
            requests (List): list of requests
            drivers (List): list of drivers

        Returns:
            List: list of tuples of (rider, driver) matches
        """
        if not ShortestDistance.is_match_possible(requests, drivers):
            return []
        
        # Determine hour of day and weekday
        hour = time / 60
        hour_of_day = [int(hour % 24)]
        
        # Get driver and rider positions
        driver_pos = [x.anticipated_pos for x in drivers]
        driver_exp_times = np.array([x.exp_time_to_availability for x in drivers]).reshape((len(drivers), 1))
        request_pos = [x.pos for x in requests]

        # Find best matches
        multi_index = list(product(hour_of_day, driver_pos, request_pos))
        travel_times = self.uber_data.loc[multi_index, 'mean_travel_time'].values / 60
        travel_times = travel_times.reshape((len(drivers), len(requests)))
        travel_times += driver_exp_times
        assignments = self.solver.solve_matching(travel_times, minimize=True)

        # Update lists
        matches = []
        for i, driver in enumerate(drivers):
            for j, request in enumerate(requests):
                if assignments[i, j] == 1:
                    matches.append((request, driver))
        
        return matches