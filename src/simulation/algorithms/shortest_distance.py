import numpy as np
import pandas as pd
from typing import List, Tuple
from .rideshare_algorithm import RideShareMatchingAlgorithm
from .linear_solver import LinearSolver

class ShortestDistance(RideShareMatchingAlgorithm):
    def __init__(self, uber_data: pd.DataFrame,  proxy_length: pd.DataFrame):
        """
        Matches riders with drivers minimizing the driver OOS travel time.
        """
        self.uber_data = uber_data
        self.proxy_length = proxy_length
        self.solver = LinearSolver()
    
    def create_matches(self, time: float, requests: List, drivers: List) -> Tuple:
        """Generates matches to minimize OOS driving time.

        Args:
            time (float): environment time
            requests (List): list of requests
            drivers (List): list of drivers

        Returns:
            Tuple: matches, unmatched requests, and unmatched drivers
        """
        if not ShortestDistance.is_match_possible(requests, drivers):
            return [], requests, drivers
        
        # Determine hour of day and weekday
        hour = time / 60
        hour_of_day = int(hour % 24)
        
        # Find best matches
        travel_times = []
        for driver in drivers:
            for request in requests:
                try:
                    cost = self.uber_data.loc[(hour_of_day, driver.curr_pos, request.pos)]['mean_travel_time'] / 60
                except KeyError:
                    cost = self.proxy_length.loc[(hour_of_day, driver.curr_pos)]['mean_travel_time'] / 60

                travel_times.append(cost)
        
        travel_times = np.array(travel_times).reshape((len(drivers), len(requests)))
        assignments = self.solver.solve_matching(travel_times, minimize=True)

        matches = []
        for i, driver in enumerate(drivers):
            for j, request in enumerate(requests):
                if assignments[i, j] == 1:
                    matches.append((request, driver))

        new_drivers = [x for i, x in enumerate(drivers) if assignments.sum(axis=1)[i] == 0]
        new_requests = [x for i, x in enumerate(requests) if assignments.sum(axis=0)[i] == 0]
        
        return matches, new_requests, new_drivers