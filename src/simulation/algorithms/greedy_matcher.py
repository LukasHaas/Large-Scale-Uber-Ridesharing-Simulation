import numpy as np
import pandas as pd
from typing import List, Tuple
from .rideshare_algorithm import RideShareMatchingAlgorithm

class GreedyMatcher(RideShareMatchingAlgorithm):
    def __init__(self, uber_data: pd.DataFrame,  proxy_length: pd.DataFrame, distance_based: bool=True):
        """
        Greedily matches riders with drivers prioritizing either shortest distance to start a new trip or
        rider which has waited the longest (in which case closest driver picks that rider up).
        """
        self.distance_based = distance_based
        self.uber_data = uber_data
        self.proxy_length = proxy_length
    
    def create_matches(self, time: float, requests: List, drivers: List) -> Tuple:
        """Generates matches according to greedy policy. 

        Args:
            time (_type_): environment time
            requests (List): list of requests
            drivers (List): list of drivers

        Returns:
            Tuple: matches, unmatched requests, and unmatched drivers
        """
        if not GreedyMatcher.is_match_possible(requests, drivers):
            return [], requests, drivers
        
        # Determine hour of day and weekday
        hour = time / 60
        hour_of_day = int(hour % 24)
        
        # Greedy algorithm means either multiple requests and one driver or other way around
        best_match = None
        
        if self.distance_based:
            for request in requests:
                for driver in drivers:
                    try:
                        cost = self.uber_data.loc[(hour_of_day, driver.curr_pos, request.pos)]['mean_travel_time'] / 60
                    except KeyError:
                        cost = self.proxy_length.loc[(hour_of_day, driver.curr_pos)]['mean_travel_time'] / 60
                    
                    if best_match is None or cost < best_match[2]:
                        best_match = (request, driver, cost)
  
        else:
            rider_times = [time - x.start_wait_time for x in requests]
            max_waiter = requests[int(np.argmax(rider_times))]
            for driver in drivers:
                try:
                    cost = self.uber_data.loc[(hour_of_day, driver.curr_pos, max_waiter.pos)]['mean_travel_time'] / 60
                except KeyError:
                    cost = self.proxy_length.loc[(hour_of_day, driver.curr_pos)]['mean_travel_time'] / 60

                if best_match is None or cost < best_match[2]:
                    best_match = (max_waiter, driver, cost)
            
        new_requests = [x for x in requests if x != best_match[0]]
        new_drivers = [x for x in drivers if x != best_match[1]]
        
        return [(best_match[0], best_match[1])], new_requests, new_drivers