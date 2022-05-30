from .matcher import Matcher
from typing import List
from simpy.core import Environment
from simpy.resources.store import FilterStore
from ..algorithms import RideShareMatchingAlgorithm
from ..elements import Driver, Trip

class IncrementalMatcher(Matcher):
    def __init__(self, env: Environment, algorithm: RideShareMatchingAlgorithm, store: FilterStore,
                 trip_collection: List, verbose: bool = True):
        """
        Matches drivers to riders to service requests in an incremental manner.
        """
        super().__init__(env, algorithm, trip_collection, verbose)
        self.store = store

    def perform_matching(self):
        """
        Matches drivers to riders to service requests in an incremental manner.
        """
        available_drivers = []
        available_requests = []
        
        while True:
            _, new_item = yield self.store.get(lambda x: x[1].available)
            if isinstance(new_item, Driver):
                available_drivers.append(new_item)
            else:
                available_requests.append(new_item)
                
            # Update availabilities
            available_drivers = [x for x in available_drivers if x.available]
            available_requests = [x for x in available_requests if x.available]

            # print(f'----- PRE : Driver Pool: {len(available_drivers)}, Rider Pool: {len(available_requests)}')

            # Get items and compute matches
            matches, available_requests, available_drivers = \
                self.algorithm.create_matches(self.env.now, available_requests, available_drivers)
            # print(f'----- POST: Driver Pool: {len(available_drivers)}, Rider Pool: {len(available_requests)}')

            # Create trips with matches
            for match in matches:
                trip = Trip(self.env, match[0], match[1], self.trip_collection, self.verbose)
                trip.perform()