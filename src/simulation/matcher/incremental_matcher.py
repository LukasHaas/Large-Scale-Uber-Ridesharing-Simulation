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
        self.available_drivers = []
        self.available_requests = []

    def perform_matching(self):
        """
        Matches drivers to riders to service requests in an incremental manner.
        """
        while True:
            _, new_item = yield self.store.get(lambda x: x[1].available)
            if isinstance(new_item, Driver):
                self.available_drivers.append(new_item)
            else:
                self.available_requests.append(new_item)
                
            # Update availabilities
            self.available_drivers = [x for x in self.available_drivers if x.available]
            self.available_requests = [x for x in self.available_requests if x.available]

            # Get items and compute matches
            matches = self.algorithm.create_matches(self.env.now, self.available_requests, self.available_drivers)

            # Create trips with matches
            for match in matches:
                trip = Trip(self.env, match[0], match[1], self.trip_collection, self.verbose)
                trip.perform()