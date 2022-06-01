from urllib.request import Request
from .matcher import Matcher
from typing import List
from simpy.core import Environment
from simpy.resources.store import FilterStore
from ..algorithms import RideShareMatchingAlgorithm
from ..elements import Driver, Trip, Rider

class BatchMatcher(Matcher):
    def __init__(self, env: Environment, algorithm: RideShareMatchingAlgorithm, frequency: float,
                 store: FilterStore, trip_collection: List, verbose: bool = True):
        """Initializes a batch matching scheduler operating at frequency "frequency".

        Note:
        Batch matching requires both the "keep_running_availabilities" and "perform_matching"
        methods to be part of the simulation environment.

        Args:
            env (Environment): simpy environment.
            algorithm (RideShareMatchingAlgorithm): ride sharing algorithm to use.
            frequency (float): frequency of batch matching.
            store (FilterStore): store containing newly available drivers and riders.
            trip_collection (List): analytics collection of trip.
            verbose (bool, optional): whether to print detailed output. Defaults to True.
        """
        super().__init__(env, algorithm, trip_collection, verbose)
        self.frequency = frequency
        self.store = store
        self.available_drivers = []
        self.available_requests = []


    def perform_matching(self):
        """Performs the batch matching.
        """
        while True:
            # Wait for next batch matching time
            yield self.env.timeout(self.frequency)

            # Update availabilities
            self.available_drivers = [x for x in self.available_drivers if x.available]
            self.available_requests = [x for x in self.available_requests if x.available]

            # Get items and compute matches
            matches = self.algorithm.create_matches(self.env.now, self.available_requests, self.available_drivers)

            # Create trips with matches
            for match in matches:
                trip = Trip(self.env, match[0], match[1], self.trip_collection, self.verbose)
                trip.perform()
            

    def keep_running_availabilities(self):
        """
        Matches drivers to riders to service requests in an incremental manner.
        """        
        while True:
            _, new_item = yield self.store.get(lambda x: x[1].available)
            if isinstance(new_item, Driver):
                self.available_drivers.append(new_item)
            elif isinstance(new_item, Rider):
                self.available_requests.append(new_item)
            else:
                raise Exception('Invalid object entered FilterStore:', new_item)