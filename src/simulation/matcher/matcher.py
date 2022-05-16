from abc import ABC, abstractmethod
from typing import List
from simpy.core import Environment
from src.simulation.algorithms import RideShareMatchingAlgorithm

class Matcher(ABC):
    def __init__(self, env: Environment, algorithm: RideShareMatchingAlgorithm,
                 trip_collection: List, verbose: bool=True):
        """
        Matches drivers to riders to service requests.
        """
        self.env = env
        self.algorithm = algorithm
        self.trip_collection = trip_collection
        self.verbose = verbose

    @abstractmethod
    def perform_matching(self):
        pass

