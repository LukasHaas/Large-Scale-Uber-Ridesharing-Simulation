from typing import List
from abc import ABC, abstractmethod
from simpy.core import Environment
from simpy.resources.store import FilterStore

class ArrivalProcess(ABC):
    """
    Abstract class for an arrival processes.
    """
    def __init__(self, env: Environment, store: FilterStore, collection: List,
                 verbose: bool=True, debug: bool=False):
        self.env = env
        self.store = store
        self.collection = collection
        self.verbose = verbose
        self.debug = debug

    @abstractmethod
    def run(self):
        pass